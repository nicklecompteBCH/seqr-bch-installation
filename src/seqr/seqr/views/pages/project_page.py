"""
APIs used by the project page
"""

import logging
import json

from django.contrib.auth.decorators import login_required
from django.db.models import Q, Count
from django.utils import timezone

from seqr.models import Family, Individual, _slugify, VariantTagType, VariantTag, VariantFunctionalData, VariantNote, \
    AnalysisGroup, Sample
from seqr.views.apis.auth_api import API_LOGIN_REQUIRED_URL
from seqr.views.apis.individual_api import export_individuals
from seqr.views.apis.locus_list_api import get_sorted_project_locus_lists
from seqr.views.apis.users_api import get_json_for_project_collaborator_list
from seqr.views.utils.json_utils import create_json_response
from seqr.views.utils.json_to_orm_utils import update_project_from_json
from seqr.views.utils.orm_to_json_utils import \
    _get_json_for_project, get_json_for_samples, _get_json_for_families, _get_json_for_individuals, \
    get_json_for_saved_variants, get_json_for_analysis_groups, get_json_for_variant_functional_data_tag_types


from seqr.views.utils.permissions_utils import get_project_and_check_permissions

logger = logging.getLogger(__name__)


@login_required(login_url=API_LOGIN_REQUIRED_URL)
def project_page_data(request, project_guid):
    """Returns a JSON object containing information used by the project page:
    ::

      json_response = {
         'project': {..},
         'familiesByGuid': {..},
         'individualsByGuid': {..},
         'samplesByGuid': {..},
       }

    Args:
        project_guid (string): GUID of the Project to retrieve data for.
    """
    project = get_project_and_check_permissions(project_guid, request.user)
    update_project_from_json(project, {'last_accessed_date': timezone.now()})

    families_by_guid, individuals_by_guid, samples_by_guid, analysis_groups_by_guid, locus_lists_by_guid = _get_project_child_entities(project, request.user)

    project_json = _get_json_for_project(project, request.user)
    project_json['collaborators'] = get_json_for_project_collaborator_list(project)
    project_json.update(_get_json_for_variant_tag_types(project, request.user, individuals_by_guid))
    project_json['locusListGuids'] = locus_lists_by_guid.keys()
    project_json['detailsLoaded'] = True

    return create_json_response({
        'projectsByGuid': {project_guid: project_json},
        'familiesByGuid': families_by_guid,
        'individualsByGuid': individuals_by_guid,
        'samplesByGuid': samples_by_guid,
        'locusListsByGuid': locus_lists_by_guid,
        'analysisGroupsByGuid': analysis_groups_by_guid,
    })


def _get_project_child_entities(project, user):
    families_by_guid = _retrieve_families(project.guid, user)
    individuals_by_guid = _retrieve_individuals(project.guid, user)
    for individual_guid, individual in list(individuals_by_guid.items()):
        families_by_guid[individual['familyGuid']]['individualGuids'].add(individual_guid)
    samples_by_guid = _retrieve_samples(project.guid, individuals_by_guid)
    analysis_groups_by_guid = _retrieve_analysis_groups(project)
    locus_lists = get_sorted_project_locus_lists(project, user)
    locus_lists_by_guid = {locus_list['locusListGuid']: locus_list for locus_list in locus_lists}
    return families_by_guid, individuals_by_guid, samples_by_guid, analysis_groups_by_guid, locus_lists_by_guid


def _retrieve_families(project_guid, user):
    """Retrieves family-level metadata for the given project.

    Args:
        project_guid (string): project_guid
        user (Model): for checking permissions to view certain fields
    Returns:
        dictionary: families_by_guid
    """
    fields = Family._meta.json_fields + Family._meta.internal_json_fields
    family_models = Family.objects.filter(project__guid=project_guid).only(*fields)

    families = _get_json_for_families(family_models, user, project_guid=project_guid)

    families_by_guid = {}
    for family in families:
        family_guid = family['familyGuid']
        family['individualGuids'] = set()
        families_by_guid[family_guid] = family

    return families_by_guid


def _retrieve_individuals(project_guid, user):
    """Retrieves individual-level metadata for the given project.

    Args:
        project_guid (string): project_guid
    Returns:
        dictionary: individuals_by_guid
    """

    individual_models = Individual.objects.filter(family__project__guid=project_guid)

    individuals = _get_json_for_individuals(individual_models, user=user, project_guid=project_guid)

    individuals_by_guid = {}
    for i in individuals:
        i['sampleGuids'] = set()
        individual_guid = i['individualGuid']
        individuals_by_guid[individual_guid] = i

    return individuals_by_guid


def _retrieve_samples(project_guid, individuals_by_guid):
    """Retrieves sample metadata for the given project.

        Args:
            project_guid (string): project_guid
            individuals_by_guid (dict): maps each individual_guid to a dictionary with individual info.
                This method adds a "sampleGuids" list to each of these dictionaries.
        Returns:
            2-tuple with dictionaries: (samples_by_guid, sample_batches_by_guid)
        """
    sample_models = Sample.objects.filter(individual__family__project__guid=project_guid)

    samples = get_json_for_samples(sample_models, project_guid=project_guid)

    samples_by_guid = {}
    for s in samples:
        sample_guid = s['sampleGuid']
        samples_by_guid[sample_guid] = s

        individual_guid = s['individualGuid']
        individuals_by_guid[individual_guid]['sampleGuids'].add(sample_guid)

    return samples_by_guid


def _retrieve_analysis_groups(project):
    group_models = AnalysisGroup.objects.filter(project=project)
    groups = get_json_for_analysis_groups(group_models, project_guid=project.guid)
    return {group['analysisGroupGuid']: group for group in groups}


def _get_json_for_variant_tag_types(project, user, individuals_by_guid):
    individual_guids_by_id = {
        individual['individualId']: individual_guid for individual_guid, individual in list(individuals_by_guid.items())
    }

    tag_counts_by_type_and_family = VariantTag.objects.filter(saved_variant__project=project).values('saved_variant__family__guid', 'variant_tag_type__name').annotate(count=Count('*'))
    note_counts_by_family = VariantNote.objects.filter(saved_variant__project=project).values('saved_variant__family__guid').annotate(count=Count('*'))
    project_variant_tags = get_project_variant_tag_types(project, tag_counts_by_type_and_family=tag_counts_by_type_and_family, note_counts_by_family=note_counts_by_family)
    discovery_tags = []
    for tag_type in project_variant_tags:
        if tag_type['category'] == 'CMG Discovery Tags' and tag_type['numTags'] > 0:
            tags = VariantTag.objects.filter(saved_variant__project=project, variant_tag_type__guid=tag_type['variantTagTypeGuid']).select_related('saved_variant')
            saved_variants = [tag.saved_variant for tag in tags]
            discovery_tags += get_json_for_saved_variants(
                saved_variants, add_tags=True, add_details=True, project=project, user=user, individual_guids_by_id=individual_guids_by_id)

    project_functional_tags = []
    for category, tags in VariantFunctionalData.FUNCTIONAL_DATA_CHOICES:
        project_functional_tags += [{
            'category': category,
            'name': name,
            'metadataTitle': json.loads(tag_json).get('metadata_title'),
            'color': json.loads(tag_json)['color'],
            'description': json.loads(tag_json).get('description'),
        } for name, tag_json in tags]

    return {
        'variantTagTypes': sorted(project_variant_tags, key=lambda variant_tag_type: variant_tag_type['order']),
        'variantFunctionalTagTypes': get_json_for_variant_functional_data_tag_types(),
        'discoveryTags': discovery_tags,
    }


def get_project_variant_tag_types(project, tag_counts_by_type_and_family=None, note_counts_by_family=None):
    note_tag_type = {
        'variantTagTypeGuid': 'notes',
        'name': 'Has Notes',
        'category': 'Notes',
        'description': '',
        'color': 'grey',
        'order': 100,
        'is_built_in': True,
    }
    if note_counts_by_family is not None:
        num_tags = sum(count['count'] for count in note_counts_by_family)
        note_tag_type.update({
            'numTags': num_tags,
            'numTagsPerFamily': {count['saved_variant__family__guid']: count['count'] for count in
                                 note_counts_by_family},
        })
    project_variant_tags = [note_tag_type]
    for variant_tag_type in VariantTagType.objects.filter(Q(project=project) | Q(project__isnull=True)):
        tag_type = {
            'variantTagTypeGuid': variant_tag_type.guid,
            'name': variant_tag_type.name,
            'category': variant_tag_type.category,
            'description': variant_tag_type.description,
            'color': variant_tag_type.color,
            'order': variant_tag_type.order,
            'is_built_in': variant_tag_type.is_built_in,
        }
        if tag_counts_by_type_and_family is not None:
            current_tag_type_counts = [counts for counts in tag_counts_by_type_and_family if
                                       counts['variant_tag_type__name'] == variant_tag_type.name]
            num_tags = sum(count['count'] for count in current_tag_type_counts)
            tag_type.update({
                'numTags': num_tags,
                'numTagsPerFamily': {count['saved_variant__family__guid']: count['count'] for count in
                                     current_tag_type_counts},
            })
        project_variant_tags.append(tag_type)

    return sorted(project_variant_tags, key=lambda variant_tag_type: variant_tag_type['order'])


"""
def _get_json_for_reference_populations(project):
    result = []

    for reference_populations in project.custom_reference_populations.all():
        result.append({
            'id': reference_populations.slug,
            'name': reference_populations.name,
        })

    return result
"""


@login_required(login_url=API_LOGIN_REQUIRED_URL)
def export_project_individuals_handler(request, project_guid):
    """Export project Individuals table.

    Args:
        project_guid (string): GUID of the project for which to export individual data
    """

    format = request.GET.get('file_format', 'tsv')
    include_phenotypes = bool(request.GET.get('include_phenotypes'))

    project = get_project_and_check_permissions(project_guid, request.user)

    # get all individuals in this project
    individuals = Individual.objects.filter(family__project=project).order_by('family__family_id', 'affected')

    filename_prefix = "%s_individuals" % _slugify(project.name)

    return export_individuals(
        filename_prefix,
        individuals,
        format,
        include_hpo_terms_present=include_phenotypes,
        include_hpo_terms_absent=include_phenotypes,
    )
