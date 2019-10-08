import logging
import json
from collections import defaultdict
from django.core.management.base import BaseCommand
from django.db.models.query_utils import Q
from pyliftover.liftover import LiftOver

from reference_data.models import GENOME_VERSION_GRCh38
from seqr.models import Project, SavedVariant, Sample
from seqr.model_utils import update_xbrowse_vcfffiles
from seqr.views.apis.dataset_api import _update_samples
from seqr.views.utils.dataset_utils import match_sample_ids_to_sample_records, validate_index_metadata, \
    get_elasticsearch_index_samples
from seqr.views.utils.json_to_orm_utils import update_model_from_json
from seqr.views.utils.orm_to_json_utils import get_json_for_saved_variants
from seqr.views.utils.variant_utils import reset_cached_search_results
from seqr.utils.es_utils import get_es_variants_for_variant_tuples
from seqr.utils.xpos_utils import get_xpos

logger = logging.getLogger(__name__)


class Command(BaseCommand):
    help = 'Transfer projects to the new seqr schema'

    def add_arguments(self, parser):
        parser.add_argument('--project', required=True)
        parser.add_argument('--es-index', required=True)

    def handle(self, *args, **options):
        """transfer project"""
        project_arg = options['project']
        elasticsearch_index = options['es_index']

        project = Project.objects.get(Q(name=project_arg) | Q(guid=project_arg))
        logger.info('Updating project genome version for {}'.format(project.name))

        # Validate the provided index
        logger.info('Validating es index {}'.format(elasticsearch_index))
        sample_ids, index_metadata = get_elasticsearch_index_samples(elasticsearch_index)
        validate_index_metadata(index_metadata, project, elasticsearch_index, genome_version=GENOME_VERSION_GRCh38)
        sample_type = index_metadata['sampleType']
        dataset_path = index_metadata['sourceFilePath']

        matched_sample_id_to_sample_record = match_sample_ids_to_sample_records(
            project=project,
            sample_ids=sample_ids,
            sample_type=sample_type,
            dataset_type=Sample.DATASET_TYPE_VARIANT_CALLS,
            elasticsearch_index=elasticsearch_index,
            sample_id_to_individual_id_mapping={},
        )

        unmatched_samples = set(sample_ids) - set(matched_sample_id_to_sample_record.keys())
        if len(unmatched_samples) > 0:
            raise Exception('Matches not found for ES sample ids: {}.'.format(', '.join(unmatched_samples)))

        included_family_individuals = defaultdict(set)
        individual_guids_by_id = {}
        for sample in matched_sample_id_to_sample_record.values():
            included_family_individuals[sample.individual.family].add(sample.individual.individual_id)
            individual_guids_by_id[sample.individual.individual_id] = sample.individual.guid
        missing_family_individuals = []
        for family, individual_ids in list(included_family_individuals.items()):
            missing_indivs = family.individual_set.filter(
                sample__sample_status=Sample.SAMPLE_STATUS_LOADED,
                sample__dataset_type=Sample.DATASET_TYPE_VARIANT_CALLS
            ).exclude(individual_id__in=individual_ids)
            if missing_indivs:
                missing_family_individuals.append(
                    '{} ({})'.format(family.family_id, ', '.join([i.individual_id for i in missing_indivs]))
                )
        if missing_family_individuals:
            raise Exception(
                'The following families are included in the callset but are missing some family members: {}.'.format(
                    ', '.join(missing_family_individuals)
                ))

        # Get and clean up expected saved variants
        saved_variant_models_by_guid = {v.guid: v for v in SavedVariant.objects.filter(project=project)}
        deleted_no_family = set()
        deleted_no_tags = set()
        for guid, variant in list(saved_variant_models_by_guid.items()):
            if not variant.family:
                deleted_no_family.add(guid)
            elif not (variant.varianttag_set.count() or variant.variantnote_set.count()):
                deleted_no_tags.add(guid)

        if deleted_no_family:
            if raw_input('Do you want to delete the following {} saved variants with no family (y/n)?: {} '.format(
                    len(deleted_no_family), ', '.join(deleted_no_family))) == 'y':
                for guid in deleted_no_family:
                    saved_variant_models_by_guid.pop(guid).delete()
                logger.info('Deleted {} variants'.format(len(deleted_no_family)))

        if deleted_no_tags:
            if raw_input('Do you want to delete the following {} saved variants with no tags (y/n)?: {} '.format(
                    len(deleted_no_tags), ', '.join(deleted_no_tags))) == 'y':
                for guid in deleted_no_tags:
                    saved_variant_models_by_guid.pop(guid).delete()
                logger.info('Deleted {} variants'.format(len(deleted_no_tags)))

        expected_families = {sv.family for sv in saved_variant_models_by_guid.values()}
        missing_families = expected_families - set(included_family_individuals.keys())
        if missing_families:
            raise Exception(
                'The following families have saved variants but are missing from the callset: {}.'.format(
                    ', '.join([f.family_id for f in missing_families])
                ))

        # Lift-over saved variants
        saved_variants = get_json_for_saved_variants(
            list(saved_variant_models_by_guid.values()), add_details=True, project=project,
            individual_guids_by_id=individual_guids_by_id)
        saved_variants_to_lift = [v for v in saved_variants if v['genomeVersion'] != GENOME_VERSION_GRCh38]

        num_already_lifted = len(saved_variants) - len(saved_variants_to_lift)
        if num_already_lifted:
            if raw_input('Found {} saved variants already on Hg38. Continue with liftover (y/n)?'.format(num_already_lifted)) != 'y':
                raise Exception('Error: found {} saved variants already on Hg38'.format(num_already_lifted))
        logger.info('Lifting over {} variants (skipping {} that are already lifted)'.format(
            len(saved_variants_to_lift), num_already_lifted))

        liftover_to_38 = LiftOver('hg19', 'hg38')
        hg37_to_hg38_xpos = {}
        lift_failed = set()
        for v in saved_variants_to_lift:
            if not (hg37_to_hg38_xpos.get(v['xpos']) or v['xpos'] in lift_failed):
                hg38_coord = liftover_to_38.convert_coordinate('chr{}'.format(v['chrom'].lstrip('chr')), int(v['pos']))
                if hg38_coord and hg38_coord[0]:
                    hg37_to_hg38_xpos[v['xpos']] = get_xpos(hg38_coord[0][0], hg38_coord[0][1])
                else:
                    lift_failed.add(v['xpos'])

        if lift_failed:
            raise Exception(
                'Unable to lift over the following {} coordinates: {}'.format(len(lift_failed), ', '.join(lift_failed)))

        saved_variants_map = defaultdict(list)
        for v in saved_variants_to_lift:
            variant_model = saved_variant_models_by_guid[v['variantGuid']]
            saved_variants_map[(hg37_to_hg38_xpos[v['xpos']], v['ref'], v['alt'])].append(variant_model)

        es_variants = get_es_variants_for_variant_tuples(expected_families, saved_variants_map.keys())

        missing_variants = set(saved_variants_map.keys()) - {(v['xpos'], v['ref'], v['alt']) for v in es_variants}
        if missing_variants:
            missing_variant_strings = ['{}-{}-{} ({})'.format(
                xpos, ref, alt,
                ', '.join(['{}: {}'.format(v.family.family_id, v.guid) for v in saved_variants_map[(xpos, ref, alt)]]))
                for xpos, ref, alt in missing_variants]
            if raw_input('Unable to find the following {} variants in the index. Continue with update (y/n)?: {} '.format(
                    len(missing_variants), ', '.join(missing_variant_strings))) != 'y':
                raise Exception('Error: unable to find {} lifted-over variants'.format(len(missing_variants)))

        logger.info('Successfully lifted over {} variants'.format(len(es_variants)))

        #  Update saved variants
        for var in es_variants:
            saved_variant_models = saved_variants_map[(var['xpos'], var['ref'], var['alt'])]
            missing_families = [v.family.guid for v in saved_variant_models if v.family.guid not in var['familyGuids']]
            if missing_families:
                raise Exception('Error with variant {}:{}-{}-{} not find for expected families {}; found in families {}'.format(
                    var['chrom'], var['pos'], var['ref'], var['alt'], ', '.join(missing_families), ', '.join(var['familyGuids'])
                ))
            for saved_variant in saved_variant_models:
                saved_variant.xpos_start = var['xpos']
                saved_variant.saved_variant_json = json.dumps(var)
                saved_variant.save()

        logger.info('Successfully updated {} variants'.format(len(es_variants)))

        # Update project and sample data
        update_model_from_json(project, {'genome_version': GENOME_VERSION_GRCh38, 'has_new_search': True})
        _update_samples(
            matched_sample_id_to_sample_record, elasticsearch_index=elasticsearch_index, dataset_path=dataset_path
        )
        update_xbrowse_vcfffiles(
            project, sample_type, elasticsearch_index, dataset_path, matched_sample_id_to_sample_record
        )

        reset_cached_search_results(project)

        logger.info('---Done---')
        logger.info('Succesfully lifted over {} variants. Skipped {} failed variants.'.format(
            len(es_variants), len(missing_variants)))
