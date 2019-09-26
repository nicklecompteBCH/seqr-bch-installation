from collections import defaultdict
import csv
from django.core.management.base import BaseCommand
import elasticsearch
import elasticsearch_dsl
import json

import settings
from seqr.models import Individual
from seqr.views.utils.orm_to_json_utils import _get_json_for_individuals
from xbrowse_server.base.models import Project as BaseProject

EXCLUDE_PROJECTS = ['ext', '1000 genomes', 'DISABLED', 'project', 'interview', 'non-cmg', 'amel']

PER_PAGE = 5000


class Command(BaseCommand):

    def add_arguments(self, parser):
        parser.add_argument("--metadata-only", action="store_true", help="Only get the project/ individual metadata.")
        parser.add_argument("--use-project-indices-csv", action="store_true", help="Load projects to search from project_indices.csv")
        parser.add_argument("--index", nargs='+', help="Individual index to use")

    def handle(self, *args, **options):

        if options["index"]:
            es_indices = options["index"]
        elif options["use_project_indices_csv"]:
            with open('project_indices.csv') as csvfile:
                reader = csv.DictReader(csvfile)
                es_indices = {row['index'] for row in reader}

        else:
            projects_q = BaseProject.objects.filter(genome_version='37')
            for exclude_project in EXCLUDE_PROJECTS:
                projects_q = projects_q.exclude(project_name__icontains=exclude_project)
            indices_for_project = defaultdict(list)
            for project in projects_q:
                indices_for_project[project.get_elasticsearch_index()].append(project)
            indices_for_project.pop(None, None)

            seqr_projects = []
            with open('project_indices.csv', 'wb') as csvfile:
                fieldnames = ['projectGuid', 'index']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                for index, projects in list(indices_for_project.items()):
                    for project in projects:
                        seqr_projects.append(project.seqr_project)
                        writer.writerow({'projectGuid': project.seqr_project.guid, 'index': index})

            individuals = _get_json_for_individuals(Individual.objects.filter(family__project__in=seqr_projects))
            with open('seqr_individuals.csv', 'wb') as csvfile:
                fieldnames = ['projectGuid', 'familyGuid', 'individualId', 'paternalId', 'maternalId', 'sex',
                              'affected']
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                writer.writeheader()
                for individual in individuals:
                    writer.writerow(individual)
            es_indices = indices_for_project.keys()

        if not options["metadata_only"]:
            es_client = elasticsearch.Elasticsearch(host=settings.ELASTICSEARCH_SERVICE_HOSTNAME, timeout=10000)
            search = elasticsearch_dsl.Search(using=es_client, index='*,'.join(es_indices) + "*")
            search = search.query("match", mainTranscript_lof='HC')
            search = search.source(['contig', 'pos', 'ref', 'alt', '*num_alt', '*gq', '*ab', '*dp', '*ad'])

            print('Searching across {} indices...'.format(len(es_indices)))
            result_count_search = search.params(size=0)
            total = result_count_search.execute().hits.total
            print('Loading {} variants...'.format(total))

            with open('lof_variants.csv', 'a') as csvfile:
                sample_fields = ['num_alt', 'gq', 'ab', 'dp', 'ad']
                fieldnames = ['contig', 'pos', 'ref', 'alt', 'index'] + sample_fields
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
                if not options["index"]:
                    writer.writeheader()
                for i, hit in enumerate(search.scan()):
                    result = {key: hit[key] for key in hit}
                    result['index'] = hit.meta.index
                    for field in sample_fields:
                        result[field] = json.dumps({
                            key.rstrip('_{}'.format(field)): val for key, val in list(result.items()) if key.endswith(field)
                        })
                    writer.writerow(result)
                    if i % 10000 == 0:
                        print('Parsed {} variants'.format(i))

            print('Loaded {} variants'.format(i))

        print('Done')