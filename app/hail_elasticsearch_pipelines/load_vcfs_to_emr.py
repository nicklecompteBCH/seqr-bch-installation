"""This module contains functions for loading data on the AWS EMR hail cluster used by the BCH hail installation.
"""

import boto3
import botocore
from urllib.parse import urlparse
import os
import time
import requests
from typing import Iterable

from hail_scripts.v02.utils.hail_utils import import_vcf, run_vep
from hail_scripts.v02.export_table_to_es import export_table_to_elasticsearch
from hail_scripts.v02.utils.computed_fields.variant_id import *
from hail_scripts.v02.utils.computed_fields.vep import *
from elasticsearch_utils import ElasticsearchClient

BCH_CLUSTER_TAG = "bch-hail-cluster"
BCH_CLUSTER_NAME = 'hail-bch'
GENOME_VERSION = '37'


client = boto3.client('emr')
s3client = boto3.client('s3')

def parse_vcf_s3_path(s3path):
    parsed = urlparse(s3path)
    bucket = parsed.netloc
    path = parsed.path[1:]
    object_list = path.split('/')
    filename = object_list[-1]
    return {
        "bucket" : bucket,
        "path" : path,
        "filename" : filename
    }


def get_hail_cluster():

    clusters = client.list_clusters(
        ClusterStates=[
            'STARTING','BOOTSTRAPPING','RUNNING','WAITING'
        ]
    )

    if not clusters or not (clusters['Clusters']):
        raise ValueError("Might need to (re)start the hail cluster, everything else is terminated")

    hail_cluster_list = list(filter(lambda x: x['Name'] == BCH_CLUSTER_NAME, clusters['Clusters']))

    if not hail_cluster_list:
        raise ValueError("Might need to (re)start the hail cluster, could not find hail-bch among running EMR clusters")

    hail_cluster = hail_cluster_list[0]
    return hail_cluster

def add_global_metadata(vds, s3bucket, genomeVersion="37", sampleType="WES", datasetType="VARIANTS"):
    """Adds structured metadata to the vds 'global' struct. This will later be copied to the elasticsearch index _meta field."""

    # Store step0_output_vds as the cached version of the dataset in google buckets, and also set it as the global.sourceFilePath
    # because
    # 1) vep is the most time-consuming step (other than exporting to elasticsearch), so it makes sense to cache results
    # 2) at this stage, all subsetting and remapping has already been applied, so the samples in the dataset are only the ones exported to elasticsearch
    # 3) annotations may be updated / added more often than vep versions.
    vds = vds.annotate_globals(sourceFilePath = s3bucket)
    vds = vds.annotate_globals(genomeVersion =genomeVersion)
    vds = vds.annotate_globals(sampleType = sampleType)
    vds = vds.annotate_globals(datasetType = datasetType)

    return vds

def add_vcf_to_hail(s3path_to_vcf):
    parts = parse_vcf_s3_path(s3path_to_vcf)
    s3buckets = boto3.resource('s3')
    s3bucket = s3buckets.Bucket(parts['bucket'])
    s3bucket.download_file(parts['path'], parts['filename'])
    os.system('hdfs dfs -put ' + parts['filename'])
    mt = import_vcf(
        parts['filename'],
        GENOME_VERSION,
        min_partitions=30)
    mt = add_global_metadata(mt, s3path_to_vcf)

    return mt

def compute_derived_fields(mt):

    retl = [
        f"va.docId = {get_expr_for_variant_id(mt, max_length=512)}",
        f"va.variantId = {get_expr_for_variant_id(mt)}",
        f"va.variantType= {get_expr_for_variant_type}",
        f"va.contig = {get_expr_for_contig(mt.locus)}",
        f"va.pos={mt.locus.position}",
        f"va.start={mt.locus.position}",
        f"va.end={get_expr_for_end_pos(mt)}",
        f"va.ref={mt.alleles[0]}",
        f"va.xpos={get_expr_for_xpos(mt.locus)}",
        f"va.xstart={{get_expr_for_xpos(mt.locus)}}",
        f"va.sortedTranscriptConsequences = {get_expr_for_vep_sorted_transcript_consequences_array(vep_root='va.vep', include_coding_annotations=True)}",
        f"va.FAF = {get_expr_for_filtering_allele_frequency('va.info.AC[va.aIndex - 1]', 'va.info.AN', 0.95)}",
    ]

    mt = mt.annotate_variants_expr(retl)

    serial_computed_annotation_exprs = [
        f"va.xstop ={get_expr_for_end_pos(mt)}",
        f"va.transcriptIds ={get_expr_for_vep_transcript_ids_set('va.sortedTranscriptConsequences')}",
        f"va.domains = {get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root='va.sortedTranscriptConsequences')}",
        f"va.transcriptConsequenceTerms = {get_expr_for_vep_consequence_terms_set('va.sortedTranscriptConsequences')}",
        f"va.mainTranscript ={get_expr_for_worst_transcript_consequence_annotations_struct('va.sortedTranscriptConsequences')}",
        f"va.geneIds = {get_expr_for_vep_gene_ids_set('va.sortedTranscriptConsequences')}",
        f"va.codingGeneIds = {get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root='va.sortedTranscriptConsequences', only_coding_genes=True)}",
    ]

    for expr in serial_computed_annotation_exprs:
        mt = mt.annotate_variants_expr(expr)

    INPUT_SCHEMA = {}
    INPUT_SCHEMA["top_level_fields"] = """
            docId: String,
            variantId: String,
            originalAltAlleles: Set[String],
            contig: String,
            start: Int,
            pos: Int,
            end: Int,
            ref: String,
            alt: String,
            xpos: Long,
            xstart: Long,
            xstop: Long,
            rsid: String,
            --- qual: Double,
            filters: Set[String],
            aIndex: Int,
            geneIds: Set[String],
            transcriptIds: Set[String],
            codingGeneIds: Set[String],
            domains: Set[String],
            transcriptConsequenceTerms: Set[String],
            sortedTranscriptConsequences: String,
            mainTranscript: Struct,
            """
    INPUT_SCHEMA["info_fields"] = """
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
                --- BaseQRankSum: Double,
                --- ClippingRankSum: Double,
                --- DP: Int,
                --- FS: Double,
                --- InbreedingCoeff: Double,
                --- MQ: Double,
                --- MQRankSum: Double,
                --- QD: Double,
                --- ReadPosRankSum: Double,
                --- VQSLOD: Double,
                --- culprit: String,
            """

    #vep_mt = run_vep(mt, GENOME_VERSION)

    #os.remove(parts['filename'])
    #return vep_mt

    expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean",**INPUT_SCHEMA)
    mt = mt.annotate_variants_expr(expr=expr)
    mt = mt.annotate_variants_expr("va = va.clean")

    return mt

def add_vep_to_vcf(mt):
    mt = run_vep(mt, GENOME_VERSION)
    return mt

class SeqrProjectDataSet:
    def __init__(
        self,
        indiv_id: str,
        fam_id : str,
        vcf_s3_path: str,
        bam_s3_path: str,
        project_name : str,
        sample_type : str = "WES"
    ):
        self.indiv_id = indiv_id
        self.fam_id = fam_id
        self.vcf_s3_path = vcf_s3_path
        self.bam_s3_path = bam_s3_path
        self.project_name = project_name
        self.sample_type = sample_type

def beggs_redcap_csv_line_to_seqr_dataset(inputline: dict) -> SeqrProjectDataSet:
# record_id,investigator,de_identified_subject,initial_study_participant_kind,
# base_pn,processed_vcf,processed_bam,family_name,initial_study_affected,
# hpo_terms,description,gender
    indiv_id = inputline['de_identified_subject']
    split_id = indiv_id.split('.')
    fam_id = split_id[0]
    vcf_s3_path = inputline['processed_vcf']
    bam_s3_path = inputline['processed_bam']
    project_name = 'alan_beggs'
    return SeqrProjectDataSet(indiv_id, fam_id, vcf_s3_path, bam_s3_path, project_name)

def bch_connect_export_to_seqr_datasets(inputline: dict) -> SeqrProjectDataSet:
# record_id	de_identified_subject family_name processed_bam
# processed_vcf investigator elasticsearch_import_yn elasticsearch_index
# import_seqr_yn seqr_project_name seqr_id seqr_failure_log
    indiv_id = inputline['de_identified_subject']
    split_id = indiv_id.split('.')
    fam_id = split_id[0]
    vcf_s3_path = inputline['processed_vcf']
    bam_s3_path = inputline['processed_bam']
    project_name = inputline['investigator']
    return SeqrProjectDataSet(
        indiv_id, fam_id,
        vcf_s3_path, bam_s3_path, project_name
    )

def compute_index_name(dataset: SeqrProjectDataSet,version="0.5"):
    """Returns elasticsearch index name computed based on a project dataset"""
    index_name = "%s%s%s__%s__grch%s__%s__%s" % (
        dataset.project_name,
        "__"+dataset.fam_id,
        "__"+dataset.indiv_id,
        dataset.sample_type,
        GENOME_VERSION,
        "WES",
        version,
    )

    index_name = index_name.lower()  # elasticsearch requires index names to be all lower-case

   # logger.info("Index name: %s" % (index_name,))

    return index_name



ELASTICSEARCH_HOST=os.environ['ELASTICSEARCH_HOST']

def determine_if_already_uploaded(dataset: SeqrProjectDataSet):
    resp = requests.get(ELASTICSEARCH_HOST + ":9200/" + compute_index_name(dataset) + "0.1vcf")
    if "index_not_found_exception" in resp.text:
        return False
    return True

def add_project_dataset_to_elastic_search(
    dataset: SeqrProjectDataSet,
    host, index_name, index_type="VARIANT",
    port=9200, num_shards=12, block_size=200):
    client = ElasticsearchClient(host=host,port=port)

    vcf_mt = add_vcf_to_hail(dataset.vcf_s3_path)
    vcf = add_global_metadata(vcf_mt.rows(),dataset.vcf_s3_path)
    index_name = compute_index_name(dataset)
    client.export_vds_to_elasticsearch(vcf, index_name=index_name+"vcf")
#    export_table_to_elasticsearch(vcf_mt.rows(), host, index_name+"vcf", index_type, port=port, num_shards=num_shards, block_size=block_size)
    vep_mt = add_vep_to_vcf(vcf)
    client.export_vds_to_elasticsearch(vep_mt.rows(), index_name=index_name+"vep")
#    export_table_to_elasticsearch(vep_mt.rows(), host, index_name+"vep", index_type, port=port, num_shards=num_shards, block_size=block_size)
    print("ES index name : %s, family : %s, individual : %s ",(index_name,dataset.fam_id, dataset.indiv_id))

def run_all_beggs(host,dry_run = True):
    import csv
    with open('beggs.csv','r') as beggs:
        for row in csv.DictReader(beggs):
            dataset = beggs_redcap_csv_line_to_seqr_dataset(row)
            if dry_run:
                print(dataset.vcf_s3_path, compute_index_name(dataset))
            else:
                add_project_dataset_to_elastic_search(dataset, host, compute_index_name(dataset))

def run_all_connect(
    dry_run = True,
    project_whitelist : Iterable = None, project_blacklist : Iterable = None
):
    import csv
    with open('import_log.csv','w') as log:
        with open('bchconnect.csv','r') as connect_results:
            for row in csv.DictReader(connect_results):
                parsed_dataset = bch_connect_export_to_seqr_datasets(row)
                if project_whitelist:
                    if parsed_dataset.project_name not in project_whitelist:
                        print(parsed_dataset.project_name + " is not on the whitelist")
                        continue
                if project_blacklist:
                    if parsed_dataset.project_name in project_blacklist:
                        continue
                if determine_if_already_uploaded(parsed_dataset):
                    retstr = f"Project {parsed_dataset.project_name} individual {parsed_dataset.indiv_id} already in Seqr under index {compute_index_name(parsed_dataset)}"
                    if dry_run:
                        print(retstr)
                    else:
                        print(retstr)
                        log.write(retstr + "\n")
                if dry_run:
                    print(parsed_dataset.vcf_s3_path, compute_index_name(parsed_dataset))
                else:
                    add_project_dataset_to_elastic_search(
                        parsed_dataset, ELASTICSEARCH_HOST, compute_index_name(parsed_dataset))
                    log.write(parsed_dataset.project_name + "," + parsed_dataset.indiv_id + "," + compute_index_name(parsed_dataset))

if __name__ == "__main__":
    run_all_connect(dry_run=False)
