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
from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient

from hail_scripts.v02.utils.clinvar import CLINVAR_GOLD_STARS_LOOKUP, download_and_import_latest_clinvar_vcf


BCH_CLUSTER_TAG = "bch-hail-cluster"
BCH_CLUSTER_NAME = 'hail-bch'
GENOME_VERSION = '37'
ELASTICSEARCH_HOST=os.environ['ELASTICSEARCH_HOST']



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
        parts['filename'],
        force_bgz=True,
        min_partitions=1000)
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

def compute_index_name(dataset: SeqrProjectDataSet, sample_type='wes',dataset_type='VARIANTS'):
    """Returns elasticsearch index name computed based on command-line args"""

    # generate the index name as:  <project>_<WGS_WES>_<family?>_<VARIANTS or SVs>_<YYYYMMDD>_<batch>
    if args.index:
        index_name = args.index.lower()
    else:
        index_name = "%s%s%s__%s__grch%s__%s__%s" % (
            dataset.project_name,
            "__"+dataset.fam_id,  # optional family id
            "__"+dataset.indiv_id,  # optional individual id
            sample_type,
            'GRCh37',
            dataset_type,
            time.strftime("%Y%m%d"),
        )

        index_name = index_name.lower()  # elasticsearch requires index names to be all lower-case

    return index_name


def load_clinvar(export_to_es=False):
    index_name = "cliivar_grch37" #"clinvar_grch{}".format(args.genome_version)
    mt = download_and_import_latest_clinvar_vcf("37")
    mt = hl.vep(mt, "vep85-loftee-gcloud.json", name="vep", block_size=1000)
    mt = mt.annotate_rows(
        sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep)
    )
    mt = mt.annotate_rows(
        main_transcript=get_expr_for_worst_transcript_consequence_annotations_struct(
            vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences
        )
    )
    mt = mt.annotate_rows(
        gene_ids=get_expr_for_vep_gene_ids_set(
            vep_transcript_consequences_root=mt.sortedTranscriptConsequences
        ),
    )

    review_status_str = hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNREVSTAT)), key=lambda s: s.replace("^_", "z")))


    if export_to_es:
        mt = mt.select_rows(
            allele_id=mt.info.ALLELEID,
            alt=get_expr_for_alt_allele(mt),
            chrom=get_expr_for_contig(mt.locus),
            clinical_significance=hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNSIG)), key=lambda s: s.replace("^_", "z"))),
            domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=mt.vep.transcript_consequences),
            gene_ids=mt.gene_ids,
            gene_id_to_consequence_json=get_expr_for_vep_gene_id_to_consequence_map(
                vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences,
                gene_ids=mt.gene_ids
            ),
            gold_stars=CLINVAR_GOLD_STARS_LOOKUP[review_status_str],
            **{f"main_transcript_{field}": mt.main_transcript[field] for field in mt.main_transcript.dtype.fields},
            pos=get_expr_for_start_pos(mt),
            ref=get_expr_for_ref_allele(mt),
            review_status=review_status_str,
            transcript_consequence_terms=get_expr_for_vep_consequence_terms_set(
                vep_transcript_consequences_root=mt.sortedTranscriptConsequences
            ),
            transcript_ids=get_expr_for_vep_transcript_ids_set(
                vep_transcript_consequences_root=mt.sortedTranscriptConsequences
            ),
            transcript_id_to_consequence_json=get_expr_for_vep_transcript_id_to_consequence_map(
                vep_transcript_consequences_root=mt.sortedTranscriptConsequences
            ),
            variant_id=get_expr_for_variant_id(mt),
            xpos=get_expr_for_xpos(mt.locus)
        )

        hl.summarize_variants(mt)

        # Drop key columns for export
        rows = mt.rows()
        rows = rows.order_by(rows.variant_id).drop("locus", "alleles")

        print("\n=== Exporting ClinVar to Elasticsearch ===")
        es = ElasticsearchClient(ELASTICSEARCH_HOST, "9200")
        es.export_table_to_elasticsearch(
            rows,
            index_name=index_name,
            index_type_name='variant',
            block_size=200,
            num_shards=2,
            delete_index_before_exporting=True,
            export_globals_to_index_meta=True,
            verbose=True,
        )
    else:
        return mt

clinvar_mt = load_clinvar()

def determine_if_already_uploaded(dataset: SeqrProjectDataSet):
    resp = requests.get(ELASTICSEARCH_HOST + ":9200/" + compute_index_name(dataset) + "0.5vep")
    if "index_not_found_exception" in resp.text:
        return False
    return True

def add_project_dataset_to_elastic_search(
    dataset: SeqrProjectDataSet,
    host, index_name, index_type="variant",
    port=9200, num_shards=12, block_size=200):
    client = ElasticsearchClient(host=host,port=port)

    vcf_mt = add_vcf_to_hail(dataset.vcf_s3_path)
    vcf = add_global_metadata(vcf_mt,dataset.vcf_s3_path)
    index_name = compute_index_name(dataset)
    vep_mt = add_vep_to_vcf(vcf)

    vep_mt = vep_mt.annotate_rows(
        sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=vep_mt.vep)
    )
    vep_mt = vep_mt.annotate_rows(
        main_transcript=get_expr_for_worst_transcript_consequence_annotations_struct(
            vep_sorted_transcript_consequences_root=vep_mt.sortedTranscriptConsequences
        )
    )
    vep_mt = vep_mt.annotate_rows(
        gene_ids=get_expr_for_vep_gene_ids_set(
            vep_transcript_consequences_root=vep_mt.sortedTranscriptConsequences
        ),
    )

        """
        'genotypes': [
            {
              'num_alt': 2,
              'ab': 1,
              'dp': 74,
              'gq': 99,
              'sample_id': 'NA20870',
            },
        """

    vep_mt = vep_mt.annotate_rows(
        genotypes = hl.Struct(**{
            'num_alt' : hl.info.NS,
            'ab':hl.info.AO,
            'dp': vep_mt.info.DP,
            'sample_id': parse_vcf_s3_path(s3path_to_vcf)['filename']}
    )

    #review_status_str = hl.delimit(hl.sorted(hl.array(hl.set(clinvar_mt.info.CLNREVSTAT)), key=lambda s: s.replace("^_", "z")))

    # vep_mt = vep_mt.annotate_rows(
    #         allele_id=clinvar_mt.info.ALLELEID,
    #         alt=get_expr_for_alt_allele(vep_mt),
    #         chrom=get_expr_for_contig(vep_mt.locus),
    #         clinical_significance=hl.delimit(hl.sorted(hl.array(hl.set(clinvar_mt.info.CLNSIG)), key=lambda s: s.replace("^_", "z"))),
    #         domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=vep_mt.vep.transcript_consequences),
    #         gene_id_to_consequence_json=get_expr_for_vep_gene_id_to_consequence_map(
    #             vep_sorted_transcript_consequences_root=vep_mt.sortedTranscriptConsequences,
    #             gene_ids=vep_mt.gene_ids
    #         ),
    #         #gold_stars=CLINVAR_GOLD_STARS_LOOKUP[review_status_str],
    #         #**{f"main_transcript_{field}": vep_mt.main_transcript[field] for field in vep_mt.main_transcript.dtype.fields},
    #         pos=get_expr_for_start_pos(vep_mt),
    #         ref=get_expr_for_ref_allele(vep_mt),
    #         #review_status=review_status_str,
    #         # transcript_consequence_terms=get_expr_for_vep_consequence_terms_set(
    #         #     vep_transcript_consequences_root=vep_mt.sortedTranscriptConsequences
    #         # ),
    #         # transcript_ids=get_expr_for_vep_transcript_ids_set(
    #         #     vep_transcript_consequences_root=vep_mt.sortedTranscriptConsequences
    #         # ),
    #         #transcript_id_to_consequence_json=get_expr_for_vep_transcript_id_to_consequence_map(
    #         #    vep_transcript_consequences_root=vep_mt.sortedTranscriptConsequences
    #         #)
    #         #variant_id=get_expr_for_variant_id(vep_mt),
    #     )
    # add clinvar
    #vep_mt = vep_mt.union_cols(clinvar_mt)
    export_table_to_elasticsearch(vep_mt.rows(), host, index_name+"vep", index_type, is_vds=True, port=port,num_shards=num_shards, block_size=block_size)
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
    import argparse

    p = argparse.ArgumentParser()
    p.add_argument("-clinvar", "--clinvar", help="Run clinvar instead of loading samples",  action="store_true")
    args = p.parse_args()

    if not args.clinvar:
        run_all_connect(dry_run=False)
    else:
        load_clinvar(export_to_es=True)

