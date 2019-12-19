"""This module contains functions for loading data on the AWS EMR hail cluster used by the BCH hail installation.
"""

import boto3
import botocore
from urllib.parse import urlparse
import os
import time
import requests
from typing import Iterable, Union, List
from enum import Enum

from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf, run_vep
from hail_elasticsearch_pipelines.hail_scripts.v02.export_table_to_es import export_table_to_elasticsearch
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.variant_id import *
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.vep import (
    get_expr_for_vep_gene_id_to_consequence_map,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
    get_expr_for_vep_consequence_terms_set,
    get_expr_for_vep_protein_domains_set,
    get_expr_for_vep_transcript_ids_set,
    get_expr_for_vep_transcript_id_to_consequence_map,
    get_expr_for_vep_gene_ids_set
)
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.clinvar import CLINVAR_GOLD_STARS_LOOKUP, download_and_import_latest_clinvar_vcf

from hail_elasticsearch_pipelines.bch_refactor.seqr_utils.seqr_dataset import *
import csv
import hail as hl
from hail_elasticsearch_pipelines.bch_refactor.cloud.s3_tools import parse_vcf_s3_path, add_vcf_to_hdfs
from hail_elasticsearch_pipelines.bch_refactor.hail_ops  import add_global_metadata
from hail_elasticsearch_pipelines.bch_refactor.add_gnomad_to_vep_results import (
        annotate_adj, read_gnomad_ht, GnomadDataset, annotate_with_gnomad
)
from hail_elasticsearch_pipelines.bch_refactor.cadd import (
        get_cadd, annotate_with_cadd
)
from hail_elasticsearch_pipelines.bch_refactor.eigen import (
    get_eigen, annotate_with_eigen
)


BCH_CLUSTER_TAG = "bch-hail-cluster"
BCH_CLUSTER_NAME = 'hail-bch'
GENOME_VERSION = '37'
ELASTICSEARCH_HOST=os.environ['ELASTICSEARCH_HOST']

client = boto3.client('emr')
s3client = boto3.client('s3')


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


def add_vcf_to_hail(filename, family_name, s3path_to_vcf):
    print("Trying to add " + str(filename) + " to hail...")
    mt = import_vcf(
        filename,
        GENOME_VERSION,
        family_name,
        force_bgz=True,
        min_partitions=1000)
    mt = add_global_metadata(mt, s3path_to_vcf)

    return mt

def add_seqr_sample_to_hadoop(sample: SeqrSample):
    add_vcf_to_hdfs(sample.path_to_vcf)


def add_seqr_family_to_hail(family: SeqrFamily) -> hl.MatrixTable:
    filenames : List[str] = []
    for sample in family.samples:
        add_seqr_sample_to_hadoop(sample)
        parts = parse_vcf_s3_path(sample.path_to_vcf)
        filenames.append(parts['filename'])
    mt = add_vcf_to_hail(filenames,family.family_id,family.index_sample.path_to_vcf)
    return mt


def add_vep_to_vcf(mt):
    mt = run_vep(mt, GENOME_VERSION)
    return mt

def load_hgmd_vcf():
    s3client.download_file('seqr-resources','GRCh37/hgmd/hgmd_pro_2018.4_hg19.vcf.gz','/tmp/hgmd.vcf.gz')

    mt = import_vcf(
        '/tmp/hgmd.vcf.gz',
        "37",
        "hgmd_grch37",
    )
    return mt


def annotate_with_hgmd(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.annotate_rows(
        hgmd=hl.struct(
            accession=hgmd_mt.rsid,
            hgmdclass = hgmd_mt.info.CLASS
        )
    )
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


def bch_connect_csv_line_to_seqr_sample(inputline: dict) -> SeqrSample:
# From the REDCap report:
# record_id,redcap_repeat_instrument,redcap_repeat_instance,
# de_identified_subject,family_name,
# processed_bam,processed_vcf,investigator,
# elasticsearch_import_yn,elasticsearch_index,import_seqr_yn,seqr_project_name,seqr_id,seqr_failure_log
    indiv_id = inputline['de_identified_subject']
    fam_id = inputline['family_name']
    vcf_s3_path = inputline['processed_vcf']
    bam_s3_path = inputline['processed_bam']
    project_name = inputline['investigator']
    project = BCHSeqrProject.from_string(project_name)
    family_member_type = FamilyMemberType.from_bchconnect_str(inputline['initial_study_participant_kind'])

    return SeqrSample(
        indiv_id, fam_id, project, family_member_type,
        vcf_s3_path, bam_s3_path
    )


def bch_connect_report_to_seqr_families(filepath) -> List[SeqrFamily]:
    samples : List[SeqrSample] = []

    with open(filepath, 'r') as connect_results:
        for row in csv.DictReader(connect_results):
            sample = bch_connect_csv_line_to_seqr_sample(row)
            samples.append(sample)
    grouped_by_family = group_by(samples, lambda x: x.family_id)
    families : List[SeqrFamily] = list(map(lambda famlist: SeqrFamily.from_list_samples(famlist),grouped_by_family.values()))
    return families


def group_by(input_list, key_function):
    ret_dict = {}
    for v in input_list:
        key = key_function(v)
        if key in ret_dict:
            ret_dict[key].append(v)
        else:
            ret_dict.update({key:[v]})
    return ret_dict


def compute_index_name(dataset: SeqrProjectDataSet, sample_type='wes',dataset_type='VARIANTS'):
    """Returns elasticsearch index name computed based on command-line args"""

    # generate the index name as:  <project>_<WGS_WES>_<family?>_<VARIANTS or SVs>_<YYYYMMDD>_<batch>
    index_name = "%s%s%s__%s__grch%s__%s__%s" % (
        dataset.project_name,
        "__"+dataset.fam_id,  # optional family id
        sample_type,
        'GRCh37',
        dataset_type,
        time.strftime("%Y%m%d"),

    )

    index_name = index_name.lower()  # elasticsearch requires index names to be all lower-case

    return index_name

def annotate_with_samples_alt(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt.describe()
    mt.rows().head(10).show()
    mt = mt.annotate_rows(
        samples_num_alt_1 = (mt.genotypes.filter(lambda s: s.num_alt == 1).map(lambda s: s.sample_id)),
        samples_num_alt_2 = (mt.genotypes.filter(lambda s: s.num_alt == 2).map(lambda s: s.sample_id)),
        samples_num_alt_3 = (mt.genotypes.filter(lambda s: s.num_alt > 2).map(lambda s: s.sample_id))
    )
    return mt

def annotate_with_genotype_num_alt(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt.describe()
    mt.head(10).show()
    if 'AD' in set(mt.entry):
        # GATK-consistent VCF
        mt = mt.annotate_rows(
            genotypes = (hl.agg.collect(hl.struct(
                num_alt = hl.cond(mt.alleles[1] == '<CNV>', 0, mt.GT.n_alt_alleles()),
                ab = hl.cond(mt.alleles[1] == '<CNV>', 0.0, hl.cond(hl.eval(hl.fold(lambda i, j : i + j, 0, mt.AD)) != 0 and hl.len(mt.AD) > 1, hl.float(mt.AD[1])/hl.float(hl.eval(hl.fold(lambda i, j : i + j, 0, mt.AD))), 0)),
                gq = mt.GQ,
                sample_id = mt.s,
                dp=mt.DP))
        )
    )
    elif 'AO' in set(mt.entry):

        mt = mt.annotate_rows(
            genotypes = hl.agg.collect(hl.struct(
                    num_alt=hl.cond(mt.alleles[1] == '<CNV>', 0, mt.GT.n_alt_alleles()),
                    ab=hl.cond(mt.alleles[1] == '<CNV>', 0.0, hl.float(mt.AO[0])/hl.float(mt.DP)),
                    dp = mt.DP,  gq = mt.GQ, sample_id = mt.s))) #hl.cond(mt.GT=="0/0",0,hl.cond(mt.GT=="1/0",1,hl.cond(mt.GT=="0/1",1,hl.cond((mt.GT=="1/1",2,hl.cond(mt.GT=="1/2",2,hl.cond(mt.GT=="2/1",2,hl.cond(mt.GT=="2/2",2,-1))))))))
    return mt



def load_clinvar(export_to_es=False):
    index_name = "clinvar_grch37" #"clinvar_grch{}".format(args.genome_version)
    mt = download_and_import_latest_clinvar_vcf("37")
    mt = hl.vep(mt, "/vep85-loftee-gcloud.json", name="vep", block_size=1000)
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

    goldstar_dict = hl.literal(CLINVAR_GOLD_STARS_LOOKUP)



    mt = mt.annotate_rows(
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
        gold_stars= hl.int(goldstar_dict.get(review_status_str)),
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

    mt = mt.annotate_rows(clinvar_clinical_significance = mt.clinical_significance)

    hl.summarize_variants(mt)

        # Drop key columns for export
    if export_to_es:
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

CLINVAR_GOLD_STARS_LOOKUP = {
    'no_interpretation_for_the_single_variant': "0",
    'no_assertion_provided' : "0",
    'no_assertion_criteria_provided' : "0",
    'criteria_provided,_single_submitter' : "1",
    'criteria_provided,_conflicting_interpretations' : "1",
    'criteria_provided,_multiple_submitters,_no_conflicts' : "2",
    'reviewed_by_expert_panel' : "3",
    'practice_guideline' : "4"

}

#clinvar_mt = load_clinvar()

def annoate_with_clinvar(mt: hl.MatrixTable) -> hl.MatrixTable:
    #joined_mt = clinvar_mt.semi_join_rows(mt)
    clinvar_mt.describe()
    mt = mt.annotate_rows(
        allele_id = clinvar_mt.index_rows(mt.row_key).allele_id,
        clinvar_clinical_significance = clinvar_mt.index_rows(mt.row_key).clinvar_clinical_significance,
        gold_stars = clinvar_mt.index_rows(mt.row_key).gold_stars
    )
    return mt
        # vds = vds.annotate_variants_vds(clinvar_vds, expr="""
        # %(root)s.allele_id = vds.info.ALLELEID,
        # %(root)s.clinical_significance = vds.info.CLNSIG.toSet.mkString(","),
        # %(root)s.gold_stars = %(CLINVAR_GOLD_STARS_LOOKUP)s.get(vds.info.CLNREVSTAT.toSet.mkString(","))

def determine_if_already_uploaded(dataset: SeqrProjectDataSet):
    resp = requests.get(ELASTICSEARCH_HOST + ":9200/" + compute_index_name(dataset) + "0.5vep")
    if "index_not_found_exception" in resp.text:
        return False
    return True

def finalize_annotated_table_for_seqr_variants(mt: hl.MatrixTable) -> hl.MatrixTable:
    """Given a messily-but-completely annotated Hail MatrixTable of variants,
    return a new MatrixTable with appropriate formatting to export to Elasticsearch
    and consume  by Seqr.

    TO-EXTREMELY-DO: Create a app/common Python 3 module with code for SeqrAnnotatedVariant,
    with methods to im/export to/from Hail/Elasticsearch.

    :param vep_mt: A VCF loaded into hail 0.2, VEP has been run,
    and reference/computed fields have been added.
    :type vep_mt: hl.MatrixTable
    :return: A hail matrix table of variants and VEP annotations with
    proper formatting to be consumed by Seqr.
    :rtype: hl.MatrixTable
    """
    mt = mt.annotate_rows(
        sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep)
    )




    mt = mt.annotate_rows(
        #allele_id=clinvar_mt.index_rows(mt.row_key).vep.id,
        alt=get_expr_for_alt_allele(mt),
        chrom=get_expr_for_contig(mt.locus),
        #clinvar_clinical_significance=clinvar_mt.index_rows(mt.row_key).clinical_significance,
        domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=mt.vep.transcript_consequences),
        #gene_ids=clinvar_mt.index_rows(mt.locus,mt.alleles).gene_ids,
        # gene_id_to_consequence_json=get_expr_for_vep_gene_id_to_consequence_map(
        #     vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences,
        #     gene_ids=clinvar_mt.gene_ids
        # ),
        #gold_stars= clinvar_mt.index_entries(mt.row_key,mt.col_key).gold_stars,
        pos=get_expr_for_start_pos(mt),
        ref=get_expr_for_ref_allele(mt),
        #review_status=clinvar_mt.index_rows(mt.locus,mt.alleles).review_status,
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
    mt = annotate_with_genotype_num_alt(mt)
    mt = annotate_with_samples_alt(mt)
    mt.describe()
    return mt

gnomad = read_gnomad_ht(GnomadDataset.Exomes37)
cadd = get_cadd()

def add_project_dataset_to_elastic_search(
    dataset: SeqrProjectDataSet,
    host, index_name, index_type="variant",
    port=9200, num_shards=12, block_size=200):
    client = ElasticsearchClient(host=host,port=port)

    filename = parse_vcf_s3_path(dataset.vcf_s3_path)['filename']
    vcf_mt = add_vcf_to_hail(filename, dataset.fam_id, dataset.vcf_s3_path)
    vcf = add_global_metadata(vcf_mt,dataset.vcf_s3_path)
    index_name = compute_index_name(dataset)
    vep_mt = add_vep_to_vcf(vcf)
    clinvar_mt = annoate_with_clinvar(vep_mt)
    gnomad_mt = annotate_with_gnomad(clinvar_mt, gnomad)
    cadd_mt = annotate_with_cadd(gnomad_mt, cadd)
    final = finalize_annotated_table_for_seqr_variants(gnomad_mt)

    export_table_to_elasticsearch(vep_mt.rows(), host, index_name+"vep", index_type, is_vds=True, port=port,num_shards=num_shards, block_size=block_size)
#    export_table_to_elasticsearch(vep_mt.rows(), host, index_name+"vep", index_type, port=port, num_shards=num_shards, block_size=block_size)
    print("ES index name : %s, family : %s, individual : %s ",(index_name,dataset.fam_id, dataset.indiv_id))

if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("-clinvar", "--clinvar", help="Run clinvar instead of loading samples",  action="store_true")
    p.add_argument("-p","--path",help="Filepath of csv from BCH_Connect seqr report.")
    args = p.parse_args()
    print(str(hl.utils.hadoop_ls('/')))
    if not args.clinvar:
        #gnomad.describe()
        hgmd_mt = load_hgmd_vcf()
        hgmd_mt.describe()

        eigen_mt = get_eigen()
        eigen_mt.describe()

        path = args.path
        families = bch_connect_report_to_seqr_families(path)
        for family in families:
            mt = add_seqr_family_to_hail(family)
            mt = add_global_metadata(mt,family.index_sample.path_to_vcf)
            index_name = "alan_beggs__" + family.family_id + "__wes__" + "GRCh37__" + "VARIANTS__" + time.strftime("%Y%m%d")
            vep_mt = add_vep_to_vcf(mt)
            clinvar_mt = annoate_with_clinvar(vep_mt)
            mt = annotate_with_genotype_num_alt(clinvar_mt)
            mt = annotate_with_samples_alt(mt)
            gnomad_mt = annotate_adj(mt)
            final = finalize_annotated_table_for_seqr_variants(gnomad_mt)
            export_table_to_elasticsearch(final.rows(), ELASTICSEARCH_HOST, (index_name+"vep").lower(), "variant", is_vds=True, port=9200,num_shards=12, block_size=200)

    else:
        load_clinvar(export_to_es=True)
