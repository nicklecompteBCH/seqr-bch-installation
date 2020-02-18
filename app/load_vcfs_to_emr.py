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
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.elasticsearch_client import ELASTICSEARCH_UPSERT, ELASTICSEARCH_UPDATE, ELASTICSEARCH_INDEX
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
from hail_elasticsearch_pipelines.bch_refactor.cloud.s3_tools import parse_vcf_s3_path
from hail_elasticsearch_pipelines.bch_refactor.hail_ops  import add_global_metadata
from hail_elasticsearch_pipelines.bch_refactor.add_gnomad_to_vep_results import (
        read_gnomad_ht, GnomadDataset, annotate_with_gnomad
)
from hail_elasticsearch_pipelines.bch_refactor.cadd import (
        get_cadd, annotate_with_cadd
)
from hail_elasticsearch_pipelines.bch_refactor.eigen import (
    get_eigen, annotate_with_eigen
)

from hail_elasticsearch_pipelines.bch_refactor.primate_ai import (
    import_primate, annotate_with_primate
)

from hail_elasticsearch_pipelines.bch_refactor.clinvar import (
    load_clinvar, annotate_with_clinvar
)

from hail_elasticsearch_pipelines.bch_refactor.topmed import (
    get_topmed, annotate_with_topmed
)

from hail_elasticsearch_pipelines.bch_refactor.mpc import (
    get_mpc, annotate_with_mpc
)

from hail_elasticsearch_pipelines.bch_refactor.exac import (
    get_exac, annotate_with_exac
)

from hail_elasticsearch_pipelines.bch_refactor.gene_constraint import (
    get_gc, annotate_with_gc
)

from hail_elasticsearch_pipelines.bch_refactor.omim import (
    get_omim, annotate_with_omim
)

from hail_elasticsearch_pipelines.bch_refactor.gene_constraint import (
    get_gc, annotate_with_gc
)

from hail_elasticsearch_pipelines.bch_refactor.hail_ops import (
    add_vcf_to_hail
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

def add_vcf_to_hdfs(s3path_to_vcf):

    parts = parse_vcf_s3_path(s3path_to_vcf)
    if hl.utils.hadoop_exists("hdfs:///user/hadoop/" +  parts['filename']):
        return parts['filename']
    s3buckets = boto3.resource('s3')
    s3bucket = s3buckets.Bucket(parts['bucket'])
    s3bucket.download_file(parts['path'], parts['filename'])
    os.system('hdfs dfs -put ' + parts['filename'])
    print(parts['filename'])
    os.system('rm ' + parts['filename'])
    return parts['filename']

def add_seqr_sample_to_hadoop(sample: SeqrSample):
    return add_vcf_to_hdfs(sample.path_to_vcf)

def add_seqr_sample_to_locals3(sample: SeqrSample):
    parts = parse_vcf_s3_path(sample.path_to_vcf)
    local_bucket = "seqr-data"
    local_filename = "vcfs/" + str(sample.project) + "/" + parts['filename']
    s3 = boto3.client('s3')
    maybe_list = s3.list_objects(
        Bucket=local_bucket,
        EncodingType='url',
        Prefix=local_filename,
        RequestPayer='requester'
    )
    if 'Contents' in maybe_list and maybe_list['Contents']:
        return local_filename
    else:
        copy_source = {
            'Bucket': parts['bucket'],
            'Key': parts['path']
        }
        s3.copy(copy_source, local_bucket, local_filename)
        return local_filename

def add_family_to_hail(family:SeqrFamily) -> hl.MatrixTable:
    fanmar : hl.MatrixTable = None
    for sample in family.samples:
        filename = add_seqr_sample_to_locals3(sample)
        mt = add_vcf_to_hail(sample, "s3n://seqr-data/" + filename)
        if not fanmar:
            fanmar = mt
        fanmar = fanmar.union_cols(mt)
    return fanmar

def add_families_to_hail(families: List[SeqrFamily]) -> hl.MatrixTable:
    retmr : hl.MatrixTable = None
    for family in families:
        mt = add_family_to_hail(family)
        if not retmr:
            if len(families) == 1:
                return mt
            else:
                retmr = mt # I hate python :/
        else:
            retmr = retmr.union_cols(mt)
    return retmr



def add_vep_to_vcf(mt):
    mt = run_vep(mt, GENOME_VERSION)
    return mt

def load_hgmd_vcf(partitions : int = None,namenode:str=""):

    mt = import_vcf(
        'hdfs://' + namenode + '/user/hadoop/data/hgmd_pro_2018.4_hg19.vcf.gz',
        "37",
        "hgmd_grch37",min_partitions=partitions, force_bgz=True
    )
    return mt


def annotate_with_hgmd(mt: hl.MatrixTable, hgmd_mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.annotate_rows(
        hgmd=hl.struct(
            accession=hgmd_mt.index_rows(mt.row_key).rsid,
            hgmdclass = hgmd_mt.index_rows(mt.row_key).info.CLASS
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
    mt = mt.annotate_rows(
        samples_num_alt_1 = (mt.genotypes.filter(lambda s: s.num_alt == 1).map(lambda s: s.sample_id)),
        samples_num_alt_2 = (mt.genotypes.filter(lambda s: s.num_alt == 2).map(lambda s: s.sample_id)),
        samples_num_alt_3 = (mt.genotypes.filter(lambda s: s.num_alt > 2).map(lambda s: s.sample_id))
    )
    return mt

def annotate_with_genotype_num_alt(mt: hl.MatrixTable) -> hl.MatrixTable:
    if 'AD' in set(mt.entry):
        # GATK-consistent VCF
        mt = mt.annotate_rows(
            genotypes = (hl.agg.collect(hl.struct(
                num_alt = hl.cond(mt.alleles[1] == '<CNV>', 0, mt.GT.n_alt_alleles()),
                ab = hl.cond(mt.alleles[1] == '<CNV>', 0.0, hl.float(hl.array(mt.AD)[1])/hl.float(hl.fold(lambda i, j : i + j, 0, mt.AD))),
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
    else:
        raise ValueError("unrecognized vcf")
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
    return mt

def export_table_to_tsv(final : hl.MatrixTable, index_prefix: str):
    print("Flattening MatrixTable...")
    final_t = final.rows().flatten()#.drop('locus','allele') # row fields already annotated by sample
    print("Coaelescning...")
    final_t = final_t.naive_coalesce(200)
    print("Persisting Table...")
    final_t = final_t.persist()
    print("Uploading file...")
    filename = 's3n://seqr-data/' + index_prefix + ".tsv.bgz"
    final_export = final_t.export(filename)
    print('uploaded file!')

def export(t,index_name, tsv:bool,tsves:bool,op=ELASTICSEARCH_INDEX):
    # First export VCF to S3:
    if tsv or tsves:
        export_table_to_tsv(t, index_name)
    if (not tsv) or tsves:
        export_table_to_elasticsearch(
            t,
            ELASTICSEARCH_HOST,
            index_name,
            "variant",
            op,
            is_vds=True,
            port=9200,
            num_shards=2,
            block_size=1000
        )


if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("-clinvar", "--clinvar", help="Run clinvar instead of loading samples",  action="store_true")
    p.add_argument("-p","--path",help="Filepath of csv from BCH_Connect seqr report.")
    p.add_argument("-proj","--project")
    p.add_argument("-tsv","--tsv", action="store_true")
    p.add_argument("-te","--tsves",action="store_true")
    p.add_argument("-parts","--partitions")
    p.add_argument("-nn","--namenode")
    p.add_argument("-ip","--index_prefix")
    args = p.parse_args()
    print(str(hl.utils.hadoop_ls('/')))
    if not args.clinvar:
        #gnomad.describe()
        partition_base = int(args.partitions)
        nn = args.namenode

        gnomad = read_gnomad_ht(GnomadDataset.Exomes37,partitions=partition_base,namenode = nn)


        # CADD seems hairy for whatever reason
        # do partition_base * 10
        #cadd : hl.Table =
        # Eigen is also hairy
        # It may bee that these are
        #eigen : hl.MatrixTable =
        #hgmd : hl.MatrixTable = load_hgmd_vcf(partitions=partition_base,namenode=nn)
        #primate : hl.MatrixTable =
        #clinvar : hl.MatrixTable =
        #topmed : hl.MatrixTable =
        #mpc : hl.MatrixTable = get_mpc()
        #exac : hl.MatrixTable =
        #gc : hl.MatrixTable =  get_gc()
        #omim = get_omim()


        path = args.path
        families = bch_connect_report_to_seqr_families(path)
        num_vcfs = len(family.samples)
        partition_count = num_vcfs*partition_base
        dataset = args.project
        index_name = dataset + "__wes__" + "GRCh37__" + "VARIANTS__" + time.strftime("%Y%m%d")
        index_name = index_name.lower()
        if args.index_prefix:
            index_name = args.index_prefix + index_name
        mt = add_families_to_hail(families)
        mt = mt.persist()
        print("Added families")

        print("Adding vep")
        mt = add_vep_to_vcf(mt)
        #partition_count = parition_count + num_vcfs # assume each annotation adds a VCF's worth of data per VCF
        mt = mt.persist()
        print("Added vep")
        partition_count = 2*partition_count # VEP adds "twice" as much info (ish)
        print("Repartitioning")
        mt = mt.repartition(partition_count)
        mt = mt.persist()

        mt = annotate_with_genotype_num_alt(mt)
        mt = annotate_with_samples_alt(mt)
        mt = finalize_annotated_table_for_seqr_variants(mt)
        mt = mt.persist()
        print("Added custom fields")
        print("Repartitioning...")
        mt = mt.repartition(partition_count)

        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_INDEX)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.locus,mt.alleles,mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting Clinvar...")
        clinvar = load_clinvar(partitions=partition_base,namenode = nn).semi_join_rows(mt.rows())
        clinvar = clinvar.persist()
        print("Adding Clinvar...")
        mt = annotate_with_clinvar(mt, clinvar)
        mt = mt.persist()
        print("Added clinvar, unpersisting...")
        clinvar = clinvar.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()


        print("Subsetting and perrsisting HGMD...")
        hgmds = hgmd.semi_join_rows(mt.rows())
        hgmds = hgmds.persist()
        print("Adding HGMD...")
        mt = annotate_with_hgmd(mt, hgmds)
        mt = mt.persist()
        print("Added HGMD, unpersisting")
        hgmds = hgmds.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting Gnomad...")
        gnomad = gnomad.semi_join_rows(mt.rows())

        gnomad = gnomad.persist()
        mt = annotate_with_gnomad(mt, gnomad)
        mt = mt.persist()
        print("Added Gnomad, unpersisting...")
        gnomad = gnomad.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting eigen...")
        eigen = get_eigen(partitions=partition_base,namenode = nn).semi_join_rows(mt.rows())
        eigen = eigen.persist()
        print("Adding eigen...")
        mt = annotate_with_eigen(mt, eigen)
        mt  = mt.persist()
        print("Added Eigen, unpersisting...")
        eigen = eigen.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting Primate...")
        primate = import_primate(partitions=partition_base,namenode = nn).semi_join_rows(mt.rows())
        primate = primate.persist()
        print("Adding primate...")
        mt = annotate_with_primate(mt, primate)
        mt = mt.persist()
        print("Added primate, unpersisting")
        primate = primate.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting TopMed...")
        topmed = get_topmed(partitions=partition_base,namenode = nn).semi_join_rows(mt.rows())
        topmed = topmed.persist()
        print("Adding topmed")
        mt = annotate_with_topmed(mt, topmed)
        mt = mt.persist()
        print("Added topmed, unpersisting...")
        topmed = topmed.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subsetting and persisting ExAc...")
        exac = get_exac(partitions=partition_base,namenode = nn).semi_join_rows(mt.rows())
        exac = exac.persist()
        print("Adding Exac...")
        mt = annotate_with_exac(mt, exac)
        mt = mt.persist()
        print("added exac, unpersisting")
        exac = exac.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()

        print("Subdsetting and persisting CADD")
        cadd = get_cadd(partitions=partition_base,namenode = nn).semi_join(mt.rows())
        cadd = cadd.persist()
        print("Adding CADD...")
        mt = annotate_with_cadd(mt, cadd)
        mt = mt.persist()
        print("Added CADD, unpersisting...")
        cadd = cadd.unpersist()
        print("Exporting partial results to elasticsearch")
        export(mt,index_name,args.tsv,args.tsves,ELASTICSEARCH_UPSERT)
        print("Dropping all non-essential fields")
        mt = mt.select_rows(mt.variant_id)
        mt = mt.persist()


        # print("added exac, unpersisting")
        # exacs = exacs.unpersist()
        #final = final.unpersist()
        # final = mt.persist()
        # #mt = mt.persist()


        # print("Preparing for export")
        # #final = final.repartition(40000) # let's try this out....
        # #famids = list(map(lambda x: x.family_id, families))
        # export(final,index_name, args.tsv,args.tsves)

    else:
        load_clinvar(es_host=ELASTICSEARCH_HOST)
