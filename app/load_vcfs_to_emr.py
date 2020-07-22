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
from hail_elasticsearch_pipelines.bch_refactor.add_derived_fields import annotate_mt_with_derived_fields
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

from hail_elasticsearch_pipelines.bch_refactor.dbsnp import annotate_with_dbsnp
from hail_elasticsearch_pipelines.bch_refactor.onekg import annotate_with_onekg

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
    local_filename = "vcfs/" + str(sample.project) + "/" + parts['filename']
    if not hl.hadoop_is_file("hdfs:///user/hdfs/" + local_filename):
        os.system('aws s3 cp ' + sample.path_to_vcf + ' .')
        os.system('hdfs dfs -put ' + parts['filename'] + ' ' + local_filename)
        os.system('rm ' + parts['filename'])
    return local_filename

def add_family_to_hail(family:SeqrFamily,partition_count:int) -> hl.MatrixTable:
    fanmar : hl.MatrixTable = None
    for sample in family.samples:
        filename = add_seqr_sample_to_locals3(sample)
        mt = add_vcf_to_hail(sample, "hdfs:///user/hdfs/" + filename,partitions=partition_count)
        if not fanmar:
            if len(family.samples) == 1:
                return mt
            else:
                fanmar = mt
        else:
            fanmar = fanmar.union_cols(mt)
            fanmar = fanmar.naive_coalesce(partition_count)
    return fanmar

def add_families_to_hail(families: List[SeqrFamily],parts:int) -> hl.MatrixTable:
    retmr : hl.MatrixTable = None
    for family in families:
        mt = add_family_to_hail(family)
        if not retmr:
            if len(families) == 1:
                return mt
            else:
                retmr = mt # I hate python :/
                retmr = retmr.persist()
        else:
            retmr = retmr.union_cols(mt)
            retmr = retmr.naive_coalesce(parts)
            retmr = retmr.persist()
    return retmr



def add_vep_to_vcf(mt):
    mt = run_vep(mt, GENOME_VERSION)
    return mt


def annotate_with_hgmd(mt: hl.MatrixTable, hgmd_mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.annotate_rows(
        hgmd_accession=hgmd_mt.index(mt.row_key).rsid,
        hgmd_class = hgmd_mt.index(mt.row_key).info.CLASS
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
                    ab=hl.cond(mt.alleles[1] == '<CNV>' or mt.DP == 0, 0.0, hl.float(mt.AO[0])/hl.float(mt.DP)),
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
        mainTranscript=hl.cond(hl.len(mt.sortedTranscriptConsequences) > 0, mt.sortedTranscriptConsequences[0],hl.null("struct {biotype: str,canonical: int32,cdna_start: int32,cdna_end: int32,codons: str,gene_id: str,gene_symbol: str,hgvsc: str,hgvsp: str,transcript_id: str,amino_acids: str,lof: str,lof_filter: str,lof_flags: str,lof_info: str,polyphen_prediction: str,protein_id: str,protein_start: int32,sift_prediction: str,consequence_terms: array<str>,domains: array<str>,major_consequence: str,category: str,hgvs: str,major_consequence_rank: int32,transcript_rank: int32}")),
        #allele_id=clinvar_mt.index_rows(mt.row_key).vep.id,
        alt=get_expr_for_alt_allele(mt),
        chrom=get_expr_for_contig(mt.locus),
        #clinvar_clinical_significance=clinvar_mt.index_rows(mt.row_key).clinical_significance,
        domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=mt.vep.transcript_consequences),
        geneIds=hl.set(mt.vep.transcript_consequences.map(lambda c: c.gene_id)),
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

def export(mt,index_name, tsv:bool,tsves:bool,op=ELASTICSEARCH_INDEX):
    # First export VCF to S3:
    if tsv or tsves:
        export_table_to_tsv(mt, index_name)
    if (not tsv) or tsves:
        export_table_to_elasticsearch(mt,ELASTICSEARCH_HOST,index_name,"variant",op,is_vds=True,port=9200,num_shards=6,block_size=1000)

def table_exists(path):
    (hl.utils.hadoop_is_dir(filename) or hl.utils.hadoop_is_file(filename))# exists clinvar:

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
        for family in families:
            partition_count = partition_base
            dataset = args.project
            index_name = dataset + "_" + family.family_id + "__wes__" + "GRCh37__" + "VARIANTS__" + time.strftime("%Y%m%d")
            index_name = index_name.lower()
            filename = 'hdfs:///user/hdfs/out/' + dataset + family.family_id
            finalname = 'hdfs:///user/hdfs/final/' + dataset + family.family_id + ".mt"
            if hl.utils.hadoop_is_file(finalname + "/metadata.json.gz"):
                final = hl.read_matrix_table(finalname)
                export(final,index_name, args.tsv,args.tsves)
                continue
            mt = None
            if args.index_prefix:
                index_name = args.index_prefix + index_name
            if hl.utils.hadoop_is_file(filename + "_vep.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_vep.mt")
            else:
                mt = add_family_to_hail(family,partition_count) #add_families_to_hail(families,partition_count)
                print("Added families")

                print("Adding vep")
                mt = add_vep_to_vcf(mt)
                #partition_count = parition_count + num_vcfs # assume each annotation adds a VCF's worth of data per VCF
                print("Adding computed fields")
                mt = annotate_with_genotype_num_alt(mt)
                mt = annotate_with_samples_alt(mt)
                mt = annotate_mt_with_derived_fields(mt)
                print("Added vep, writing partial results")
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_vep.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_vep.mt")

            if hl.utils.hadoop_is_file(filename + "_gc.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_gc.mt")
            else:
                print("Adding gene constraint")
                gc = hl.read_table('hdfs:///user/hdfs/data/gc.ht')#.semi_join(mt.rows())
                mt = annotate_with_gc(mt,gc)
                mt = mt.write(filename + "_gc.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_gc.mt")


            if hl.utils.hadoop_is_file(filename + "_clinvar.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_clinvar.mt")
            else:
                print("Subsetting and persisting Clinvar...")
                clinvar = hl.read_matrix_table('hdfs:///user/hdfs/data/clinvar.mt').semi_join_rows(mt.rows())
                clinvar = clinvar.rows()
                print("Adding Clinvar...")
                mt = annotate_with_clinvar(mt, clinvar)
                mt = mt.write(filename + "_clinvar.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_clinvar.mt")

            if hl.utils.hadoop_is_file(filename + "_hgmd.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_hgmd.mt")
            else:
                print("Subsetting and persisting HGMD...")
                hgmd = hl.read_table('hdfs:///user/hdfs/data/hgmd.ht').semi_join(mt.rows())
                print("Adding HGMD...")
                mt = annotate_with_hgmd(mt, hgmd)
                mt = mt.write(filename + "_hgmd.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_hgmd.mt")

            if hl.utils.hadoop_is_file(filename + "_splice.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_splice.mt")
            else:
                print("Adding SpliceAI")
                sp = hl.read_matrix_table('hdfs:///user/hdfs/data/spliceai.mt').semi_join_rows(mt.rows())
                mt = mt.annotate_rows(
                    splice_ai_delta_score = sp.index_rows(mt.locus,mt.alleles).info.DS_AG
                )
                mt = mt.write(filename + "_splice.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_splice.mt")

            if hl.utils.hadoop_is_file(filename + "_dbsnp.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_dbsnp.mt")
            else:
                print("Subsetting and persisting DBSNP...")
                dbsnp =  hl.read_table('hdfs:///user/hdfs/data/dbsnp.ht').semi_join(mt.rows())
                mt = annotate_with_dbsnp(mt, dbsnp)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_dbsnp.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_dbsnp.mt")

            if hl.utils.hadoop_is_file(filename + "_caddind.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_caddind.mt")
            else:
                print("Subdsetting and persisting CADD indels")
                cadd = hl.read_table('hdfs:///user/hdfs/data/cadd_indels.mt').semi_join(mt.rows())
                print("Adding CADD...")
                mt = annotate_with_cadd(mt, cadd)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_caddind.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_caddind.mt")

            if hl.utils.hadoop_is_file(filename + "_caddsnv.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_caddsnv.mt")
            else:
                print("Subdsetting and persisting CADD SNV")
                cadd = hl.read_table('hdfs:///user/hdfs/data/cadd_snv.mt').semi_join(mt.rows())
                print("Adding CADD...")
                mt = annotate_with_cadd(mt, cadd)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_caddsnv.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_caddsnv.mt")

            if hl.utils.hadoop_is_file(filename + "_gnomad.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_gnomad.mt")
            else:
                print("Subsetting and persisting Gnomad...")
                gnomad =  hl.read_matrix_table('hdfs:///user/hdfs/data/gnomad.mt').semi_join_rows(mt.rows())
                mt = annotate_with_gnomad(mt, gnomad)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_gnomad.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_gnomad.mt")

            if hl.utils.hadoop_is_file(filename + "_eigen.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_eigen.mt")
            else:
                print("Subsetting and persisting eigen...")
                eigen =  hl.read_matrix_table('hdfs:///user/hdfs/data/eigen.mt').semi_join_rows(mt.rows())
                print("Adding eigen...")
                mt = annotate_with_eigen(mt, eigen)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_eigen.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_eigen.mt")

            if hl.utils.hadoop_is_file(filename + "_primate.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_primate.mt")
            else:
                print("Subsetting and persisting Primate...")
                primate =  hl.read_matrix_table('hdfs:///user/hdfs/data/primate.mt').semi_join_rows(mt.rows())
                print("Adding primate...")
                mt = annotate_with_primate(mt, primate)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_primate.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_primate.mt")

            if hl.utils.hadoop_is_file(filename + "_topmed.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_topmed.mt")
            else:
                print("Subsetting and persisting TopMed...")
                topmed =  hl.read_matrix_table('hdfs:///user/hdfs/data/topmed.mt').semi_join_rows(mt.rows())
                print("Adding topmed")
                mt = annotate_with_topmed(mt, topmed)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_topmed.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_topmed.mt")

            if hl.utils.hadoop_is_file(filename + "_exac.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_exac.mt")
            else:
                print("Subsetting and persisting ExAc...")
                exac =  hl.read_matrix_table('hdfs:///user/hdfs/data/exac.mt').semi_join_rows(mt.rows())
                print("Adding Exac...")
                mt = annotate_with_exac(mt, exac)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_exac.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_exac.mt")

            if hl.utils.hadoop_is_file(filename + "_mpc.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_mpc.mt")
            else:
                print("Subsetting and persisting MPC...")
                mpc =  hl.read_matrix_table('hdfs:///user/hdfs/data/mpc.mt').semi_join_rows(mt.rows())
                print("Adding MPC...")
                mt = annotate_with_mpc(mt, mpc)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_mpc.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_mpc.mt")

            if hl.utils.hadoop_is_file(filename + "_okg.mt/metadata.json.gz"):
                mt = hl.read_matrix_table(filename + "_okg.mt")
            else:
                print("Subsetting and persisting 1kg...")
                okg =  hl.read_matrix_table('hdfs:///user/hdfs/data/onekg.mt').semi_join_rows(mt.rows())
                print("Adding 1kg...")
                mt = annotate_with_onekg(mt, okg)
                mt = mt.repartition(partition_count)
                mt = mt.write(filename + "_okg.mt",overwrite=True)
                mt = hl.read_matrix_table(filename + "_okg.mt")

            print("Subsetting and persisting Omim...")
            omim =  hl.read_matrix_table('hdfs:///user/hdfs/data/omim.mt')
            print("Adding Omim...")
            mt = annotate_with_omim(mt, omim)

            final = mt
            #mt = mt.persist(

            print("Preparing for export")
            final.write(finalname)
            print("Done with family " + family.family_id)
            #final = final.repartition(40000) # let's try this out....
            #famids = list(map(lambda x: x.family_id, families))
            #export(final,index_name, args.tsv,args.tsves)

    else:
        load_clinvar(es_host=ELASTICSEARCH_HOST)
