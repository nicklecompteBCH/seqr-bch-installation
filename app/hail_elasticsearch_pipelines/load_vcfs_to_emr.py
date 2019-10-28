"""This module contains functions for loading data on the AWS EMR hail cluster used by the BCH hail installation.
"""

import boto3
import botocore
from urllib.parse import urlparse
import os
import time
import requests
from typing import Iterable, Union
from enum import Enum

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

# """ class SexChromosome(Enum):
#     X = 1,
#     Y = 2

#     def __str__(self):
#         if self == SexChromosome.X:
#             return "X"
#         elif self == SexChromosome.Y:
#             return "Y"
#         else:
#             raise ValueError(f"unexpected value for SexChromosome {self}")

# class AlleleId:

#     def __init__(
#         self,
#         chromosome: Union[Int, SexChromosome],
#         position: int,
#         change: str
#     ):
#         self.chromosome = chromosome
#         self.position = position
#         self.change = change

#     def __str__(self):
#         return f"{str(self.chromosome)}-{self.position}-{self.change}"



# class ElasticsearchVariant:

#     The fields that need to get exported to Elasticsearch.

#     """

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
    index_name = "clinvar_grch37" #"clinvar_grch{}".format(args.genome_version)
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

    goldstar_dict = hl.literal(CLINVAR_GOLD_STARS_LOOKUP)


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
        gold_stars= hl.int(goldstar_dict.get(review_status_str))
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
    if export_table_to_elasticsearch:
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

clinvar_mt = load_clinvar()

def annoate_with_clinvar(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.annotate_rows(
        allele_id = clinvar_mt.allele_id,
        clinical_significance = clinvar_mt.clinical_significance,
        gold_stars = clinvar_mt.gold_stars
    )
    return mt
        # vds = vds.annotate_variants_vds(clinvar_vds, expr="""
        # %(root)s.allele_id = vds.info.ALLELEID,
        # %(root)s.clinical_significance = vds.info.CLNSIG.toSet.mkString(","),
        # %(root)s.gold_stars = %(CLINVAR_GOLD_STARS_LOOKUP)s.get(vds.info.CLNREVSTAT.toSet.mkString(","))

def load_hgmd_vcf():

    mt = import_vcf(
        's3://seqr-resources/GRCh37/hgmd/hgmd_pro_2018.4_hg19.vcf.gz',
        "37",
        "hgmd_grch37",
    )
    return mt

hgmd_mt = load_hgmd()

def annotate_with_hgmd(mt: hl.MatrixTable) -> hl.MatrixTable:
    mt = mt.annotate_rows(
        hgmd=hl.struct(
            accession=hgmd_mt.rsid,
            #class=hgmd_mt.info.CLASS
        )
    )
    return mt


def determine_if_already_uploaded(dataset: SeqrProjectDataSet):
    resp = requests.get(ELASTICSEARCH_HOST + ":9200/" + compute_index_name(dataset) + "0.5vep")
    if "index_not_found_exception" in resp.text:
        return False
    return True

def finalize_annotated_table_for_seqr_variants(vep_mt: hl.MatrixTable) -> hl.MatrixTable:
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
    # return vep_mt.select_rows(

    # )

    #         {  'gnomad_exomes_Hemi': None,
    #       'originalAltAlleles': [
    #         '1-248367227-TC-T'
    #       ],
    #       'hgmd_accession': None,
    #       'g1k_AF': None,
    #       'gnomad_genomes_Hom': 0,
    #       'cadd_PHRED': 25.9,
    #       'exac_AC_Hemi': None,
    #       'g1k_AC': None,
    #       'topmed_AN': 125568,
    #       'g1k_AN': None,
    #       'topmed_AF': 0.00016724,
    #       'dbnsfp_MutationTaster_pred': None,
    #       'ref': 'TC',
    #       'exac_AC_Hom': 0,
    #       'topmed_AC': 21,
    #       'dbnsfp_REVEL_score': None,
    #       'primate_ai_score': None,
    #       'variantId': '1-248367227-TC-T',
    #       'sortedTranscriptConsequences': [
    #         {
    #           'amino_acids': 'LL/L',
    #           'biotype': 'nonsense_mediated_decay',
    #           'lof': None,
    #           'lof_flags': None,
    #           'major_consequence_rank': 10,
    #           'codons': 'ctTCTc/ctc',
    #           'gene_symbol': 'MFSD9',
    #           'domains': [
    #             'Transmembrane_helices:TMhelix',
    #             'Gene3D:1',
    #           ],
    #           'canonical': None,
    #           'transcript_rank': 1,
    #           'cdna_end': 143,
    #           'lof_filter': None,
    #           'hgvs': 'ENSP00000413641.1:p.Leu48del',
    #           'hgvsc': 'ENST00000428085.1:c.141_143delTCT',
    #           'cdna_start': 141,
    #           'transcript_id': 'ENST00000428085',
    #           'protein_id': 'ENSP00000413641',
    #           'category': 'missense',
    #           'gene_id': 'ENSG00000135953',
    #           'hgvsp': 'ENSP00000413641.1:p.Leu48del',
    #           'major_consequence': 'frameshift_variant',
    #           'consequence_terms': [
    #             'frameshift_variant',
    #             'inframe_deletion',
    #             'NMD_transcript_variant'
    #           ]
    #         },
    #         {
    #           'amino_acids': 'P/X',
    #           'biotype': 'protein_coding',
    #           'lof': None,
    #           'lof_flags': None,
    #           'major_consequence_rank': 4,
    #           'codons': 'Ccc/cc',
    #           'gene_symbol': 'OR2M3',
    #           'domains': [
    #               'Transmembrane_helices:TMhelix',
    #               'Prints_domain:PR00237',
    #           ],
    #           'canonical': 1,
    #           'transcript_rank': 0,
    #           'cdna_end': 897,
    #           'lof_filter': None,
    #           'hgvs': 'ENSP00000389625.1:p.Leu288SerfsTer10',
    #           'hgvsc': 'ENST00000456743.1:c.862delC',
    #           'cdna_start': 897,
    #           'transcript_id': 'ENST00000456743',
    #           'protein_id': 'ENSP00000389625',
    #           'category': 'lof',
    #           'gene_id': 'ENSG00000228198',
    #           'hgvsp': 'ENSP00000389625.1:p.Leu288SerfsTer10',
    #           'major_consequence': 'frameshift_variant',
    #           'consequence_terms': [
    #               'frameshift_variant'
    #           ]
    #         }
    #       ],
    #       'hgmd_class': None,
    #       'AC': 2,
    #       'exac_AN_Adj': 121308,
    #       'mpc_MPC': None,
    #       'AF': 0.063,
    #       'alt': 'T',
    #       'clinvar_clinical_significance': None,
    #       'rsid': None,
    #       'dbnsfp_DANN_score': None,
    #       'AN': 32,
    #       'gnomad_genomes_AF_POPMAX_OR_GLOBAL': 0.0004590314436538903,
    #       'exac_AF': 0.00006589,
    #       'dbnsfp_GERP_RS': None,
    #       'dbnsfp_SIFT_pred': None,
    #       'exac_AC_Adj': 8,
    #       'g1k_POPMAX_AF': None,
    #       'topmed_Hom': 0,
    #       'gnomad_genomes_AN': 30946,
    #       'dbnsfp_MetaSVM_pred': None,
    #       'dbnsfp_Polyphen2_HVAR_pred': None,
    #       'clinvar_allele_id': None,
    #       'gnomad_exomes_Hom': 0,
    #       'gnomad_exomes_AF_POPMAX_OR_GLOBAL': 0.0009151523074911753,
    #       'gnomad_genomes_Hemi': None,
    #       'xpos': 1248367227,
    #       'start': 248367227,
    #       'filters': [],
    #       'dbnsfp_phastCons100way_vertebrate': None,
    #       'gnomad_exomes_AN': 245930,
    #       'contig': '1',
    #       'clinvar_gold_stars': None,
    #       'eigen_Eigen_phred': None,
    #       'exac_AF_POPMAX': 0.0006726888333653661,
    #       'gnomad_exomes_AC': 16,
    #       'dbnsfp_FATHMM_pred': None,
    #       'gnomad_exomes_AF': 0.00006505916317651364,
    #       'gnomad_genomes_AF': 0.00012925741614425127,
    #       'gnomad_genomes_AC': 4,
    #       'genotypes': [
    #         {
    #           'num_alt': 2,
    #           'ab': 1,
    #           'dp': 74,
    #           'gq': 99,
    #           'sample_id': 'NA20870',
    #         },
    #         {
    #           'num_alt': 0,
    #           'ab': 0,
    #           'dp': 88,
    #           'gq': 99,
    #           'sample_id': 'HG00731',
    #         },
    #         {
    #             'num_alt': 1,
    #             'ab': 0.631,
    #             'dp': 50,
    #             'gq': 99,
    #             'sample_id': 'NA20885',
    #         },
    #       ],
    #       'samples_num_alt_1': ['NA20885'],
    #       'samples_num_alt_2': ['NA20870'],}


    raise NotImplementedError()

def add_project_dataset_to_elastic_search(
    dataset: SeqrProjectDataSet,
    host, index_name, index_type="variant",
    port=9200, num_shards=12, block_size=200):
    client = ElasticsearchClient(host=host,port=port)

    vcf_mt = add_vcf_to_hail(dataset.vcf_s3_path)
    vcf = add_global_metadata(vcf_mt,dataset.vcf_s3_path)
    index_name = compute_index_name(dataset)
    vep_mt = add_vep_to_vcf(vcf)

        # """
        # 'genotypes': [
        #     {
        #       'num_alt': 2,
        #       'ab': 1,
        #       'dp': 74,
        #       'gq': 99,
        #       'sample_id': 'NA20870',
        #     },
        # """

    vep_mt = vep_mt.annotate_entries(
        genotypes = hl.Struct(**{
            'num_alt' : vep_mt.info.NS,
            'ab':vep_mt.info.AO,
            'dp': vep_mt.info.DP,
            'gq': vep_mt.qual,
            'sample_id': hl.literal(parse_vcf_s3_path(dataset.vcf_s3_path)['filename'])
        })
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

