from .seqr_dataset import SeqrFamily, SeqrSample

import boto3
import botocore

import hail as hl

client = boto3.client('emr')
s3client = boto3.client('s3')


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

def load_seqr_family_to_hail(family: SeqrFamily) -> hl.MatrixTable:
    index_vcf = family.index_sample.path_to_vcf


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