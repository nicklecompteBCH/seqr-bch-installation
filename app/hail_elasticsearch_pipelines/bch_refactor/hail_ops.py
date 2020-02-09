from .seqr_dataset import SeqrFamily, SeqrSample

import boto3
import botocore

from typing import Union

import hail as hl

from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import  import_vcf

client = boto3.client('emr')
s3client = boto3.client('s3')


def add_global_metadata(
    vds : Union[hl.MatrixTable,hl.Table],
    s3bucket : str,
    family_name : str,
    individual_id : str,
    genomeVersion="37",
    sampleType="WES",
    datasetType="VARIANTS"
    ):
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
    vds = vds.annotate_globals(family_name = family_name)
    vds  = vds.annotate_globals(individual_id = individual_id)
    return vds


def add_vcf_to_hail(sample : SeqrSample):
    mt = import_vcf(
        sample.path_to_vcf,
        GENOME_VERSION,
        sample.individual_id,
        force_bgz=True,
        min_partitions=1000)
    mt = add_global_metadata(
        mt,
        s3path_to_vcf,
        sample.family_id,
        sample.individual_id
    )

    return mt