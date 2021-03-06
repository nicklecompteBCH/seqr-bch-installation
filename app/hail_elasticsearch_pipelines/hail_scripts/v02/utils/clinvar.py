import gzip
import os
import subprocess
from enum import Enum

import hail as hl

from .hail_utils import import_vcf

CLINVAR_FTP_PATH = "ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh{genome_version}/clinvar.vcf.gz"
CLINVAR_HT_PATH = "gs://seqr-reference-data/GRCh{genome_version}/clinvar/clinvar.GRCh{genome_version}.ht"

class ClinvarClinicalSignificance(Enum):
    Benign = 1
    LikelyBenign = 2
    Uncertain = 3
    LikelyPathogenic = 4
    Pathogenic = 5
    DrugResponse = 6
    Association = 7
    RiskFactor = 8
    Protective = 9
    Affects = 10
    ConflictingDataFromSubmitters = 11
    Other = 12
    NotProvided = 13

CLINVAR_GOLD_STARS_LOOKUP = hl.dict(
    {
        "no_interpretation_for_the_single_variant": 0,
        "no_assertion_provided": 0,
        "no_assertion_criteria_provided": 0,
        "criteria_provided,_single_submitter": 1,
        "criteria_provided,_conflicting_interpretations": 1,
        "criteria_provided,_multiple_submitters,_no_conflicts": 2,
        "reviewed_by_expert_panel": 3,
        "practice_guideline": 4,
    }
)


def download_and_import_latest_clinvar_vcf(genome_version: str, partitions : int = None,namenode : str="") -> hl.MatrixTable:
    """Downloads the latest clinvar VCF from the NCBI FTP server, copies it to HDFS and returns the hdfs file path
    as well the clinvar release date that's specified in the VCF header.

    Args:
        genome_version (str): "37" or "38"
    """

    if genome_version not in ["37", "38"]:
        raise ValueError("Invalid genome_version: " + str(genome_version))

    clinvar_url = CLINVAR_FTP_PATH.format(genome_version=genome_version)
    clinvar_vcf_hdfs_path = "hdfs://" + namenode + "/user/hdfs/data/clinvar.GRCh37.vcf.gz"

    #subprocess.run(["wget", clinvar_url, "-O", local_tmp_file_path], check=True)
    #subprocess.run(["hdfs", "dfs", "-put", "-f", f"file://{local_tmp_file_path}", clinvar_vcf_hdfs_path], check=True)

    #clinvar_release_date = _parse_clinvar_release_date(local_tmp_file_path)
    mt = import_vcf(
        clinvar_vcf_hdfs_path,
        genome_version,
        "clinvar37",
        drop_samples=True,
        min_partitions=partitions,
        skip_invalid_loci=True,
        force_bgz=True
    )
    #mt = mt.annotate_globals(version=clinvar_release_date)

    return mt


def _parse_clinvar_release_date(local_vcf_path: str) -> str:
    """Parse clinvar release date from the VCF header.

    Args:
        local_vcf_path (str): clinvar vcf path on the local file system.

    Returns:
        str: return VCF release date as string, or None if release date not found in header.
    """
    with gzip.open(local_vcf_path, "rt") as f:
        for line in f:
            if line.startswith("##fileDate="):
                clinvar_release_date = line.split("=")[-1].strip()
                return clinvar_release_date

            if not line.startswith("#"):
                return ""

    return ""
