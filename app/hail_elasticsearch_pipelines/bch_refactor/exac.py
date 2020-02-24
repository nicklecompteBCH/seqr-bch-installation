import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_exac(partitions : int = None,namenode : str = ""):

    ht = import_vcf(
        "hdfs://" + namenode + "/user/hdfs/data/ExAC.r1.sites.vep.vcf.gz",
        "37",
        "exac",
        min_partitions=partitions,
        force_bgz=True
    )
    return ht

def annotate_with_exac(ht : hl.MatrixTable, exac_ht : hl.MatrixTable):
    newht = ht.annotate_rows(
        exac = exac_ht.index_rows(ht.locus,ht.alleles).info
    )
    return newht