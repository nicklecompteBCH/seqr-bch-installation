import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_eigen(partitions : int = None,namenode : str = ""):

    ht = import_vcf(
        "hdfs://" + namenode + "/user/hadoop/data/EIGEN_coding_noncoding.grch37.vcf.gz",
        "37",
        "eigen",
        min_partitions=partitions,
        force_bgz=True)
    return ht

def annotate_with_eigen(ht : hl.MatrixTable, eigen_ht : hl.MatrixTable):
    newht = ht.annotate_rows(
        eigen = hl.struct(
            Phred = eigen_ht.index_rows(ht.locus,ht.alleles).info['Eigen-phred']
        )
    )
    return newht