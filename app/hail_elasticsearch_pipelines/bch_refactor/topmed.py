import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_topmed(partitions : int = None,namenode:str = ""):

    ht = import_vcf("hdfs://" + namenode + "/user/hdfs/data/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed",min_partitions=partitions,force_bgz=True)
    return ht

def annotate_with_topmed(ht : hl.MatrixTable, topmed : hl.MatrixTable):
    topmed.describe()
    newht = ht.annotate_rows(
        topmed = topmed.index_rows(ht.locus,ht.alleles).info
    )
    return newht