import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_exac():

    ht = import_vcf("s3n://seqr-resources/GRCh37/gnomad/ExAC.r1.sites.vcf.gz","37","eigen")
    return ht

def annotate_with_exac(ht : hl.MatrixTable, exac_ht : hl.MatrixTable):
    exac_ht.describe()
    newht = ht.annotate_rows(
        exac = exac_ht.index_rows(ht.locus,ht.alleles).info
    )
    return newht