import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf


def annotate_with_onekg(ht : hl.MatrixTable, onekg : hl.MatrixTable):
    mpc.describe()
    newht = ht.annotate_rows(
        **{'1kg' : onekg.index_rows(ht.locus,ht.alleles).info.MPC}
    )
    return newht