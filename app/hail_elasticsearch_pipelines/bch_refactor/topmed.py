import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_topmed():

    ht = import_vcf("s3://seqr-resources/topmed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed")
    return ht

def annotate_with_topmed(ht : hl.MatrixTable, topmed : hl.MatrixTable):
    topmed.describe()
    newht = ht.annotate_rows(
        topmed = topmed.index_rows(ht.locus,ht.alleles).info
    )
    return newht