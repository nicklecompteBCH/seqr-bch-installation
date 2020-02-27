import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_mpc():

    ht= import_vcf('s3://seqr-resources/mpc/fordist_constraint_official_mpc_values.vcf.gz',"37","mpc",min_partitions=30)    #ht = import_vcf("s3://seqr-resources/topmed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed")
    return ht

def annotate_with_mpc(ht : hl.MatrixTable, mpc : hl.MatrixTable):
    mpc.describe()
    newht = ht.annotate_rows(
        MPC = mpc.index_rows(ht.locus,ht.alleles).info.MPC
    )
    return newht