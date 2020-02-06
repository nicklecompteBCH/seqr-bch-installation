import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_eigen():

    ht = import_vcf("s3n://seqr-resources/GRCh37/eigen/EIGEN_coding_noncoding.grch37.vcf.gz","37","eigen")
    return ht

def annotate_with_eigen(ht : hl.MatrixTable, eigen_ht : hl.MatrixTable):
    newht = ht.annotate_rows(
        eigen = hl.struct(
            Phred = eigen_ht.index_rows(ht.locus,ht.alleles).info['Eigen-phred']
        )
    )
    return newht