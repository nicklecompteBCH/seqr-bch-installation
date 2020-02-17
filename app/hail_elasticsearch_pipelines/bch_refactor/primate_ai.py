import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import write_ht, import_vcf

def import_primate():

    ht = import_vcf("s3n://seqr-resources/GRCh37/primate_ai/PrimateAI_scores_v0.2.vcf.gz","37","primate_ai",min_partitions=11)
    return ht

def annotate_with_primate(ht : hl.MatrixTable, primate_ht : hl.MatrixTable):
    newht = ht.annotate_rows(
        primate_ai = hl.struct(
            score = primate_ht.index_rows(ht.locus,ht.alleles).info.score
        )
    )
    return newht