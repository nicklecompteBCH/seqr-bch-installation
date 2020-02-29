import hail as hl
#from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_gc():

    ht = hl.import_table("s3://seqr-resources/gene_constraint/fordist_cleaned_exac_r03_march16_z_pli_rec_null_data.txt")
    ht = ht.annotate(colname=hl.str("gene_constraint"))
    ht = ht.to_matrix_table(['transcript'],["colname"])
        #ht = import_vcf("s3://seqr-resources/topmed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed")
    return ht

def annotate_with_gc(ht : hl.MatrixTable, gc : hl.Table):
    newht = ht.annotate_rows(
        gene_constraint_mis_z = gc.index(ht.mainTranscript.transcript_id).mis_z,
        gene_constraint_pLI_rank=gc.index(ht.mainTranscript.transcript_id).pLI
    )
    return newht