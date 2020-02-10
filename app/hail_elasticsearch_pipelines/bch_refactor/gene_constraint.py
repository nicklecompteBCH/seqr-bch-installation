import hail as hl
#from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_gc():

    ht = hl.import_table("s3://seqr-resources/GRCH36/MPC/fordist_cleaned_exac_r03_march16_z_pli_rec_null_data.txt")
    ht = ht.to_matrix_table(ht['transcript'],"gene_constraint")
        #ht = import_vcf("s3://seqr-resources/topmed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed")
    return ht

def annotate_with_gc(ht : hl.MatrixTable, gc : hl.MatrixTable):
    gc.describe()
    newht = ht.annotate_rows(
        gene_constraint = gc.index_rows(ht.mainTranscript.transcript_id)
    )
    return newht