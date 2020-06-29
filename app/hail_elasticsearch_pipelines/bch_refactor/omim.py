import hail as hl
#from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf

def get_omim():

    ht = hl.import_table("s3://seqr-resources/omim/genemap2.txt",delimiter='|')
    ht = ht.annotate(colname=hl.str("omim"))
    ht = ht.to_matrix_table('Ensembl Gene ID',"colname")
        #ht = import_vcf("s3://seqr-resources/topmed/bravo-dbsnp-all.removed_chr_prefix.liftunder_GRCh37.vcf.gz","37","topmed")
    return ht

def annotate_with_omim(ht : hl.MatrixTable, omim : hl.Table):
    newht = ht.annotate_rows(
        omim = hl.cond(hl.len(ht.vep.transcript_consequences) == 0, hl.literal(""),omim.index(ht.vep.transcript_consequences[0].gene_id)['MIM']))
    return newht