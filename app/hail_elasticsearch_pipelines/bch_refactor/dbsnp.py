import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf



def annotate_with_dbsnp(ht : hl.MatrixTable, dbsnpt : hl.Table):
    newht = ht.annotate_rows(
        #dbnsfp_DANN_score=".",
        dbnsfp_GERP_RS=dbsnpt.index(ht.locus,ht.alleles).GERP_RS
        dbnsfp_MutationTaster_pred=dbsnpt.index(ht.locus,ht.alleles).MutationTaster_pred,
        dbnsfp_phastCons100way_vertebrate=dbsnpt.index(ht.locus,ht.alleles).phastCons100way_vertebrate,
        dbnsfp_Polyphen2_HVAR_pred=dbsnpt.index(ht.locus,ht.alleles).Polyphen2_HVAR_pred,
        dbnsfp_MetaSVM_pred=dbsnpt.index(ht.locus,ht.alleles).MetaSVM_pred,
        dbnsfp_REVEL_score=dbsnpt.index(ht.locus,ht.alleles).REVEL_score,
        dbnsfp_SIFT_pred=dbsnpt.index(ht.locus,ht.alleles).SIFT_pred
    )
    return newht

"""
----------------------------------------
Global fields:
    None
----------------------------------------
Row fields:
    'locus': locus<GRCh37>
    'allele': array<str>
    'SIFT_pred': str
    'Polyphen2_HVAR_pred': str
    'MutationTaster_pred': str
    'FATHMM_pred': str
    'MetaSVM_pred': str
    'REVEL_score': str
    'GERP_RS': str
    'phastCons100way_vertebrate': str
----------------------------------------
Key: ['locus', 'allele']
----------------------------------------
"""
'SIFT_pred',
'Polyphen2_HVAR_pred',
'MutationTaster_pred',
'FATHMM_pred',
'MetaSVM_pred',
'REVEL_score',
'GERP_RS',
'phastCons100way_vertebrate'