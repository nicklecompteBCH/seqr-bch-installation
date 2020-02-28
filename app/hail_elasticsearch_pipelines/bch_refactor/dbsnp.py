import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import import_vcf



def annotate_with_dbsnp(ht : hl.MatrixTable, dbsnpt : hl.Table):
    newht = ht.annotate_rows(
        dbsnp = dbsnpt.index(ht.locus,ht.alleles)
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