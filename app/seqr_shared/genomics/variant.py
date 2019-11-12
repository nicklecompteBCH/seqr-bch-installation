from enum import Enum

from typing import List

class DNANucleobase(Enum):
    Adenine = 1
    Guanine = 2
    Cytosine = 3
    Thymine = 4

    def __str__(self):
        if self == Adenine:
            return "adenine"
        elif self == Guanine:
            return "guanine"
        elif self == Cytosine:
            return "cytosine"
        elif self == Thymine:
            return "thymine"

    def str_short(self):
        if self == Adenine:
            return "A"
        elif self == Guanine:
            return "G"
        elif self == Cytosine:
            return "C"
        elif self == Thymine:
            return "T"

class HumanChromosome(Enum):
    Chr1 = 1
    Chr2 = 2
    Chr3 = 3
    Chr4 = 4
    Chr5 = 5
    Chr6 = 6
    Chr7 = 7
    Chr8 = 8
    Chr9 = 9
    Chr10 = 10
    Chr11 = 11
    Chr12 = 12
    Chr13 = 13
    Chr14 = 14
    Chr15 = 15
    Chr16 = 16
    Chr17 = 17
    Chr18 = 18
    Chr19 = 19
    Chr20 = 20
    Chr21 = 21
    Chr22 = 22
    ChrX = 23
    ChrY = 24

    def __str__(self):
        if self == Chr1:
            return "chr1"
        elif self == Chr2:
            return "chr2"
        elif self == Chr3:
            return "chr3"
        elif self == Chr4:
            return "chr4"
        elif self == Chr5:
            return "chr5"
        elif self == Chr6:
            return "chr6"
        elif self == Chr7:
            return "chr7"
        elif self == Chr8:
            return "chr8"
        elif self == Chr9:
            return "chr9"
        elif self == Chr10:
            return "chr10"
        elif self == Chr11:
            return "chr11"
        elif self == Chr12:
            return "chr12"
        elif self == Chr13:
            return "chr13"
        elif self == Chr14:
            return "chr14"
        elif self == Chr15:
            return "chr15"
        elif self == Chr16:
            return "chr16"
        elif self == Chr17:
            return "chr17"
        elif self == Chr18:
            return "chr18"
        elif self == Chr19:
            return "chr19"
        elif self == Chr20:
            return "chr20"
        elif self == Chr21:
            return "chr21"
        elif self == Chr22:
            return "chr22"
        elif self == ChrX:
            return "chrX"
        elif self == ChrY:
            return "chrY"

    def str_short(self):
        return str(self)[3:]

vep_json_schema = {}

Position = Int

PARSED_VARIANTS = [
    {
        'alt': 'T',
        'chrom': '1',
        'clinvar': {'clinicalSignificance': None, 'alleleId': None, 'variationId': None, 'goldStars': None},
        'familyGuids': ['F000003_3'],
        'genotypes': {
            'I000007_na20870': {'ab': 1, 'ad': None, 'gq': 99, 'sampleId': 'NA20870', 'numAlt': 2, 'dp': 74, 'pl': None}
        },
        'genomeVersion': '37',
        'genotypeFilters': '',
        'hgmd': {'accession': None, 'hgmdclass': None},
        'liftedOverChrom': None,
        'liftedOverGenomeVersion': None,
        'liftedOverPos': None,
        'mainTranscript': TRANSCRIPT_3,
        'originalAltAlleles': ['T'],
        'populations': {
            'callset': {'an': 32, 'ac': 2, 'hom': None, 'af': 0.063, 'hemi': None},
            'g1k': {'an': 0, 'ac': 0, 'hom': 0, 'af': 0.0, 'hemi': 0},
            'gnomad_genomes': {'an': 30946, 'ac': 4, 'hom': 0, 'af': 0.0004590314436538903, 'hemi': 0},
            'exac': {'an': 121308, 'ac': 8, 'hom': 0, 'af': 0.0006726888333653661, 'hemi': 0},
            'gnomad_exomes': {'an': 245930, 'ac': 16, 'hom': 0, 'af': 0.0009151523074911753, 'hemi': 0},
            'topmed': {'an': 125568, 'ac': 21, 'hom': 0, 'af': 0.00016724, 'hemi': 0}
        },
        'pos': 248367227,
        'predictions': {'splice_ai': None, 'eigen': None, 'revel': None, 'mut_taster': None, 'fathmm': None,
                        'polyphen': None, 'dann': None, 'sift': None, 'cadd': 25.9, 'metasvm': None, 'primate_ai': None,
                        'gerp_rs': None, 'mpc': None, 'phastcons_100_vert': None},
        'ref': 'TC',
        'rsid': None,
        'transcripts': {
            'ENSG00000135953': [TRANSCRIPT_3],
            'ENSG00000228198': [TRANSCRIPT_2],
        },
        'variantId': '1-248367227-TC-T',
        'xpos': 1248367227,
        '_sort': [1248367227],
    },
    {
        'alt': 'G',
        'chrom': '2',
        'clinvar': {'clinicalSignificance': None, 'alleleId': None, 'variationId': None, 'goldStars': None},
        'familyGuids': ['F000002_2', 'F000003_3'],
        'genotypes': {
            'I000004_hg00731': {'ab': 0, 'ad': None, 'gq': 99, 'sampleId': 'HG00731', 'numAlt': 0, 'dp': 67, 'pl': None},
            'I000005_hg00732': {'ab': 0, 'ad': None, 'gq': 96, 'sampleId': 'HG00732', 'numAlt': 2, 'dp': 42, 'pl': None},
            'I000006_hg00733': {'ab': 0, 'ad': None, 'gq': 96, 'sampleId': 'HG00733', 'numAlt': 1, 'dp': 42, 'pl': None},
            'I000007_na20870': {'ab': 0.70212764, 'ad': None, 'gq': 46, 'sampleId': 'NA20870', 'numAlt': 1, 'dp': 50, 'pl': None}
        },
        'genotypeFilters': '',
        'genomeVersion': '37',
        'hgmd': {'accession': None, 'hgmdclass': None},
        'liftedOverGenomeVersion': None,
        'liftedOverChrom': None,
        'liftedOverPos': None,
        'mainTranscript': TRANSCRIPT_1,
        'originalAltAlleles': ['G'],
        'populations': {
            'callset': {'an': 32, 'ac': 1, 'hom': None, 'af': 0.031, 'hemi': None},
            'g1k': {'an': 0, 'ac': 0, 'hom': 0, 'af': 0.0, 'hemi': 0},
            'gnomad_genomes': {'an': 0, 'ac': 0, 'hom': 0, 'af': 0.0, 'hemi': 0},
            'exac': {'an': 121336, 'ac': 6, 'hom': 0, 'af': 0.000242306760358614, 'hemi': 0},
            'gnomad_exomes': {'an': 245714, 'ac': 6, 'hom': 0, 'af': 0.00016269686320447742, 'hemi': 0},
            'topmed': {'an': 0, 'ac': 0, 'hom': 0, 'af': 0.0, 'hemi': 0}
        },
        'pos': 103343353,
        'predictions': {
            'splice_ai': None, 'eigen': None, 'revel': None, 'mut_taster': None, 'fathmm': None, 'polyphen': None,
            'dann': None, 'sift': None, 'cadd': 17.26, 'metasvm': None, 'primate_ai': None, 'gerp_rs': None,
            'mpc': None, 'phastcons_100_vert': None
        },
        'ref': 'GAGA',
        'rsid': None,
        'transcripts': {
            'ENSG00000135953': [TRANSCRIPT_1],
            'ENSG00000228198': [TRANSCRIPT_2],
        },
        'variantId': '2-103343353-GAGA-G',
        'xpos': 2103343353,
        '_sort': [2103343353],
    },
]

#  "vep_json_schema":
#     "Struct{
#         assembly_name:String,
#         allele_string:String,
#         ancestral:String,
#         colocated_variants:
#             Array[
#                 Struct{
#                     aa_allele:String,
#                     aa_maf:Float64,
#                     afr_allele:String,
#                     afr_maf:Float64,
#                     allele_string:String,
#                     amr_allele:String,
#                     amr_maf:Float64,
#                     clin_sig:Array[String],
#                     end:Int32,
#                     eas_allele:String,
#                     eas_maf:Float64,
#                     ea_allele:String,
#                     ea_maf:Float64,
#                     eur_allele:String,
#                     eur_maf:Float64,
#                     exac_adj_allele:String,
#                     exac_adj_maf:Float64,
#                     exac_allele:String,
#                     exac_afr_allele:String,
#                     exac_afr_maf:Float64,
#                     exac_amr_allele:String,
#                     exac_amr_maf:Float64,
#                     exac_eas_allele:String,
#                     exac_eas_maf:Float64,
#                     exac_fin_allele:String,
#                     exac_fin_maf:Float64,
#                     exac_maf:Float64,
#                     exac_nfe_allele:String,
#                     exac_nfe_maf:Float64,
#                     exac_oth_allele:String,
#                     exac_oth_maf:Float64,
#                     exac_sas_allele:String,
#                     exac_sas_maf:Float64,
#                     id:String,
#                     minor_allele:String,
#                     minor_allele_freq:Float64,
#                     phenotype_or_disease:Int32,
#                     pubmed:Array[Int32],
#                     sas_allele:String,
#                     sas_maf:Float64,
#                     somatic:Int32,
#                     start:Int32,
#                     strand:Int32
#                 }
#             ],
#         context:String,
#         end:Int32,
#         id:String,
#         input:String,
#         intergenic_consequences:
#             Array[
#                 Struct{
#                     allele_num:Int32,
#                     consequence_terms:Array[String],
#                     impact:String,
#                     minimised:Int32,
#                     variant_allele:String
#                     }
#                 ],
#         most_severe_consequence:String,
#         motif_feature_consequences:
#             Array[
#                 Struct{
#                     allele_num:Int32,
#                     consequence_terms:Array[String],
#                     high_inf_pos:String,
#                     impact:String,
#                     minimised:Int32,
#                     motif_feature_id:String,
#                     motif_name:String,
#                     motif_pos:Int32,
#                     motif_score_change:Float64,
#                     strand:Int32,
#                     variant_allele:String
#                     }
#                 ],
#         regulatory_feature_consequences:
#             Array[
#                 Struct{
#                     allele_num:Int32,
#                     biotype:String,
#                     consequence_terms:Array[String],
#                     impact:String,
#                     minimised:Int32,
#                     regulatory_feature_id:String,
#                     variant_allele:String
#                 }],
#         seq_region_name:String,
#         start:Int32,
#         strand:Int32,
#         transcript_consequences:
#             Array[
#                 Struct{
#                     allele_num:Int32,
#                     amino_acids:String,
#                     biotype:String,
#                     canonical:Int32,
#                     ccds:String,
#                     cdna_start:Int32,
#                     cdna_end:Int32,
#                     cds_end:Int32,
#                     cds_start:Int32,
#                     codons:String,
#                     consequence_terms:Array[String],
#                     distance:Int32,
#                     domains:
#                         Array[
#                             Struct{db:String,name:String}],
#                     exon:String,
#                     gene_id:String,
#                     gene_pheno:Int32,
#                     gene_symbol:String,
#                     gene_symbol_source:String,
#                     hgnc_id:String,
#                     hgvsc:String,
#                     hgvsp:String,
#                     hgvs_offset:Int32,
#                     impact:String,
#                     intron:String,
#                     lof:String,
#                     lof_flags:String,
#                     lof_filter:String,
#                     lof_info:String,
#                     minimised:Int32,
#                     polyphen_prediction:String,
#                     polyphen_score:Float64,
#                     protein_end:Int32,
#                     protein_start:Int32,
#                     protein_id:String,
#                     sift_prediction:String,
#                     sift_score:Float64,
#                     strand:Int32,
#                     swissprot:String,
#                     transcript_id:String,
#                     trembl:String,
#                     uniparc:String,
#                     variant_allele:String}],
#         variant_class:String}"