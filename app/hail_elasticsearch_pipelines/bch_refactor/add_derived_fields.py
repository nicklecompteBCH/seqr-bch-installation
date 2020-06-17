import hail as hl
from enum import Enum
from typing import Union, Tuple

from .common_types import LocusInterval

from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.variant_id import *
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.vep import *

def annotate_mt_with_derived_fields(mt: hl.MatrixTable) -> hl.MatrixTable:

    mt = mt.annotate_rows(
        docId = get_expr_for_variant_id(mt,512),
        variantId = get_expr_for_variant_id(mt),
        variant_type = get_expr_for_variant_type(mt),
        contig = get_expr_for_contig(mt.locus),
        pos = get_expr_for_start_pos(mt),
        start = get_expr_for_start_pos(mt),
        end = get_expr_for_end_pos(mt),
        ref = get_expr_for_ref_allele(mt),
        alt = get_expr_for_alt_allele(mt),
        xpos = get_expr_for_xpos(mt.locus),
        xstart = get_expr_for_xpos(mt.locus),
        xstop = get_expr_for_xpos_end(mt.locus),
        sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep),
        FAF=get_expr_for_filtering_allele_frequency()
    )


    mt = mt.annotate_rows(
            mainTranscript=get_expr_for_worst_transcript_consequence_annotations_struct(
                    vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences
                ),
            gene_ids=hl.set(mt.vep.transcript_consequences.map(lambda c: c.gene_id))
    )
    return mt


# def step1_compute_derived_fields_v01(hc, vds, args):
#     if args.start_with_step > 1 or args.stop_after_step < 1:
#         return hc, vds

#     logger.info("\n\n=============================== pipeline - step 1 - compute derived fields ===============================")

#     if vds is None or not args.skip_writing_intermediate_vds:
#         stop_hail_context(hc)
#         hc = create_hail_context()
#         vds = read_in_dataset(hc, args.step0_output_vds, dataset_type=args.dataset_type, skip_summary=True, num_partitions=args.cpu_limit)

#     parallel_computed_annotation_exprs = [
#             "va.docId = %s" % get_expr_for_variant_id(512),
#             "va.variantId = %s" % get_expr_for_variant_id(),
#             "va.contig = %s" % get_expr_for_contig(),
#             "va.pos = %s" % get_expr_for_start_pos(),
#             "va.start = %s" % get_expr_for_start_pos(),
#             "va.end = %s" % get_expr_for_end_pos(),
#             "va.ref = %s" % get_expr_for_ref_allele(),
#             "va.alt = %s" % get_expr_for_alt_allele(),
#             "va.xpos = %s" % get_expr_for_xpos(pos_field="start"),
#             "va.xstart = %s" % get_expr_for_xpos(pos_field="start"),
#             "va.sortedTranscriptConsequences = %s" % get_expr_for_vep_sorted_transcript_consequences_array(
#                 vep_root="va.vep",
#                 include_coding_annotations=True,
#                 add_transcript_rank=bool(args.use_nested_objects_for_vep)),
#         ]
#     if args.dataset_type == "VARIANTS":
#         FAF_CONFIDENCE_INTERVAL = 0.95  # based on https://macarthurlab.slack.com/archives/C027LHMPP/p1528132141000430

#         parallel_computed_annotation_exprs += [
#             "va.FAF = %s" % get_expr_for_filtering_allele_frequency("va.info.AC[va.aIndex - 1]", "va.info.AN", FAF_CONFIDENCE_INTERVAL),
#         ]

#     serial_computed_annotation_exprs = [
#         "va.xstop = %s" % get_expr_for_xpos(field_prefix="va.", pos_field="end"),
#         "va.transcriptIds = %s" % get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
#         "va.domains = %s" % get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
#         "va.transcriptConsequenceTerms = %s" % get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
#         "va.mainTranscript = %s" % get_expr_for_worst_transcript_consequence_annotations_struct("va.sortedTranscriptConsequences"),
#         "va.geneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences"),
#         "va.codingGeneIds = %s" % get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root="va.sortedTranscriptConsequences", only_coding_genes=True),
#     ]

#     # serial_computed_annotation_exprs += [
#     #   "va.sortedTranscriptConsequences = va.sortedTranscriptConsequences.map(c => drop(c, amino_acids, biotype))"
#     #]

#     if not bool(args.use_nested_objects_for_vep):
#         serial_computed_annotation_exprs += [
#             "va.sortedTranscriptConsequences = json(va.sortedTranscriptConsequences)"
#         ]

#     vds = vds.annotate_variants_expr(parallel_computed_annotation_exprs)

#     for expr in serial_computed_annotation_exprs:
#         vds = vds.annotate_variants_expr(expr)

#     pprint(vds.variant_schema)

#     INPUT_SCHEMA  = {}
#     if args.dataset_type == "VARIANTS":
#         INPUT_SCHEMA["top_level_fields"] = """
#             docId: String,
#             variantId: String,
#             originalAltAlleles: Set[String],

#             contig: String,
#             start: Int,
#             pos: Int,
#             end: Int,
#             ref: String,
#             alt: String,

#             xpos: Long,
#             xstart: Long,
#             xstop: Long,

#             rsid: String,
#             --- qual: Double,
#             filters: Set[String],
#             aIndex: Int,

#             geneIds: Set[String],
#             transcriptIds: Set[String],
#             codingGeneIds: Set[String],
#             domains: Set[String],
#             transcriptConsequenceTerms: Set[String],
#             sortedTranscriptConsequences: String,
#             mainTranscript: Struct,
#         """

#         if args.not_gatk_genotypes:
#             INPUT_SCHEMA["info_fields"] = """
#                 AC: Array[Int],
#                 AF: Array[Double],
#                 AN: Int,
#                 --- BaseQRankSum: Double,
#                 --- ClippingRankSum: Double,
#                 --- DP: Int,
#                 --- FS: Double,
#                 --- InbreedingCoeff: Double,
#                 --- MQ: Double,
#                 --- MQRankSum: Double,
#                 --- QD: Double,
#                 --- ReadPosRankSum: Double,
#                 --- VQSLOD: Double,
#                 --- culprit: String,
#             """
#         else:
#             INPUT_SCHEMA["info_fields"] = """
#                 AC: Array[Int],
#                 AF: Array[Double],
#                 AN: Int,
#                 --- BaseQRankSum: Double,
#                 --- ClippingRankSum: Double,
#                 --- DP: Int,
#                 --- FS: Double,
#                 --- InbreedingCoeff: Double,
#                 --- MQ: Double,
#                 --- MQRankSum: Double,
#                 --- QD: Double,
#                 --- ReadPosRankSum: Double,
#                 --- VQSLOD: Double,
#                 --- culprit: String,
#             """
#     elif args.dataset_type == "SV":
#         INPUT_SCHEMA["top_level_fields"] = """
#             docId: String,
#             variantId: String,

#             contig: String,
#             start: Int,
#             pos: Int,
#             end: Int,
#             ref: String,
#             alt: String,

#             xpos: Long,
#             xstart: Long,
#             xstop: Long,

#             rsid: String,
#             --- qual: Double,
#             filters: Set[String],
#             aIndex: Int,

#             geneIds: Set[String],
#             transcriptIds: Set[String],
#             codingGeneIds: Set[String],
#             domains: Set[String],
#             transcriptConsequenceTerms: Set[String],
#             sortedTranscriptConsequences: String,
#             mainTranscript: Struct,
#         """

#         # END=100371979;SVTYPE=DEL;SVLEN=-70;CIGAR=1M70D	GT:FT:GQ:PL:PR:SR
#         INPUT_SCHEMA["info_fields"] = """
#             IMPRECISE: Boolean,
#             SVTYPE: String,
#             SVLEN: Int,
#             END: Int,
#             --- OCC: Int,
#             --- FRQ: Double,
#         """
#     else:
#         raise ValueError("Unexpected dataset_type: %s" % args.dataset_type)

#     if args.exclude_vcf_info_field:
#         INPUT_SCHEMA["info_fields"] = ""

#     expr = convert_vds_schema_string_to_annotate_variants_expr(root="va.clean", **INPUT_SCHEMA)

#     vds = vds.annotate_variants_expr(expr=expr)
#     vds = vds.annotate_variants_expr("va = va.clean")

#     if not args.skip_writing_intermediate_vds:
#         write_vds(vds, args.step1_output_vds)

#     args.start_with_step = 2  # step 1 finished, so, if an error occurs and it goes to retry, start with the next step

#     return hc, vds
