from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.vep import (
    get_expr_for_vep_gene_id_to_consequence_map,
    get_expr_for_vep_sorted_transcript_consequences_array,
    get_expr_for_worst_transcript_consequence_annotations_struct,
    get_expr_for_vep_consequence_terms_set,
    get_expr_for_vep_protein_domains_set,
    get_expr_for_vep_transcript_ids_set,
    get_expr_for_vep_transcript_id_to_consequence_map,
    get_expr_for_vep_gene_ids_set
)
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.clinvar import CLINVAR_GOLD_STARS_LOOKUP, download_and_import_latest_clinvar_vcf
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.computed_fields.variant_id import *
import hail as hl

def load_clinvar(es_host = None, partitions : int = None, namenode : str=""):
    index_name = "clinvar_grch37" #"clinvar_grch{}".format(args.genome_version)
    mt = download_and_import_latest_clinvar_vcf("37", partitions=partitions,namenode = namenode)
    mt = hl.vep(mt, "hdfs:///user/vep85-loftee-gcloud.json", name="vep", block_size=1000)
    mt = mt.annotate_rows(
        sortedTranscriptConsequences=get_expr_for_vep_sorted_transcript_consequences_array(vep_root=mt.vep)
    )
    mt = mt.annotate_rows(
        main_transcript=get_expr_for_worst_transcript_consequence_annotations_struct(
            vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences
        )
    )
    mt = mt.annotate_rows(
        gene_ids=get_expr_for_vep_gene_ids_set(
            vep_transcript_consequences_root=mt.sortedTranscriptConsequences
        ),
    )

    review_status_str = hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNREVSTAT)), key=lambda s: s.replace("^_", "z")))

    goldstar_dict = hl.literal(CLINVAR_GOLD_STARS_LOOKUP)



    mt = mt.annotate_rows(
        allele_id=mt.info.ALLELEID,
        alt=get_expr_for_alt_allele(mt),
        chrom=get_expr_for_contig(mt.locus),
        clinical_significance=hl.delimit(hl.sorted(hl.array(hl.set(mt.info.CLNSIG)), key=lambda s: s.replace("^_", "z"))),
        domains=get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root=mt.vep.transcript_consequences),
        gene_ids=mt.gene_ids,
        gene_id_to_consequence_json=get_expr_for_vep_gene_id_to_consequence_map(
            vep_sorted_transcript_consequences_root=mt.sortedTranscriptConsequences,
            gene_ids=mt.gene_ids
        ),
        gold_stars= hl.int(goldstar_dict.get(review_status_str)),
        **{f"main_transcript_{field}": mt.main_transcript[field] for field in mt.main_transcript.dtype.fields},
        pos=get_expr_for_start_pos(mt),
        ref=get_expr_for_ref_allele(mt),
        review_status=review_status_str,
        transcript_consequence_terms=get_expr_for_vep_consequence_terms_set(
            vep_transcript_consequences_root=mt.sortedTranscriptConsequences
        ),
        transcript_ids=get_expr_for_vep_transcript_ids_set(
            vep_transcript_consequences_root=mt.sortedTranscriptConsequences
        ),
        transcript_id_to_consequence_json=get_expr_for_vep_transcript_id_to_consequence_map(
            vep_transcript_consequences_root=mt.sortedTranscriptConsequences
        ),
        variant_id=get_expr_for_variant_id(mt),
        xpos=get_expr_for_xpos(mt.locus)
    )

    mt = mt.annotate_rows(clinvar_clinical_significance = mt.clinical_significance)

    hl.summarize_variants(mt)

        # Drop key columns for export
    if es_host:
        rows = mt.rows()
        rows = rows.order_by(rows.variant_id).drop("locus", "alleles")
        print("\n=== Exporting ClinVar to Elasticsearch ===")
        es = ElasticsearchClient(es_host, "9200")
        es.export_table_to_elasticsearch(
            rows,
            index_name=index_name,
            index_type_name='variant',
            block_size=200,
            num_shards=2,
            delete_index_before_exporting=True,
            export_globals_to_index_meta=True,
            verbose=True,
        )
    else:
        return mt

CLINVAR_GOLD_STARS_LOOKUP = {
    'no_interpretation_for_the_single_variant': "0",
    'no_assertion_provided' : "0",
    'no_assertion_criteria_provided' : "0",
    'criteria_provided,_single_submitter' : "1",
    'criteria_provided,_conflicting_interpretations' : "1",
    'criteria_provided,_multiple_submitters,_no_conflicts' : "2",
    'reviewed_by_expert_panel' : "3",
    'practice_guideline' : "4"

}

def annotate_with_clinvar(mt: hl.MatrixTable, clinvar_mt: hl.Table) -> hl.MatrixTable:
    #joined_mt = clinvar_mt.semi_join_rows(mt)
    mt = mt.annotate_rows(
        clinvar_allele_id = clinvar_mt.index(mt.row_key).allele_id,
        clinvar_clinical_significance = clinvar_mt.index(mt.row_key).clinvar_clinical_significance,
        clinvar_gold_stars = clinvar_mt.index(mt.row_key).gold_stars
    )
    return mt