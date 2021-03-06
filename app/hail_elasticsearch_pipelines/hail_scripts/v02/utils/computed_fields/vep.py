import hail as hl

# Consequence terms in order of severity (more severe to less severe) as estimated by Ensembl.
# See https://ensembl.org/info/genome/variation/prediction/predicted_data.html
CONSEQUENCE_TERMS = [
    "transcript_ablation",
    "splice_acceptor_variant",
    "splice_donor_variant",
    "stop_gained",
    "frameshift_variant",
    "stop_lost",
    "start_lost",  # new in v81
    "initiator_codon_variant",  # deprecated
    "transcript_amplification",
    "inframe_insertion",
    "inframe_deletion",
    "missense_variant",
    "protein_altering_variant",  # new in v79
    "splice_region_variant",
    "incomplete_terminal_codon_variant",
    "start_retained_variant",
    "stop_retained_variant",
    "synonymous_variant",
    "coding_sequence_variant",
    "mature_miRNA_variant",
    "5_prime_UTR_variant",
    "3_prime_UTR_variant",
    "non_coding_transcript_exon_variant",
    "non_coding_exon_variant",  # deprecated
    "intron_variant",
    "NMD_transcript_variant",
    "non_coding_transcript_variant",
    "nc_transcript_variant",  # deprecated
    "upstream_gene_variant",
    "downstream_gene_variant",
    "TFBS_ablation",
    "TFBS_amplification",
    "TF_binding_site_variant",
    "regulatory_region_ablation",
    "regulatory_region_amplification",
    "feature_elongation",
    "regulatory_region_variant",
    "feature_truncation",
    "intergenic_variant",
]

# hail DictExpression that maps each CONSEQUENCE_TERM to it's rank in the list
CONSEQUENCE_TERM_RANK_LOOKUP = hl.dict({term: rank for rank, term in enumerate(CONSEQUENCE_TERMS)})


OMIT_CONSEQUENCE_TERMS = [
    "upstream_gene_variant",
    "downstream_gene_variant",
]

def get_expr_for_vep_consequence_terms_set(vep_transcript_consequences_root):
    return hl.set(vep_transcript_consequences_root.flatmap(lambda c: c.consequence_terms))


def get_expr_for_vep_gene_ids_set(vep_transcript_consequences_root, only_coding_genes=False):
    """Expression to compute the set of gene ids in VEP annotations for this variant.

    Args:
        vep_transcript_consequences_root (ArrayExpression): VEP transcript_consequences root in the struct
        only_coding_genes (bool): If set to True, non-coding genes will be excluded.
    Return:
        SetExpression: expression
    """

    expr = vep_transcript_consequences_root

    if only_coding_genes:
        expr = expr.filter(lambda c: hl.or_else(c.biotype, "") == "protein_coding")

    return hl.set(expr.map(lambda c: c.gene_id))


def get_expr_for_vep_protein_domains_set(vep_transcript_consequences_root):
    return hl.set(
        vep_transcript_consequences_root.flatmap(lambda c: c.domains.map(lambda domain: domain.db + ":" + domain.name))
    )


PROTEIN_LETTERS_1TO3 = hl.dict(
    {
        "A": "Ala",
        "C": "Cys",
        "D": "Asp",
        "E": "Glu",
        "F": "Phe",
        "G": "Gly",
        "H": "His",
        "I": "Ile",
        "K": "Lys",
        "L": "Leu",
        "M": "Met",
        "N": "Asn",
        "P": "Pro",
        "Q": "Gln",
        "R": "Arg",
        "S": "Ser",
        "T": "Thr",
        "V": "Val",
        "W": "Trp",
        "Y": "Tyr",
        "X": "Ter",
        "*": "Ter",
        "U": "Sec",
    }
)


HGVSC_CONSEQUENCES = hl.set(["splice_donor_variant", "splice_acceptor_variant", "splice_region_variant"])


def get_expr_for_formatted_hgvs(csq):
    return hl.cond(
        hl.is_missing(csq.hgvsp) | HGVSC_CONSEQUENCES.contains(csq.major_consequence),
        csq.hgvsc.split(":")[-1],
        hl.cond(
            csq.hgvsp.contains("=") | csq.hgvsp.contains("%3D"),
            hl.bind(
                lambda protein_letters: "p." + protein_letters + hl.str(csq.protein_start) + protein_letters,
                hl.delimit(csq.amino_acids.split("").map(lambda l: PROTEIN_LETTERS_1TO3.get(l)), ""),
            ),
            csq.hgvsp.split(":")[-1],
        ),
    )

"""
'array<struct{allele_num: int32, amino_acids: str, biotype: str, canonical: int32, ccds: str, cdna_start: int32, cdna_end: int32, cds_end: int32, cds_start: int32, codons: str, consequence_terms: array<str>, distance: int32, domains: array<struct{db: str, name: str}>, exon: str, gene_id: str, gene_pheno: int32, gene_symbol: str, gene_symbol_source: str, hgnc_id: str, hgvsc: str, hgvsp: str, hgvs_offset: int32, impact: str, intron: str, lof: str, lof_flags: str, lof_filter: str, lof_info: str, minimised: int32, polyphen_prediction: str, polyphen_score: float64, protein_end: int32, protein_start: int32, protein_id: str, sift_prediction: str, sift_score: float64, strand: int32, swissprot: str, transcript_id: str, trembl: str, uniparc: str, variant_allele: str}>'
'array<struct{allele_num: int32, amino_acids: str, biotype: str, canonical: int32, ccds: int32, cdna_start: int32, cdna_end: int32, cds_end: int32, cds_start: int32, codons: str, consequence_terms: array<str>, distance: int32, domains: array<struct{db: str, name: str}>, exon: str, gene_id: str, gene_pheno: int32, gene_symbol: str, gene_symbol_source: str, hgnc_id: str, hgvsc: str, hgvsp: str, hgvs_offset: int32, impact: str, intron: str, lof: str, lof_flags: str, lof_filter: str, lof_info: str, minimised: int32, polyphen_prediction: str, polyphen_score: float64, protein_end: int32, protein_start: int32, protein_id: str, sift_prediction: str, sift_score: float64, strand: int32, swissprot: str, transcript_id: str, trembl: str, uniparc: str, variant_allele: str}>'
"""

def get_expr_for_vep_all_consequences_array(vep_root):
    retval = vep_root.annotate(
        formattedIntergenic = hl.map(lambda x: hl.struct(
            allele_num = x.allele_num,
            amino_acids = hl.null(hl.tstr),
            biotype = hl.literal("intron"),
            canonical = hl.null(hl.tint32),
            ccds = hl.null(hl.tstr),
            cdna_start  = hl.null(hl.tint32),
            cdna_end = hl.null(hl.tint32),
            cds_end  = hl.null(hl.tint32),
            cds_start = hl.null(hl.tint32),
            codons  = hl.null(hl.tstr),
            consequence_terms = x.consequence_terms,
            distance = hl.null(hl.tint32), # not na...,
            domains = hl.null(hl.tarray(hl.tstruct(db=hl.tstr, name= hl.tstr))),
            exon = hl.null(hl.tstr),
            gene_id = hl.null(hl.tstr),
            gene_pheno = hl.null(hl.tint32), # not na...,
            gene_symbol= hl.null(hl.tstr),
            gene_symbol_source = hl.null(hl.tstr),
            hgnc_id = hl.null(hl.tstr),
            hgvsc= hl.null(hl.tstr),
            hgvsp = hl.null(hl.tstr),
            hgvs_offset = hl.null(hl.tint32),
            impact = x.impact,
            intron = hl.null(hl.tstr),
            lof = hl.null(hl.tstr),
            lof_flags = hl.null(hl.tstr),
            lof_filter = hl.null(hl.tstr),
            lof_info = hl.null(hl.tstr),
            minimised = x.minimised,
            polyphen_prediction = hl.null(hl.tstr),
            polyphen_score = hl.null(hl.tfloat64),
            protein_end = hl.null(hl.tint32),
            protein_start = hl.null(hl.tint32),
            protein_id = hl.null(hl.tstr),
            sift_prediction = hl.null(hl.tstr),
            sift_score = hl.null(hl.tfloat64),
            strand = hl.null(hl.tint32),
            swissprot = hl.null(hl.tstr),
            transcript_id = hl.null(hl.tstr),
            trembl = hl.null(hl.tstr),
            uniparc = hl.null(hl.tstr),
            variant_allele = x.variant_allele), vep_root.intergenic_consequences))
    vep_root = retval.annotate(all_consequences = hl.cond(hl.is_missing(retval.transcript_consequences),retval.formattedIntergenic,retval.transcript_consequences))
    return vep_root

def get_expr_for_vep_sorted_transcript_consequences_array(vep_root,
                                                          include_coding_annotations=True,
                                                          omit_consequences=OMIT_CONSEQUENCE_TERMS):
    """Sort transcripts by 3 properties:

        1. coding > non-coding
        2. transcript consequence severity
        3. canonical > non-canonical

    so that the 1st array entry will be for the coding, most-severe, canonical transcript (assuming
    one exists).

    Also, for each transcript in the array, computes these additional fields:
        domains: converts Array[Struct] to string of comma-separated domain names
        hgvs: set to hgvsp is it exists, or else hgvsc. formats hgvsp for synonymous variants.
        major_consequence: set to most severe consequence for that transcript (
            VEP sometimes provides multiple consequences for a single transcript)
        major_consequence_rank: major_consequence rank based on VEP SO ontology (most severe = 1)
            (see http://www.ensembl.org/info/genome/variation/predicted_data.html)
        category: set to one of: "lof", "missense", "synonymous", "other" based on the value of major_consequence.

    Args:
        vep_root (StructExpression): root path of the VEP struct in the MT
        include_coding_annotations (bool): if True, fields relevant to protein-coding variants will be included
    """

    selected_annotations = [
        "biotype",
        "canonical",
        "cdna_start",
        "cdna_end",
        "codons",
        "gene_id",
        "gene_symbol",
        "hgvsc",
        "hgvsp",
        "transcript_id",
    ]

    if include_coding_annotations:
        selected_annotations.extend(
            [
                "amino_acids",
                "lof",
                "lof_filter",
                "lof_flags",
                "lof_info",
                "polyphen_prediction",
                "protein_id",
                "protein_start",
                "sift_prediction",
            ]
        )

    omit_consequence_terms = hl.set(omit_consequences) if omit_consequences else hl.empty_set(hl.tstr)
    vep_root = get_expr_for_vep_all_consequences_array(vep_root)
    result = hl.sorted(
        vep_root.all_consequences.map(
            lambda c: c.select(
                *selected_annotations,
                consequence_terms=c.consequence_terms.filter(lambda t: ~omit_consequence_terms.contains(t)),
                domains=c.domains.map(lambda domain: domain.db + ":" + domain.name),
                major_consequence=hl.cond(
                    c.consequence_terms.size() > 0,
                    hl.sorted(c.consequence_terms, key=lambda t: CONSEQUENCE_TERM_RANK_LOOKUP.get(t))[0],
                    hl.null(hl.tstr),
                )
            )
        )
        .filter(lambda c: c.consequence_terms.size() > 0)
        .map(
            lambda c: c.annotate(
                category=(
                    hl.case()
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("frameshift_variant"),
                        "lof",
                    )
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("missense_variant"),
                        "missense",
                    )
                    .when(
                        CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence)
                        <= CONSEQUENCE_TERM_RANK_LOOKUP.get("synonymous_variant"),
                        "synonymous",
                    )
                    .default("other")
                ),
                hgvs=get_expr_for_formatted_hgvs(c),
                major_consequence_rank=CONSEQUENCE_TERM_RANK_LOOKUP.get(c.major_consequence),
            )
        ),
        lambda c: (
            hl.bind(
                lambda is_coding, is_most_severe, is_canonical: (
                    hl.cond(
                        is_coding,
                        hl.cond(is_most_severe, hl.cond(is_canonical, 1, 2), hl.cond(is_canonical, 3, 4)),
                        hl.cond(is_most_severe, hl.cond(is_canonical, 5, 6), hl.cond(is_canonical, 7, 8)),
                    )
                ),
                hl.or_else(c.biotype, "") == "protein_coding",
                hl.set(c.consequence_terms).contains(vep_root.most_severe_consequence),
                hl.or_else(c.canonical, 0) == 1,
            )
        ),
    )

    if not include_coding_annotations:
        # for non-coding variants, drop fields here that are hard to exclude in the above code
        result = result.map(lambda c: c.drop("domains", "hgvsp"))

    return hl.zip_with_index(result).map(
        lambda csq_with_index: csq_with_index[1].annotate(transcript_rank=csq_with_index[0])
    )


def get_expr_for_vep_protein_domains_set_from_sorted(vep_sorted_transcript_consequences_root):
    return hl.set(
        vep_sorted_transcript_consequences_root.flatmap(lambda c: c.domains)
    )


def get_expr_for_vep_gene_id_to_consequence_map(vep_sorted_transcript_consequences_root, gene_ids):
    # Manually build string because hl.json encodes a dictionary as [{ key: ..., value: ... }, ...]
    return (
        "{"
        + hl.delimit(
            gene_ids.map(
                lambda gene_id: hl.bind(
                    lambda worst_consequence_in_gene: '"' + gene_id + '":"' + worst_consequence_in_gene.major_consequence + '"',
                    vep_sorted_transcript_consequences_root.find(lambda c: c.gene_id == gene_id)
                )
            )
        )
        + "}"
    )


def get_expr_for_vep_transcript_id_to_consequence_map(vep_transcript_consequences_root):
    # Manually build string because hl.json encodes a dictionary as [{ key: ..., value: ... }, ...]
    return (
        "{"
        + hl.delimit(
            vep_transcript_consequences_root.map(lambda c: '"' + c.transcript_id + '": "' + c.major_consequence + '"')
        )
        + "}"
    )


def get_expr_for_vep_transcript_ids_set(vep_transcript_consequences_root):
    return hl.set(vep_transcript_consequences_root.map(lambda c: c.transcript_id))


def get_expr_for_worst_transcript_consequence_annotations_struct(
    vep_sorted_transcript_consequences_root, include_coding_annotations=True
):
    """Retrieves the top-ranked transcript annotation based on the ranking computed by
    get_expr_for_vep_sorted_transcript_consequences_array(..)

    Args:
        vep_sorted_transcript_consequences_root (ArrayExpression):
        include_coding_annotations (bool):
    """

    transcript_consequences = {
        "biotype": hl.tstr,
        "canonical": hl.tint,
        "category": hl.tstr,
        "cdna_start": hl.tint,
        "cdna_end": hl.tint,
        "codons": hl.tstr,
        "gene_id": hl.tstr,
        "gene_symbol": hl.tstr,
        "hgvs": hl.tstr,
        "hgvsc": hl.tstr,
        "major_consequence": hl.tstr,
        "major_consequence_rank": hl.tint,
        "transcript_id": hl.tstr,
    }

    if include_coding_annotations:
        transcript_consequences.update(
            {
                "amino_acids": hl.tstr,
                "domains": hl.tstr,
                "hgvsp": hl.tstr,
                "lof": hl.tstr,
                "lof_flags": hl.tstr,
                "lof_filter": hl.tstr,
                "lof_info": hl.tstr,
                "polyphen_prediction": hl.tstr,
                "protein_id": hl.tstr,
                "sift_prediction": hl.tstr,
            }
        )

    return hl.cond(
        vep_sorted_transcript_consequences_root.size() == 0,
        hl.struct(**{field: hl.null(field_type) for field, field_type in transcript_consequences.items()}),
        hl.bind(
            lambda worst_transcript_consequence: (
                worst_transcript_consequence.annotate(
                    domains=hl.delimit(hl.set(worst_transcript_consequence.domains))
                ).select(*transcript_consequences.keys())
            ),
            vep_sorted_transcript_consequences_root[0],
        ),
    )

def get_expr_for_filtering_allele_frequency(ac_field="va.AC[va.aIndex - 1]", an_field="va.AN", confidence_interval=0.95):
    """Compute the filtering allele frequency for the given AC, AN and confidence interval."""
    if not (0 < confidence_interval < 1):
        raise ValueError("Invalid confidence interval: %s. Confidence interval must be between 0 and 1." % confidence_interval)
    return "filtering_allele_frequency(%(ac_field)s, %(an_field)s, %(confidence_interval)s)"

def convert_vds_schema_string_to_annotate_variants_expr(
        top_level_fields="",
        info_fields="",
        other_source_fields="",
        other_source_root="",
        root="",
        split_multi=True):
    """Takes a string representation of the VDS variant_schema and generates a string expression
    that can be passed to hail's annotate_variants_expr function to clean up the data shape to:
    1. flatten the data so that VCF "top_level" fields and "INFO" fields now appear at the same level
    2. discard unused fields
    3. convert all Array-type values to a single value in the underlying primitive type by
        applying [va.aIndex - 1]. This assumes that split_multi() has already been run to
        split multi-allelic variants.
    Args:
        top_level_fields (str): VDS fields that are direct children of the 'va' struct. For example:
            '''
                rsid: String,
                qual: Double,
                filters: Set[String],
                pass: Boolean,
            '''
        info_fields (str): For example:
            '''
                AC: Array[Int],
                AF: Array[Double],
                AN: Int,
            '''
        root (str): Where to attach the new data shape in the 'va' data struct.
    Returns:
        string:
    """
    fields = []
    if top_level_fields:
        fields += [("va", top_level_fields)]
    if info_fields:
        fields += [("va.info", info_fields)]
    if other_source_root and other_source_fields:
        fields += [(other_source_root, other_source_fields)]

    expr_lines = []
    for source_root, fields_string in fields:
        # in some cases aIndex is @ vds.aIndex instead of va.aIndex
        aIndex_root = "va" if source_root in ("v", "va") else source_root.split(".")[0]

        for field_name, field_type in parse_field_names_and_types(fields_string):
            field_expr = "%(root)s.%(field_name)s = %(source_root)s.%(field_name)s" % locals()

            if split_multi and field_type.startswith("Array"):
                field_expr += "[%(aIndex_root)s.aIndex-1]" % locals()

            expr_lines.append(field_expr)
