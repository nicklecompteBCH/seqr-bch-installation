""" Hail 0.2 port of adding Gnomad results for consumption by Seqr.

For reference (for Nick) I have included the 0.1 functions.

Many functions here taken from https://github.com/macarthur-lab/gnomad_hail/blob/master/utils/gnomad_functions.py

"""

import hail as hl
from enum import Enum
from typing import Union, Tuple

from common_types import LocusInterval

def get_adj_expr(
        gt_expr: hl.expr.CallExpression,
        gq_expr: Union[hl.expr.Int32Expression, hl.expr.Int64Expression],
        dp_expr: Union[hl.expr.Int32Expression, hl.expr.Int64Expression],
        ad_expr: hl.expr.ArrayNumericExpression,
        adj_gq: int = 20,
        adj_dp: int = 10,
        adj_ab: float = 0.2,
        haploid_adj_dp: int = 10
) -> hl.expr.BooleanExpression:
    """
    https://github.com/macarthur-lab/gnomad_hail/blob/master/utils/gnomad_functions.py
    Gets adj genotype annotation.
    Defaults correspond to gnomAD values.
    """
    return (
            (gq_expr >= adj_gq) &
            hl.cond(
                gt_expr.is_haploid(),
                dp_expr >= haploid_adj_dp,
                dp_expr >= adj_dp
            ) &
            (
                hl.case()
                .when(~gt_expr.is_het(), True)
                .when(gt_expr.is_het_ref(), ad_expr[gt_expr[1]] / dp_expr >= adj_ab)
                .default((ad_expr[gt_expr[0]] / dp_expr >= adj_ab ) & (ad_expr[gt_expr[1]] / dp_expr >= adj_ab ))
            )
    )


def annotate_adj(
        mt: hl.MatrixTable,
        adj_gq: int = 20,
        adj_dp: int = 10,
        adj_ab: float = 0.2,
        haploid_adj_dp: int = 10
) -> hl.MatrixTable:
    """
    https://github.com/macarthur-lab/gnomad_hail/blob/master/utils/gnomad_functions.py
    Annotate genotypes with adj criteria (assumes diploid)
    Defaults correspond to gnomAD values.
    """
    return mt.annotate_entries(adj=get_adj_expr(mt.GT, mt.GQ, mt.DP, mt.AD, adj_gq, adj_dp, adj_ab, haploid_adj_dp))



GNOMAD_SEQR_VDS_PATHS_oh_one = {
    "exomes_37": "s3://seqr-resources/GRCh37/gnomad/gnomad.exomes.r2.0.2.sites.grch37.split.vds",
    "exomes_38": "s3://seqr-resources/GRCh38/gnomad/gnomad.exomes.r2.0.2.sites.liftover_grch38.split.vds",

    "genomes_37": "s3://seqr-resources/GRCh37/gnomad/gnomad.genomes.r2.0.2.sites.grch37.split.vds",
    "genomes_38": "s3://seqr-resources/GRCh38/gnomad/gnomad.genomes.r2.0.2.sites.liftover_grch38.split.vds",
}

GNOMAD_HT_PATHS = {
    "exomes_37": "" #"s3://seqr-resources/GRCh37/gnomad/gnomad.exomes.r2.0.2.sites.grch37.split.vds",
}

def read_gnomad_vds_oh_one(hail_context, genome_version, exomes_or_genomes, subset=None):
    if genome_version not in ("37", "38"):
        raise ValueError("Invalid genome_version: %s. Must be '37' or '38'" % str(genome_version))

    gnomad_vds_path = GNOMAD_SEQR_VDS_PATHS["%s_%s" % (exomes_or_genomes, genome_version)]

    gnomad_vds = hail_context.read(gnomad_vds_path)

    if subset:
        gnomad_vds = gnomad_vds.filter_intervals(hail.Interval.parse(subset))

    return gnomad_vds

class GnomadDataset(Enum):
    Genomes37 = 1
    Genomes38 = 2
    Exomes37 = 1
    Exomes38 = 2

    def get_bch_s3_path(self):
        if self == GnomadDataset.Exomes37:
            return "" # upload to s3
        else:
            raise NotImplementedError("Have not uploaded these yet...")

def read_gnomad_ht(
    gnomad_version: GnomadDataset,
    subset: Union[LocusInterval, None] = None
) -> hl.MatrixTable
:
    gnomad_s3_path = gnomad_version.get_bch_s3_path()
    gnomad_hailtable = hl.import_table(gnomad_s3_path)
    if subset:
        locus_expr = hl.parse_locus_interval(subset.to_hail_expr())
        gnomad_hailtable = gnomad_hailtable.filter(hl.is_defined(gnomad_hailtable[locus_expr]))
    return gnomad_hailtable

def add_gnomad_to_vep_matrixtable(
    vep_results: hl.MatrixTable,
    gnomad_version: GnomadDataset
) -> hl.MatrixTable:

    gnomad_table = read_gnomad_ht(gnomad_version)


    vep_results = vep_results.


    raise NotImplementedError()

def add_gnomad_to_vds_oh_one(hail_context, vds, genome_version, exomes_or_genomes, root=None, top_level_fields=USEFUL_TOP_LEVEL_FIELDS, info_fields=USEFUL_INFO_FIELDS, subset=None, verbose=True):
    if genome_version not in ("37", "38"):
        raise ValueError("Invalid genome_version: %s. Must be '37' or '38'" % str(genome_version))

    if exomes_or_genomes not in ("exomes", "genomes"):
        raise ValueError("Invalid genome_version: %s. Must be 'exomes' or 'genomes'" % str(genome_version))

    if root is None:
        root = "va.gnomad_%s" % exomes_or_genomes

    gnomad_vds = read_gnomad_vds(hail_context, genome_version, exomes_or_genomes, subset=subset)

    if exomes_or_genomes == "genomes":
        # remove any *SAS* fields from genomes since South Asian population only defined for exomes
        info_fields = "\n".join(field for field in info_fields.split("\n") if "SAS" not in field)

    top_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=top_level_fields,
        other_source_root="vds",
    )
    if verbose:
        print(top_fields_expr)

    info_fields_expr = convert_vds_schema_string_to_annotate_variants_expr(
        root=root,
        other_source_fields=info_fields,
        other_source_root="vds.info",
    )
    if verbose:
        print(info_fields_expr)

    expr = []
    if top_fields_expr:
        expr.append(top_fields_expr)
    if info_fields_expr:
        expr.append(info_fields_expr)
    return (vds
        .annotate_variants_vds(gnomad_vds, expr=", ".join(expr))
    )

USEFUL_TOP_LEVEL_FIELDS = ""
USEFUL_INFO_FIELDS = """
    AC: Int,
    Hom: Int,
    Hemi: Int,
    AF: Double,
    AN: Int,
    AF_POPMAX_OR_GLOBAL: Double
"""
