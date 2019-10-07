def get_expr_for_contig(field_prefix="v."):
    """Normalized contig name"""
    return field_prefix+'contig.replace("chr", "")'

def get_expr_for_variant_id(max_length=None):
    """Expression for computing <chrom>-<pos>-<ref>-<alt>
    Args:
        max_length: (optional) length at which to truncate the <chrom>-<pos>-<ref>-<alt> string
    Return:
        string: "<chrom>-<pos>-<ref>-<alt>"
    """
    contig_expr = get_expr_for_contig()
    if max_length is not None:
        return '(%(contig_expr)s + "-" + v.start + "-" + v.ref + "-" + v.alt)[0:%(max_length)s]' % locals()
    else:
        return '%(contig_expr)s + "-" + v.start + "-" + v.ref + "-" + v.alt' % locals()