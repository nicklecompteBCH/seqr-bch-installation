import hail as hl
from hail_elasticsearch_pipelines.hail_scripts.v02.utils.hail_utils import write_ht, import_table

def import_cadd_table(path: str, genome_version: str="GRCh37", partitions : int = None) -> hl.Table:
    column_names = {'f0': 'chrom', 'f1': 'pos', 'f2': 'ref', 'f3': 'alt', 'f4': 'RawScore', 'f5': 'PHRED'}
    types = {'f0': hl.tstr, 'f1': hl.tint, 'f4': hl.tfloat32, 'f5': hl.tfloat32}
    cadd_ht = hl.import_table(path, force_bgz=True, comment="#", no_header=True, types=types, min_partitions=partitions)
    cadd_ht = cadd_ht.rename(column_names)
    chrom = hl.format("chr%s", cadd_ht.chrom) if genome_version == "38" else cadd_ht.chrom
    locus = hl.locus(chrom, cadd_ht.pos, reference_genome=hl.get_reference(genome_version))
    alleles = hl.array([cadd_ht.ref, cadd_ht.alt])
    cadd_ht = cadd_ht.transmute(locus=locus, alleles=alleles)
    cadd_union_ht = cadd_ht.head(0)
    for contigs in (range(1, 10), list(range(10, 23)) + ["X", "Y", "MT"]):
        contigs = ["chr%s" % contig for contig in contigs] if genome_version == "38" else contigs
        cadd_ht_subset = cadd_ht.filter(hl.array(list(map(str, contigs))).contains(cadd_ht.locus.contig))
        cadd_union_ht = cadd_union_ht.union(cadd_ht_subset)
    cadd_union_ht = cadd_union_ht.key_by("locus", "alleles")
    cadd_union_ht.describe()
    return cadd_union_ht

def get_cadd(partitions : int = None,namenode : str = "",s3 = False):

    snvs_ht = None
    indel_ht = None
    if s3:
        snvs_ht = import_cadd_table("s3://seqr-resources/GRCh37/CADD/whole_genome_SNVs.v1.4.tsv.bgz",partitions=partitions)
        indel_ht = import_cadd_table("s3://seqr-resources/GRCh37/CADD/InDels.v1.4.tsv.bgz",partitions=partitions)
    else:
        snvs_ht = import_cadd_table("hdfs://" + namenode  + "/user/hdfs/data/whole_genome_SNVs.v1.4.tsv.bgz",partitions=partitions)
        indel_ht = import_cadd_table("hdfs://" + namenode + "/user/hdfs/data/InDels.v1.4.tsv.bgz",partitions=partitions)

    snvs_ht = snvs_ht.split_multi_hts(snvs_ht)

    if not s3:
        snvs_ht.write('hdfs:///user/hdfs/data/caddsnvs.mt')

    return (snvs_ht, indel_ht)

def annotate_with_cadd(ht : hl.MatrixTable, cadd_ht : hl.Table):
    newht = ht.annotate_rows(
        cadd = hl.struct(
            PHRED = cadd_ht.index(ht.locus, ht.alleles).PHRED
        )
    )
    return newht