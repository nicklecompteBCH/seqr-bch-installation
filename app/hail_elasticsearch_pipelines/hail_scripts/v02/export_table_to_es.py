import argparse

import hail as hl

from typing import List

from hail_elasticsearch_pipelines.hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient, ELASTICSEARCH_INDEX, ELASTICSEARCH_UPDATE, ELASTICSEARCH_UPSERT

"""
From the hail docs:

Col field is sample id

Row Fields

locus (tlocus or tstruct) – Row key.
    The chromosome (CHROM field) and position (POS field).
    If reference_genome is defined, the type will be
        tlocus parameterized by reference_genome.
    Otherwise, the type will be a tstruct with two fields:
        contig with type tstr
        position with type tint32.

alleles (tarray of tstr) –
    Row key. An array containing the alleles of the variant.
    The reference allele (REF field) is the first element in the array
    and the alternate alleles (ALT field) are the subsequent elements.

filters (tset of tstr) – Set containing all filters applied to a variant.

rsid (tstr) – rsID of the variant.

qual (tfloat64) – Floating-point number in the QUAL field.

info (tstruct) – All INFO fields defined in the VCF header can be found in the struct info.
    Data types match the type specified in the VCF header,
    and if the declared Number is not 1, the result will be stored as an array.

"""



def export_table_to_elasticsearch(ds: hl.MatrixTable, host, index_name, index_type,op,mapid="variant_id", is_vds  = False, port=9200, num_shards=1, block_size=200,write_op=ELASTICSEARCH_INDEX):
    es = ElasticsearchClient(host, port)
    ta = ds.rows().flatten()#.drop('locus','allele')
    #ta = ta.naive_coalesce(450)
    #ta = ta.persist()
    es.export_table_to_elasticsearch(
            ta,
            index_name=index_name,
            index_type_name=index_type,
            block_size=block_size,
            num_shards=num_shards,
            delete_index_before_exporting=False,
            export_globals_to_index_meta=True,
            verbose=True,
            elasticsearch_mapping_id=mapid,
            elasticsearch_write_operation=op
        )


def main():
    p = argparse.ArgumentParser()
    p.add_argument("table", help="URL of Hail table")
    p.add_argument("--host", help="Elasticsearch host or IP address", required=True)
    p.add_argument("--port", help="Elasticsearch port", default=9200, type=int)
    p.add_argument("--index-name", help="Elasticsearch index name", required=True)
    p.add_argument("--index-type", help="Elasticsearch index type", required=True)
    p.add_argument("--num-shards", help="Number of Elasticsearch shards", default=1, type=int)
    p.add_argument("--block-size", help="Elasticsearch block size to use when exporting", default=200, type=int)
    args = p.parse_args()

    export_table_to_elasticsearch(
        args.table, args.host, args.index_name, args.index_type, args.port, args.num_shards, args.block_size
    )


if __name__ == "__main__":
    main()
