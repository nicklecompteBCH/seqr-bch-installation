import argparse

import hail as hl

from hail_scripts.v02.utils.elasticsearch_client import ElasticsearchClient

def add_sample_field_to_vds(vds : hl.Table, field_name, field_filter):
    vds.annotate(field_name = set(map(lambda x: x.sample_id, filter(field_filter, vds.genotypes))))


def export_table_to_elasticsearch(ds: hl.Table, host, index_name, index_type, port=9200, num_shards=1, block_size=200):
    for i in range(1,3):
        ds = add_sample_field_to_vds(ds, f"num_alt_{i}",(lambda x: x.num_alt = i))
    es = ElasticsearchClient(host, port)
    es.export_table_to_elasticsearch(
        ds,
        index_name=index_name,
        index_type_name=index_type,
        block_size=block_size,
        num_shards=num_shards,
        delete_index_before_exporting=True,
        export_globals_to_index_meta=True,
        verbose=True,
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
