The hail scripts in this repo can be used to pre-process variant callsets and export them to elasticsearch.

Scripts
-------

**./hail_scripts/** - contains hail scripts that can only run in a hail environment or dataproc cluster.

Main hail pipelines:

* `load_dataset_to_es.py` annotation and pre-processing pipeline for GRCh37 and GRCh38 rare disease callsets.
* `load_gnomad_to_es.py` - joins gnomad exome and genome datasets into a structure that contains the info used in the gnomAD browser, and exports this to elasticsearch.
* `run_vep.py` run VEP on a vcf or vds and write the result to a .vds. WARNING: this must run on a cluster created with either `create_cluster_GRCh37.py` or `create_cluster_GRCh38.py`, depending on the genome version of the dataset being annotated.

Utilities:

* `create_subset.py` subsets a vcf or vds to a specific chromosome or locus - useful for creating small datasets for testing.
* `convert_tsv_to_vds.py` converts a .tsv table to a VDS by allowing the user to specify the chrom, pos, ref, alt column names
* `convert_vcf_to_vds.py` import a vcf and writes it out as a vds
* `export_vds_to_tsv.py`  export a subset of vds variants to a .tsv for inspection
* `print_vds_schema.py` print out the vds variant schema
* `print_keytable_schema.py` reads in a tsv and imputes the types. Then prints out the keytable schema.
* `print_vds_stats.py`  print out vds stats such as the schema, variant count, etc.
* `print_elasticsearch_stats.py` connects to an elasticsearch instance and prints current indices and other stats

*NOTE:* Some of the scripts require a running elasticsearch instance. For deploying a stand-alone elasticsearch cluster see: https://github.com/macarthur-lab/elasticsearch-kubernetes-cluster or for deploying one as part of seqr see: https://github.com/macarthur-lab/seqr

**Examples:**

Run VEP:
```
./gcloud_dataproc/v01/create_cluster_GRCh37.py
./gcloud_dataproc/submit.py --hail-version 0.1 ./hail_scripts/v01/run_vep.py gs://<dataset path>
```

Run rare disease callset pipeline:
```
./gcloud_dataproc/v01/create_cluster_GRCh38.py cluster1 2 12 ;   # create cluster with 2 persistent, 12 preemptible nodes

./gcloud_dataproc/submit.py --cluster cluster1 --project seqr-project ./hail_scripts/v01/load_dataset_to_es.py -g 38 --max-samples-per-index 180 --host $ELASTICSEARCH_HOST_IP --num-shards 12  --project-guid my_dataset_name  --sample-type WES  --dataset-type VARIANTS  gs://my-datasets/GRCh38/my_dataset.vcf.gz
```

There's also a shortcut for running the rare disease pipeline which combines the 2 commands above into 1:
```
python ./gcloud_dataproc/load_dataset.py --genome-version 38 --host $ELASTICSEARCH_HOST_IP --project-guid my_dataset_name  --sample-type WES  --dataset-type VARIANTS gs://my-datasets/GRCh38/my_dataset.vcf.gz
```

