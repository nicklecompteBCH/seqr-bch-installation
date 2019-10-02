#!/bin/bash

CLUSTER_ID="j-2CBJCQQWBGTC2"
#$(aws emr list-clusters --active | python -c "import sys, json; print json.load(sys.stdin)['Clusters'][0]['Id']")
ES_NODE_IP="172.31.37.127"
#$(kubectl get nodes -o json | python -c "import sys, json; addrs=json.load(sys.stdin)['items'][0]['status']['addresses']; print [a['address'] for a in addrs if a['type']=='InternalIP'][0]")
ES_NODE_PORT="9200"
#$(kubectl get svc/elasticsearch -o json | python -c "import sys, json; print json.load(sys.stdin)['spec']['ports'][0]['nodePort']")
#PROJECT="$1"
#shift

HAIL_HOME=/home/hadoop/seqr-bch-installation/
SPARK_HOME=/usr/lib/spark
SPARK_ARGS="--py-files \"${SPARK_HOME}/python/lib/py4j-src.zip\""

aws emr add-steps \
    --cluster-id ${CLUSTER_ID} \
    --steps Type=Spark,Name="Import VCF",ActionOnFailure=CONTINUE,Args=[$(echo ${SPARK_ARGS} $@ | sed -e 's/ /,/g')] \/home/hadoop/seqr-bch-installation/app/hail_elasticsearch_pipelines/load_vcfs_to_emr.py
