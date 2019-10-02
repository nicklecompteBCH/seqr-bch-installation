import boto3

emr_client = boto3.client('emr')

emr_cluster = list(filter(lambda x: x['Name'] == 'hail-bch' and x['Status']['State'] == 'WAITING', emr_client.list_clusters()['Clusters']))[0]

CLUSTER_ID = emr_cluster['Id']
ES_NODE_IP = "172.31.37.127"
ES_NODE_PORT = "9200"
PROJECT = "alan_beggs"

HAIL_HOME = "/home/hadoop/seqr-bch-installation/"
SPARK_HOME = "/usr/lib/spark"

function add-step () {
    aws emr add-steps \
        --cluster-id ${CLUSTER_ID} \
        --steps Type=Spark,Name="Import VCF",ActionOnFailure=CONTINUE,Args=[]

}

for url in $@; do
    add-step \
        /home/hadoop/seqr-bch-installation/app/hail_elasticsearch_pipelines/load_vcfs_to_emr.py
done
