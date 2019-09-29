#!/usr/bin/env bash

VERSION=7.3.1

set +x
set +x
echo
echo "==== Install and start elasticsearch ====="
echo
set -x

wget -nv http://artifacts.elastic.co/downloads/elasticsearch/elasticsearch-${VERSION}.tar.gz
tar xzf elasticsearch-${VERSION}.tar.gz
rm elasticsearch-${VERSION}.tar.gz

cd elasticsearch-${VERSION}

echo '
cd '$(pwd)'
LOG_FILE=$(pwd)/elasticsearch.log
(ES_JAVA_OPTS="-Xms3900m -Xmx3900m" nohup ./usr/local/bin/elasticsearch -E network.host=0.0.0.0 >& ${LOG_FILE}) &
sleep 7;
curl http://0.0.0.0:9200
echo "Elasticsearch started in background. See ${LOG_FILE}"
' | tee start_elasticsearch.sh
chmod 777 ./start_elasticsearch.sh

set +x

./start_elasticsearch.sh

cd  ..
