#!/usr/bin/env bash

KIBANA_VERSION=6.8.3
if [ $PLATFORM == "macos" ]; then
    KIBANA_PLATFORM="darwin"
else
    KIBANA_PLATFORM="linux"
fi


set +x
set +x
echo
echo "==== Install and start kibana ====="
echo
set -x

wget -nv https://artifacts.elastic.co/downloads/kibana/kibana-${KIBANA_VERSION}-${KIBANA_PLATFORM}-x86_64.tar.gz
tar xzf kibana-${KIBANA_VERSION}-${KIBANA_PLATFORM}-x86_64.tar.gz
rm kibana-${KIBANA_VERSION}-${KIBANA_PLATFORM}-x86_64.tar.gz

cd kibana-${KIBANA_VERSION}-${KIBANA_PLATFORM}-x86_64

echo '
cd '$(pwd)'
LOG_FILE=$(pwd)/kibana.log
(nohup ./bin/kibana >& ${LOG_FILE}) &
echo "Kibana started in background. See ${LOG_FILE}"
' | tee start_kibana.sh
chmod 777 ./start_kibana.sh

set +x

./start_kibana.sh

cd  ..
