#!/usr/bin/env bash

set +x
set +x
echo
echo "==== Installing redis ===="
echo
set -x

wget -nv http://download.redis.io/releases/redis-5.0.5.tar.gz

tar xvzf redis-5.0.5.tar.gz
rm redis-5.0.5.tar.gz

mv redis-5.0.5 redis
cd redis

make
sudo make install

echo 'cd '$(pwd)'
LOG_FILE=$(pwd)/redis.log
(nohup redis-server ${SEQR_DIR}/deploy/docker/redis/redis.conf >& ${LOG_FILE}) &
echo "redis started in background on port 6379. See ${LOG_FILE}"
' | tee start_redis.sh
chmod 777 ./start_redis.sh

set +x

./start_redis.sh

cd ..
