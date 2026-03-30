#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -euxo pipefail

BASE=/tmp/arma-all-in-one
LOG_DIR=${BASE}/logs

rm -rf ${BASE}
mkdir -p ${BASE}/storage
mkdir -p ${LOG_DIR}

for i in 1 2 3 4; do
  mkdir -p ${BASE}/storage/party${i}/{router,assembler,batcher,consenter}
done

REPO_ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd ${REPO_ROOT}

echo "Starting all-in-one container..."

CONTAINER_ID=$(docker run -d \
  -p 6022:6022 -p 6023:6023 -p 6024:6024 -p 6025:6025 \
  -p 6122:6122 -p 6123:6123 -p 6124:6124 -p 6125:6125 \
  -p 6222:6222 -p 6223:6223 -p 6224:6224 -p 6225:6225 \
  -p 6322:6322 -p 6323:6323 -p 6324:6324 -p 6325:6325 \
  -v ${BASE}:/tmp/arma-all-in-one \
  -v ${BASE}/storage:/storage \
  -v ${REPO_ROOT}/node/examples/all-in-one/config:/config \
  arma-4p1s)

echo "Container started: ${CONTAINER_ID}"

echo "Waiting for config generation..."
for i in {1..30}; do
  if [ -f "${BASE}/config/party1/user_config.yaml" ]; then
    echo "Config ready"
    break
  fi
  sleep 1
done

if [ ! -f "${BASE}/config/party1/user_config.yaml" ]; then
  echo "ERROR: config not generated"
  docker logs ${CONTAINER_ID}
  exit 1
fi

echo "Waiting a bit for services..."
sleep 5

echo "---- PORT STATUS ----"
for port in 6022 6023 6024 6025 6122 6123 6124 6125 6222 6223 6224 6225 6322 6323 6324 6325; do
  if nc -z localhost $port 2>/dev/null; then
    echo "PORT $port OPEN"
  else
    echo "PORT $port CLOSED"
  fi
done

echo "---- LOGS ----"
docker logs ${CONTAINER_ID} | tail -n 50

echo "RUNNING TEST..."

# create output directory for receiver
mkdir -p ${LOG_DIR}/output1

docker exec ${CONTAINER_ID} sh -c "
  armageddon receive \
    --config=/tmp/arma-all-in-one/config/party1/user_config.yaml \
    --pullFromPartyId=1 \
    --expectedTxs=1000 \
    --output=/tmp/arma-all-in-one/logs/output1 \
    > /tmp/arma-all-in-one/logs/receiver.log 2>&1 &

  sleep 3

  armageddon load \
    --config=/tmp/arma-all-in-one/config/party1/user_config.yaml \
    --transactions=1000 \
    --rate=200 \
    --txSize=300 \
    > /tmp/arma-all-in-one/logs/loader.log 2>&1

  wait
"

echo "======================"
echo "RESULT SUMMARY"
echo "======================"

echo "Loader result:"
grep "Load command finished" ${LOG_DIR}/loader.log || echo "Loader summary not found"

echo ""
echo "Receiver result:"
grep "successfully received" ${LOG_DIR}/receiver.log || echo "Receiver summary not found"

echo ""
echo "DONE"