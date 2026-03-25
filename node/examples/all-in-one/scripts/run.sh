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

# 🔥 FIXED PATH (4 levels up)
REPO_ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd ${REPO_ROOT}

docker build -t arma-4p1s -f node/examples/all-in-one/Dockerfile .

cd node/examples/all-in-one/scripts

echo "Starting all-in-one container..."

CONTAINER_ID=$(docker run -d \
  -p 6022:6022 -p 6023:6023 \
  -p 6122:6122 -p 6123:6123 \
  -p 6222:6222 -p 6223:6223 \
  -p 6322:6322 -p 6323:6323 \
  -v ${BASE}:/tmp/arma-all-in-one \
  -v ${BASE}/storage:/storage \
  -v $(pwd)/../config:/config \
  arma-4p1s)

echo "Container started: ${CONTAINER_ID}"
echo "Waiting for system to be ready..."

READY=0
for i in {1..30}; do
  CONFIG_READY=0
  PORTS_READY=0

  if [ -f "${BASE}/config/party1/user_config.yaml" ]; then
    CONFIG_READY=1
  fi

  if command -v nc >/dev/null 2>&1; then
    if nc -z localhost 6022 && nc -z localhost 6023; then
      PORTS_READY=1
    fi
  else
    # fallback if nc is not installed yet
    if [ "${CONFIG_READY}" -eq 1 ]; then
      PORTS_READY=1
    fi
  fi

  if [ "${CONFIG_READY}" -eq 1 ] && [ "${PORTS_READY}" -eq 1 ]; then
    echo "System is ready!"
    READY=1
    break
  fi

  echo "Waiting... (${i}) config=${CONFIG_READY} ports=${PORTS_READY}"
  sleep 2
done

if [ "${READY}" -ne 1 ]; then
  echo "ERROR: system did not become ready in time"
  echo "---- CONTAINER LOG ----"
  docker logs ${CONTAINER_ID} || true
  exit 1
fi

echo "======================"
echo "RUNNING TEST (HOST SIDE)"
echo "======================"

# Ensure hostnames exist (safe)
sudo bash -c 'grep -q assembler.p1 /etc/hosts || cat >> /etc/hosts <<EOF
127.0.0.1 assembler.p1 router.p1 consensus.p1 batcher1.p1
127.0.0.1 assembler.p2 router.p2 consensus.p2 batcher1.p2
127.0.0.1 assembler.p3 router.p3 consensus.p3 batcher1.p3
127.0.0.1 assembler.p4 router.p4 consensus.p4 batcher1.p4
EOF'

echo "Starting receiver..."

armageddon receive \
  --config ${BASE}/config/party1/user_config.yaml \
  --pullFromPartyId=1 \
  --expectedTxs=1000 \
  --output=${LOG_DIR}/output1 \
  > ${LOG_DIR}/receiver.log 2>&1 &

RECEIVER_PID=$!
echo "Receiver PID: ${RECEIVER_PID}"

sleep 3

echo "Starting loader..."

armageddon load \
  --config ${BASE}/config/party1/user_config.yaml \
  --transactions=1000 \
  --rate=200 \
  --txSize=300 \
  > ${LOG_DIR}/loader.log 2>&1 &

LOADER_PID=$!
echo "Loader PID: ${LOADER_PID}"

echo "Waiting for loader..."
wait ${LOADER_PID} || echo "Loader exited with error"

echo "Waiting for receiver..."
wait ${RECEIVER_PID} || echo "Receiver exited with error"

echo "======================"
echo "TEST RESULTS"
echo "======================"

if [ -f "${LOG_DIR}/output1" ]; then
  wc -l ${LOG_DIR}/output1
else
  echo "Output file missing"
  echo "---- LOADER LOG ----"
  cat ${LOG_DIR}/loader.log || true
  echo "---- RECEIVER LOG ----"
  cat ${LOG_DIR}/receiver.log || true
  echo "---- CONTAINER LOG ----"
  docker logs ${CONTAINER_ID} || true
fi

echo "======================"
echo "DONE"
echo "======================"