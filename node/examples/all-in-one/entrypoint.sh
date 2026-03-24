#!/bin/sh
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -e

echo "127.0.0.1 assembler.p1 consensus.p1 router.p1 batcher1.p1" >> /etc/hosts
echo "127.0.0.1 assembler.p2 consensus.p2 router.p2 batcher1.p2" >> /etc/hosts
echo "127.0.0.1 assembler.p3 consensus.p3 router.p3 batcher1.p3" >> /etc/hosts
echo "127.0.0.1 assembler.p4 consensus.p4 router.p4 batcher1.p4" >> /etc/hosts

BASE_DIR=/tmp/arma-all-in-one
STORAGE_DIR=/storage

echo "Generating config..."
armageddon generate \
  --config=/config/example-deployment.yaml \
  --output=${BASE_DIR}

echo "Patching configs..."

for i in 1 2 3 4; do
  PARTY_DIR=${BASE_DIR}/config/party${i}

  OFFSET=$(( (i - 1) * 100 ))
  ROUTER_PORT=$((6022 + OFFSET))
  ASSEMBLER_PORT=$((6023 + OFFSET))
  BATCHER_PORT=$((6024 + OFFSET))
  CONSENTER_PORT=$((6025 + OFFSET))

  # ROUTER
  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_router.yaml
  sed -i "s/ListenPort:.*/ListenPort: ${ROUTER_PORT}/" ${PARTY_DIR}/local_config_router.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/router|g" ${PARTY_DIR}/local_config_router.yaml

  # FIX RATE LIMITER
  sed -i "s/RateLimiterCapacity:.*/RateLimiterCapacity: 1000/" ${PARTY_DIR}/local_config_router.yaml
  sed -i "s/RateLimiterRefillInterval:.*/RateLimiterRefillInterval: 100ms/" ${PARTY_DIR}/local_config_router.yaml

  # ASSEMBLER
  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_assembler.yaml
  sed -i "s/ListenPort:.*/ListenPort: ${ASSEMBLER_PORT}/" ${PARTY_DIR}/local_config_assembler.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/assembler|g" ${PARTY_DIR}/local_config_assembler.yaml

  # BATCHER
  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_batcher1.yaml
  sed -i "s/ListenPort:.*/ListenPort: ${BATCHER_PORT}/" ${PARTY_DIR}/local_config_batcher1.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/batcher|g" ${PARTY_DIR}/local_config_batcher1.yaml

  # CONSENTER
  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_consenter.yaml
  sed -i "s/ListenPort:.*/ListenPort: ${CONSENTER_PORT}/" ${PARTY_DIR}/local_config_consenter.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/consenter|g" ${PARTY_DIR}/local_config_consenter.yaml

done

echo "Starting services..."

# 🔥 run ONLY servers
/run_all.sh