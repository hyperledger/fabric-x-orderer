#!/bin/sh
set -e

BASE_DIR=/tmp/arma-all-in-one
STORAGE_DIR=/storage
CONFIG=/config/example-deployment.yaml

echo "🔥 PATCH SOURCE CONFIG BEFORE GENERATE (CRITICAL)"

sed -i 's/router.p1/127.0.0.1/g' $CONFIG
sed -i 's/router.p2/127.0.0.1/g' $CONFIG
sed -i 's/router.p3/127.0.0.1/g' $CONFIG
sed -i 's/router.p4/127.0.0.1/g' $CONFIG

sed -i 's/assembler.p1/127.0.0.1/g' $CONFIG
sed -i 's/assembler.p2/127.0.0.1/g' $CONFIG
sed -i 's/assembler.p3/127.0.0.1/g' $CONFIG
sed -i 's/assembler.p4/127.0.0.1/g' $CONFIG

sed -i 's/batcher1.p1/127.0.0.1/g' $CONFIG
sed -i 's/batcher1.p2/127.0.0.1/g' $CONFIG
sed -i 's/batcher1.p3/127.0.0.1/g' $CONFIG
sed -i 's/batcher1.p4/127.0.0.1/g' $CONFIG

sed -i 's/consensus.p1/127.0.0.1/g' $CONFIG
sed -i 's/consensus.p2/127.0.0.1/g' $CONFIG
sed -i 's/consensus.p3/127.0.0.1/g' $CONFIG
sed -i 's/consensus.p4/127.0.0.1/g' $CONFIG

echo "Generating config..."

armageddon generate \
  --config=$CONFIG \
  --output=${BASE_DIR}

echo "Patching ports..."

for i in 1 2 3 4; do
  PARTY_DIR=${BASE_DIR}/config/party${i}

  OFFSET=$(( (i - 1) * 100 ))

  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_router.yaml
  sed -i "s/ListenPort:.*/ListenPort: $((6022 + OFFSET))/" ${PARTY_DIR}/local_config_router.yaml

  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_assembler.yaml
  sed -i "s/ListenPort:.*/ListenPort: $((6023 + OFFSET))/" ${PARTY_DIR}/local_config_assembler.yaml

  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_batcher1.yaml
  sed -i "s/ListenPort:.*/ListenPort: $((6024 + OFFSET))/" ${PARTY_DIR}/local_config_batcher1.yaml

  sed -i "s/ListenAddress:.*/ListenAddress: 0.0.0.0/" ${PARTY_DIR}/local_config_consenter.yaml
  sed -i "s/ListenPort:.*/ListenPort: $((6025 + OFFSET))/" ${PARTY_DIR}/local_config_consenter.yaml

  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/router|g" ${PARTY_DIR}/local_config_router.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/assembler|g" ${PARTY_DIR}/local_config_assembler.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/batcher|g" ${PARTY_DIR}/local_config_batcher1.yaml
  sed -i "s|/var/dec-trust/production/orderer/store|${STORAGE_DIR}/party${i}/consenter|g" ${PARTY_DIR}/local_config_consenter.yaml

done

echo "🚀 STARTING SERVICES"

/run_all.sh