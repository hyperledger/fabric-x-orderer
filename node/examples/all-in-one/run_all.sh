#!/bin/sh
set -e

BASE_DIR=/tmp/arma-all-in-one

for i in 1 2 3 4; do
  arma consensus --config=${BASE_DIR}/config/party${i}/local_config_consenter.yaml &
done

sleep 2

for i in 1 2 3 4; do
  arma batcher --config=${BASE_DIR}/config/party${i}/local_config_batcher1.yaml &
done

sleep 2

for i in 1 2 3 4; do
  arma assembler --config=${BASE_DIR}/config/party${i}/local_config_assembler.yaml &
done

sleep 2

for i in 1 2 3 4; do
  arma router --config=${BASE_DIR}/config/party${i}/local_config_router.yaml &
done

wait