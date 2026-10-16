#!/bin/sh
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -e

BASE_DIR=/tmp/arma-all-in-one

# Waits up to 100s for the given node configs to answer on /healthz (see healthcheck.sh)
wait_healthy() {
  for _ in $(seq 1 50); do
    sleep 2
    /healthcheck.sh "$1" && return
  done
  return 1
}

for i in 1 2 3 4; do
  arma consensus --config=${BASE_DIR}/config/party${i}/local_config_consenter.yaml &
done

wait_healthy local_config_consenter.yaml

for i in 1 2 3 4; do
  arma batcher --config=${BASE_DIR}/config/party${i}/local_config_batcher1.yaml &
done

wait_healthy local_config_batcher1.yaml

for i in 1 2 3 4; do
  arma assembler --config=${BASE_DIR}/config/party${i}/local_config_assembler.yaml &
done

wait_healthy local_config_assembler.yaml

for i in 1 2 3 4; do
  arma router --config=${BASE_DIR}/config/party${i}/local_config_router.yaml &
done

wait