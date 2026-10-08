#!/bin/sh
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -e

BASE_DIR=/tmp/arma-all-in-one

# $1 is party1's operations port; each next party is +100 (see entrypoint.sh)
wait_healthy() {
  for i in 1 2 3 4; do
    curl -sf --retry 100 --retry-delay 1 --retry-connrefused "http://127.0.0.1:$(( $1 + (i - 1) * 100 ))/healthz" > /dev/null
  done
}

for i in 1 2 3 4; do
  arma consensus --config=${BASE_DIR}/config/party${i}/local_config_consenter.yaml &
done

wait_healthy 8025

for i in 1 2 3 4; do
  arma batcher --config=${BASE_DIR}/config/party${i}/local_config_batcher1.yaml &
done

wait_healthy 8024

for i in 1 2 3 4; do
  arma assembler --config=${BASE_DIR}/config/party${i}/local_config_assembler.yaml &
done

wait_healthy 8023

for i in 1 2 3 4; do
  arma router --config=${BASE_DIR}/config/party${i}/local_config_router.yaml &
done

wait