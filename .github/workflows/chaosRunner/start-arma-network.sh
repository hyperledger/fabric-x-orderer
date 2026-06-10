#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Start all ARMA network components

TEST_DIR=$1
NUM_PARTIES=$2
NUM_SHARDS=$3

echo "=========================================="
echo "Starting ARMA Network"
echo "=========================================="
echo "Parties: ${NUM_PARTIES}"
echo "Shards: ${NUM_SHARDS}"
echo "=========================================="

# Start consenters first
echo "Starting consenters..."
for i in $(seq 1 $NUM_PARTIES); do
  ./bin/arma consensus \
    --config=${TEST_DIR}/config/party${i}/local_config_consenter.yaml \
    >> consenter${i}.log 2>&1 &
  echo "  Started consenter ${i} (PID: $!)"
done

# Wait for consenters to initialize
echo "Waiting 5 seconds for consenters to initialize..."
sleep 5

# Start batchers
echo "Starting batchers..."
for i in $(seq 1 $NUM_PARTIES); do
  for j in $(seq 1 $NUM_SHARDS); do
    ./bin/arma batcher \
      --config=${TEST_DIR}/config/party${i}/local_config_batcher${j}.yaml \
      >> batcher${i}-${j}.log 2>&1 &
    echo "  Started batcher ${i}-${j} (PID: $!)"
  done
done

# Start assemblers
echo "Starting assemblers..."
for i in $(seq 1 $NUM_PARTIES); do
  ./bin/arma assembler \
    --config=${TEST_DIR}/config/party${i}/local_config_assembler.yaml \
    >> assembler${i}.log 2>&1 &
  echo "  Started assembler ${i} (PID: $!)"
done

# Start routers
echo "Starting routers..."
for i in $(seq 1 $NUM_PARTIES); do
  ./bin/arma router \
    --config=${TEST_DIR}/config/party${i}/local_config_router.yaml \
    >> router${i}.log 2>&1 &
  echo "  Started router ${i} (PID: $!)"
done

# Wait for network to stabilize
echo "Waiting 10 seconds for network to stabilize..."
sleep 10

echo "=========================================="
echo "✅ ARMA network started successfully"
echo "=========================================="
