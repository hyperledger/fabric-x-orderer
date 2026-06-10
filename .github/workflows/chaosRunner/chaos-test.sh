#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Main chaos test orchestration script

# Exit on error
set -e  

# Parse inputs from environment variables
DURATION=${DURATION_MINUTES:-120}
TX_RATE=${TX_RATE:-1000}
TX_SIZE=${TX_SIZE:-300}
NUM_PARTIES=${NUM_PARTIES:-4}
NUM_SHARDS=${NUM_SHARDS:-2}
CHAOS_ENABLED=${CHAOS_ENABLED:-true}

# Calculate total transactions
TOTAL_TXS=$((DURATION * 60 * TX_RATE))

echo "=========================================="
echo "Chaos Test Configuration"
echo "=========================================="
echo "Duration: ${DURATION} minutes"
echo "TX Rate: ${TX_RATE} tx/s"
echo "TX Size: ${TX_SIZE} bytes"
echo "Total TXs: ${TOTAL_TXS}"
echo "Parties: ${NUM_PARTIES}"
echo "Shards: ${NUM_SHARDS}"
echo "Chaos Enabled: ${CHAOS_ENABLED}"
echo "=========================================="

# Create temp directory for test
TEST_DIR=$(mktemp -d -t chaos-test-XXXXXX)
echo "Test directory: ${TEST_DIR}"

# Generate config YAML
CONFIG_PATH="${TEST_DIR}/config.yaml"
echo "Generating config at ${CONFIG_PATH}..."

cat > ${CONFIG_PATH} <<EOF
Parties:
EOF

# Generate party configurations
for i in $(seq 1 $NUM_PARTIES); do
  cat >> ${CONFIG_PATH} <<EOF
  - ID: $i
    AssemblerEndpoint: "127.0.0.1:$((8000 + i * 10 + 1))"
    ConsenterEndpoint: "127.0.0.1:$((8000 + i * 10 + 2))"
    RouterEndpoint: "127.0.0.1:$((8000 + i * 10 + 3))"
    BatchersEndpoints:
EOF
  for j in $(seq 1 $NUM_SHARDS); do
    echo "      - \"127.0.0.1:$((8000 + i * 10 + 3 + j))\"" >> ${CONFIG_PATH}
  done
done

cat >> ${CONFIG_PATH} <<EOF
UseTLSRouter: "none"
UseTLSAssembler: "none"
EOF

echo "Config generated successfully"

# Generate arma configs using armageddon
echo "Generating ARMA configurations..."
./bin/armageddon generate --config=${CONFIG_PATH} --output=${TEST_DIR}
echo "ARMA configurations generated"

# Start ARMA network
echo "Starting ARMA network..."
.github/workflows/chaosRunner/start-arma-network.sh ${TEST_DIR} ${NUM_PARTIES} ${NUM_SHARDS}

# Start receivers (background)
echo "Starting receivers..."
for i in $(seq 1 $NUM_PARTIES); do
  ./bin/armageddon receive \
    --config=${TEST_DIR}/config/party${i}/user_config.yaml \
    --pullFromPartyId=${i} \
    --expectedTxs=${TOTAL_TXS} \
    --output=${TEST_DIR}/output${i} \
    >> receiver${i}.log 2>&1 &
  echo "Started receiver for party ${i} (PID: $!)"
done

# Start loader (background)
echo "Starting loader..."
./bin/armageddon load \
  --config=${TEST_DIR}/config/party1/user_config.yaml \
  --transactions=${TOTAL_TXS} \
  --rate=${TX_RATE} \
  --txSize=${TX_SIZE} \
  >> loader.log 2>&1 &
LOADER_PID=$!
echo "Started loader (PID: ${LOADER_PID})"

# Start chaos runner (if enabled)
if [ "$CHAOS_ENABLED" = "true" ]; then
  echo "Starting chaos runner..."
  .github/workflows/chaosRunner/chaos-runner.sh ${TEST_DIR} ${NUM_PARTIES} ${NUM_SHARDS} &
  CHAOS_PID=$!
  echo "Started chaos runner (PID: ${CHAOS_PID})"
fi

# Monitor completion
echo "Monitoring test completion..."
.github/workflows/chaosRunner/monitor-completion.sh ${NUM_PARTIES} ${TOTAL_TXS}

# Stop chaos runner
if [ "$CHAOS_ENABLED" = "true" ] && [ -n "$CHAOS_PID" ]; then
  echo "Stopping chaos runner..."
  kill ${CHAOS_PID} 2>/dev/null || true
fi

# Collect results
echo "Collecting results..."
.github/workflows/chaosRunner/collect-results.sh ${TEST_DIR} ${NUM_PARTIES} ${DURATION}

# Cleanup processes
echo "Cleaning up processes..."
pkill -f "arma " || true
pkill -f "armageddon" || true

echo "=========================================="
echo "✅ Chaos test completed successfully!"
echo "=========================================="
