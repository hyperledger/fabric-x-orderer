#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Main deterministic failure test orchestration script

# Exit on error
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# Parse inputs from environment variables
DURATION=${DURATION_MINUTES:-120}
TX_RATE=${TX_RATE:-1000}
TX_SIZE=${TX_SIZE:-300}
NUM_PARTIES=${NUM_PARTIES:-4}
NUM_SHARDS=${NUM_SHARDS:-2}
FAILURE_RUNNER_ENABLED=${FAILURE_RUNNER_ENABLED:-true}

# Export variables so child processes (like collect-results.sh) can access them
export DURATION TX_RATE TX_SIZE NUM_PARTIES NUM_SHARDS FAILURE_RUNNER_ENABLED

# Calculate total transactions
TOTAL_TXS=$((DURATION * 60 * TX_RATE))

echo "=========================================="
echo "Deterministic Failure Test Configuration"
echo "=========================================="
echo "Duration: ${DURATION} minutes"
echo "TX Rate: ${TX_RATE} tx/s"
echo "TX Size: ${TX_SIZE} bytes"
echo "Total TXs: ${TOTAL_TXS}"
echo "Parties: ${NUM_PARTIES}"
echo "Shards: ${NUM_SHARDS}"
echo "Failure Runner Enabled: ${FAILURE_RUNNER_ENABLED}"
echo "=========================================="

# Create temp directory for test
TEST_DIR=$(mktemp -d -t deterministic-failure-test-XXXXXX)
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
./bin/armageddon generate --config=${CONFIG_PATH} --output=${TEST_DIR} --sampleConfigPath=testutil/fabric/sampleconfig
echo "ARMA configurations generated"

# Create data directory with proper permissions
DATA_DIR="${TEST_DIR}/data"
mkdir -p ${DATA_DIR}
echo "Created data directory: ${DATA_DIR}"

# Fix FileStore Location in all generated config files to use writable temp directory
# Each component needs its own data directory to avoid LevelDB lock conflicts
echo "Updating FileStore Location in generated configs..."
for config_file in ${TEST_DIR}/config/party*/local_config_*.yaml; do
  if [ -f "$config_file" ]; then
    # Extract component type and party from filename
    # e.g., local_config_assembler.yaml → assembler
    # e.g., local_config_batcher1.yaml → batcher1
    filename=$(basename "$config_file")
    component=$(echo "$filename" | sed 's/local_config_//; s/\.yaml//')
    party=$(echo "$config_file" | grep -oP 'party\K[0-9]+')

    # Create unique data path for this component
    component_data="${DATA_DIR}/${component}_party${party}"
    mkdir -p "$component_data"

    # Replace the Location line under FileStore section
    sed -i "s|Location: /var/dec-trust.*|Location: ${component_data}|g" "$config_file"
    # Replace the WALDir line under ConsensusParams (consenter only, no-op on others)
    sed -i "s|WALDir: /var/dec-trust.*|WALDir: ${component_data}/wal|g" "$config_file"
    echo "  Updated: $config_file → $component_data"
  fi
done
echo "FileStore Location updated in all configs (each component has its own data)"

# Remove log files from any previous run so the monitor does not read stale data
echo "Cleaning up log files from previous runs..."
rm -f loader.log
for i in $(seq 1 $NUM_PARTIES); do
  rm -f receiver${i}.log
done
for i in $(seq 1 $NUM_PARTIES); do
  rm -f consenter${i}.log assembler${i}.log router${i}.log
  for j in $(seq 1 $NUM_SHARDS); do
    rm -f batcher${i}-${j}.log
  done
done
echo "Log files cleaned up"

# Start ARMA network
echo "Starting ARMA network..."
"${SCRIPT_DIR}/start-arma-network.sh" "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}"

# Create output directories for receivers
echo "Creating output directories for receivers..."
for i in $(seq 1 $NUM_PARTIES); do
  mkdir -p ${TEST_DIR}/output${i}
  echo "  Created: ${TEST_DIR}/output${i}"
done

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

# Start failure runner (if enabled)
if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
  echo "Starting failure runner..."
  # Write marker so monitor-completion.sh knows failure runner mode is active
  touch "${TEST_DIR}/failure_runner_enabled"
  "${SCRIPT_DIR}/deterministic-failure-runner.sh" "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}" &
  FAILURE_RUNNER_PID=$!
  echo "Started failure runner (PID: ${FAILURE_RUNNER_PID})"
fi

# Monitor completion (with duration timeout)
echo "Monitoring test completion..."
"${SCRIPT_DIR}/monitor-completion.sh" "${NUM_PARTIES}" "${TOTAL_TXS}" "${TEST_DIR}" "${DURATION}"

# Wait a bit for failure runner to see the stop signal and exit gracefully
if [ "$FAILURE_RUNNER_ENABLED" = "true" ] && [ -n "$FAILURE_RUNNER_PID" ]; then
  echo "Waiting for failure runner to stop gracefully..."
  sleep 5

  # Check if it's still running and force kill if needed
  if kill -0 ${FAILURE_RUNNER_PID} 2>/dev/null; then
    echo "Force stopping failure runner..."
    kill ${FAILURE_RUNNER_PID} 2>/dev/null || true
  else
    echo "Failure runner stopped gracefully"
  fi
fi

# Collect results
echo "Collecting results..."
"${SCRIPT_DIR}/collect-results.sh" "${TEST_DIR}" "${NUM_PARTIES}" "${DURATION}"

# Cleanup processes
echo "Cleaning up processes..."
pkill -f "arma " || true
pkill -f "armageddon" || true

echo "=========================================="
echo "✅ Deterministic failure test completed successfully!"
echo "=========================================="
