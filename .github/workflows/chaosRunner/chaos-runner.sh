#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Chaos runner - stops and starts components in sequential order

TEST_DIR=$1
NUM_PARTIES=$2
NUM_SHARDS=$3

# Read timing configuration from environment or use defaults
INITIAL_WAIT=${CHAOS_INITIAL_WAIT:-300}
STOP_WAIT=${CHAOS_STOP_DURATION:-60}
START_WAIT=${CHAOS_RESTART_WAIT:-60}

echo "=========================================="
echo "Chaos Runner Started"
echo "=========================================="
echo "Configuration:"
echo "  Initial wait: ${INITIAL_WAIT}s"
echo "  Stop duration: ${STOP_WAIT}s"
echo "  Restart wait: ${START_WAIT}s"
echo "=========================================="

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${INITIAL_WAIT}s for network to stabilize..."
sleep $INITIAL_WAIT

# Function to kill and restart a component
kill_and_restart() {
  local component=$1
  local party=$2
  local config_file=$3
  local log_file=$4
  
  # Find and kill the process
  local PID=$(pgrep -f "${component}.*party${party}" | head -1)
  if [ -n "$PID" ]; then
    kill $PID 2>/dev/null
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stopping ${component} (party ${party}) - was PID ${PID}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}) DOWN — waiting ${STOP_WAIT} seconds"
    sleep $STOP_WAIT
    
    # Restart the component
    ./bin/arma ${component} --config=${config_file} >> ${log_file} 2>&1 &
    local NEW_PID=$!
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ${component} (party ${party}) - PID ${NEW_PID}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}) UP — waiting ${START_WAIT} seconds"
    sleep $START_WAIT
  else
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: ${component} (party ${party}) not found"
  fi
}

# Main chaos loop
while true; do
  for party in $(seq 1 $NUM_PARTIES); do
    echo "----------------------------------------------------"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — starting chaos sequence"
    echo "----------------------------------------------------"
    
    # 1. Assembler
    kill_and_restart "assembler" ${party} \
      "${TEST_DIR}/config/party${party}/local_config_assembler.yaml" \
      "assembler${party}.log"
    
    # 2. Consenter
    kill_and_restart "consensus" ${party} \
      "${TEST_DIR}/config/party${party}/local_config_consenter.yaml" \
      "consenter${party}.log"
    
    # 3. Router
    kill_and_restart "router" ${party} \
      "${TEST_DIR}/config/party${party}/local_config_router.yaml" \
      "router${party}.log"
    
    # 4. Batchers (in order)
    for shard in $(seq 1 $NUM_SHARDS); do
      kill_and_restart "batcher" ${party} \
        "${TEST_DIR}/config/party${party}/local_config_batcher${shard}.yaml" \
        "batcher${party}-${shard}.log"
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — chaos sequence DONE"
  done
done
