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

# Get PID directory and working directory
PID_DIR="${TEST_DIR}/pids"
WORK_DIR=$(cat ${PID_DIR}/work_dir.txt 2>/dev/null || pwd)

# Create a stop signal file that monitor-completion.sh will create when done
STOP_SIGNAL="${TEST_DIR}/chaos_stop_signal"

echo "=========================================="
echo "Chaos Runner Started"
echo "=========================================="
echo "Configuration:"
echo "  Initial wait: ${INITIAL_WAIT}s"
echo "  Stop duration: ${STOP_WAIT}s"
echo "  Restart wait: ${START_WAIT}s"
echo "  PID directory: ${PID_DIR}"
echo "  Working directory: ${WORK_DIR}"
echo "  Stop signal file: ${STOP_SIGNAL}"
echo "=========================================="

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${INITIAL_WAIT}s for network to stabilize..."
sleep $INITIAL_WAIT

# Function to kill and restart a component
kill_and_restart() {
  local component=$1
  local party=$2
  local shard=$3  # Optional, only for batchers
  local config_file=$4
  local log_file=$5
  
  # Determine PID file name
  local pid_file
  if [ -n "$shard" ]; then
    pid_file="${PID_DIR}/${component}${party}-${shard}.pid"
  else
    pid_file="${PID_DIR}/${component}${party}.pid"
  fi
  
  # Read PID from file
  if [ ! -f "$pid_file" ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: ${component} (party ${party}${shard:+ shard ${shard}}) PID file not found: ${pid_file}"
    return
  fi
  
  local PID=$(cat ${pid_file} 2>/dev/null)
  
  # Check if process is still running
  if [ -z "$PID" ] || ! kill -0 $PID 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] WARNING: ${component} (party ${party}${shard:+ shard ${shard}}) not running (PID: ${PID:-unknown})"
    # Try to restart anyway
  else
    # Kill the process
    kill $PID 2>/dev/null
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stopping ${component} (party ${party}${shard:+ shard ${shard}}) - was PID ${PID}"
    
    # Wait for process to die
    local wait_count=0
    while kill -0 $PID 2>/dev/null && [ $wait_count -lt 10 ]; do
      sleep 1
      wait_count=$((wait_count + 1))
    done
    
    # Force kill if still running
    if kill -0 $PID 2>/dev/null; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] Force killing ${component} (party ${party}${shard:+ shard ${shard}})"
      kill -9 $PID 2>/dev/null
    fi
  fi
  
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}${shard:+ shard ${shard}}) DOWN — waiting ${STOP_WAIT} seconds"
  sleep $STOP_WAIT
  
  # Restart the component from the correct working directory
  cd ${WORK_DIR}
  ${WORK_DIR}/bin/arma ${component} --config=${config_file} >> ${log_file} 2>&1 &
  local NEW_PID=$!
  echo ${NEW_PID} > ${pid_file}
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ${component} (party ${party}${shard:+ shard ${shard}}) - PID ${NEW_PID}"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}${shard:+ shard ${shard}}) UP — waiting ${START_WAIT} seconds"
  sleep $START_WAIT
}

# Main chaos loop - run until stop signal is received
while true; do
  # Check if we should stop (test completed)
  if [ -f "${STOP_SIGNAL}" ]; then
    echo "=========================================="
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop signal received - chaos runner exiting"
    echo "=========================================="
    break
  fi
  
  for party in $(seq 1 $NUM_PARTIES); do
    # Check stop signal before each party
    if [ -f "${STOP_SIGNAL}" ]; then
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop signal received - exiting chaos loop"
      break 2
    fi
    
    echo "----------------------------------------------------"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — starting chaos sequence"
    echo "----------------------------------------------------"
    
    # 1. Assembler
    kill_and_restart "assembler" ${party} "" \
      "${TEST_DIR}/config/party${party}/local_config_assembler.yaml" \
      "assembler${party}.log"
    
    # 2. Consenter
    kill_and_restart "consensus" ${party} "" \
      "${TEST_DIR}/config/party${party}/local_config_consenter.yaml" \
      "consenter${party}.log"
    
    # 3. Router
    kill_and_restart "router" ${party} "" \
      "${TEST_DIR}/config/party${party}/local_config_router.yaml" \
      "router${party}.log"
    
    # 4. Batchers (in order)
    for shard in $(seq 1 $NUM_SHARDS); do
      kill_and_restart "batcher" ${party} ${shard} \
        "${TEST_DIR}/config/party${party}/local_config_batcher${shard}.yaml" \
        "batcher${party}-${shard}.log"
    done
    
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — chaos sequence DONE"
  done
done

echo "[$(date '+%Y-%m-%d %H:%M:%S')] Chaos runner finished"
