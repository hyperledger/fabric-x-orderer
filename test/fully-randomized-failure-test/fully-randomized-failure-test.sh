#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Fully randomized failure test — single entry-point script.
#
# Previously split across five files:
#   fully-randomized-failure-test.sh   (main orchestration)
#   start-arma-network.sh              (network startup)
#   fully-randomized-failure-runner.sh (failure injection loop)
#   monitor-completion.sh              (progress monitor)
#   collect-results.sh                 (result collection)
#
# All functions are now defined here and called from the main() function at the
# bottom of this file.  The GitHub Actions workflow and local usage are
# unchanged — just run this script directly.
#
# The key difference from the deterministic test: instead of cycling through
# parties and components in a fixed order, the failure runner picks a victim
# completely at random from the full pool of all components (all assemblers,
# consenters, routers, and batchers across every party).  Any component can be
# selected multiple times in a row; there is no guaranteed ordering.
#
# A status snapshot is printed after every N = (3 + NUM_SHARDS) kills, which
# corresponds roughly to one "equivalent party's worth" of kills — matching
# the conceptual reporting cadence of the deterministic test without imposing
# any ordering on which components are actually killed.

# Exit on error
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
cd "${REPO_ROOT}"

# ---------------------------------------------------------------------------
# Configuration — read from environment variables, fall back to defaults
# ---------------------------------------------------------------------------
DURATION=${DURATION_MINUTES:-120}
TX_RATE=${TX_RATE:-1000}
TX_SIZE=${TX_SIZE:-300}
NUM_PARTIES=${NUM_PARTIES:-4}
NUM_SHARDS=${NUM_SHARDS:-2}
FAILURE_RUNNER_ENABLED=${FAILURE_RUNNER_ENABLED:-true}

# Export variables so subprocesses (receivers, loader, etc.) can access them
export DURATION TX_RATE TX_SIZE NUM_PARTIES NUM_SHARDS FAILURE_RUNNER_ENABLED

# ---------------------------------------------------------------------------
# Seed bash's $RANDOM with the current date/time so every run gets a different
# random sequence while remaining reproducible if you know the start time.
# ---------------------------------------------------------------------------
RANDOM_SEED=$(date +%Y%m%d%H%M%S | tail -c 9)
RANDOM=$RANDOM_SEED
echo "Random seed: ${RANDOM_SEED} (based on $(date -u '+%Y-%m-%d %H:%M:%S UTC'))"

# ---------------------------------------------------------------------------
# wait_for_healthz
#   Polls a single component's log file for the health-check URL logged at
#   startup, then polls that URL until it returns HTTP 200.
#
#   Phase 1 (up to 60 s): wait for the line
#       "Health check serving on URL: http://..." to appear in the log.
#   Phase 2 (up to 60 s): poll the URL with curl until HTTP 200.
#
#   On any timeout the failure reason is written to test-results/startup_failure.txt
#   and the script exits with code 1, aborting the test before it starts.
#
# Args: log_file  label  test_dir
#   log_file — component log file, e.g. consenter1.log
#   label    — human-readable name, e.g. "Consenter 1"
#   test_dir — path to the temp test directory (used only for failure file)
# ---------------------------------------------------------------------------
wait_for_healthz() {
  local log_file=$1
  local label=$2
  local test_dir=$3
  local phase_timeout=60

  # Phase 1: wait for the health URL to appear in the log
  local url=""
  local elapsed=0
  while [ -z "$url" ] && [ $elapsed -lt $phase_timeout ]; do
    url=$(grep -oP 'Health check serving on URL:\s+\K(https?://\S+)' "$log_file" 2>/dev/null || true)
    if [ -z "$url" ]; then
      sleep 1
      elapsed=$((elapsed + 1))
    fi
  done

  if [ -z "$url" ]; then
    local reason="${label} failed to start: health check URL never appeared in log within ${phase_timeout}s"
    echo "❌ ${reason}"
    mkdir -p test-results
    echo "${reason}" > test-results/startup_failure.txt
    exit 1
  fi

  echo "  ${label}: health URL found at ${url}"

  # Phase 2: poll the URL until HTTP 200
  elapsed=0
  while [ $elapsed -lt $phase_timeout ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo "  ✅ ${label} healthy"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  local reason="${label} failed to start: /healthz at ${url} did not return HTTP 200 within ${phase_timeout}s"
  echo "❌ ${reason}"
  mkdir -p test-results
  echo "${reason}" > test-results/startup_failure.txt
  exit 1
}

# ---------------------------------------------------------------------------
# start_arma_network
#   Starts all ARMA network components in the correct order: consenters first,
#   then batchers, assemblers, and routers.  Stores each process PID under
#   ${TEST_DIR}/pids/.
#
#   After starting each component, wait_for_healthz is called to confirm it is
#   up and healthy before proceeding — no fixed sleep is used.
#
# Args: TEST_DIR  NUM_PARTIES  NUM_SHARDS
# ---------------------------------------------------------------------------
start_arma_network() {
  local TEST_DIR=$1
  local NUM_PARTIES=$2
  local NUM_SHARDS=$3

  echo "=========================================="
  echo "Starting ARMA Network"
  echo "=========================================="
  echo "Parties: ${NUM_PARTIES}"
  echo "Shards: ${NUM_SHARDS}"
  echo "=========================================="

  # Create PID directory
  local PID_DIR="${TEST_DIR}/pids"
  mkdir -p ${PID_DIR}
  echo "PID directory: ${PID_DIR}"

  # Save current working directory for later use by failure runner
  local WORK_DIR=$(pwd)
  echo "${WORK_DIR}" > ${PID_DIR}/work_dir.txt
  echo "Working directory: ${WORK_DIR}"

  # Start consenters first and wait for each to be healthy before continuing
  echo "Starting consenters..."
  for i in $(seq 1 $NUM_PARTIES); do
    ./bin/arma consensus \
      --config=${TEST_DIR}/config/party${i}/local_config_consenter.yaml \
      >> consenter${i}.log 2>&1 &
    local PID=$!
    echo ${PID} > ${PID_DIR}/consensus${i}.pid
    echo "  Started consenter ${i} (PID: ${PID})"
    wait_for_healthz "consenter${i}.log" "Consenter ${i}" "${TEST_DIR}"
  done

  # Start batchers and wait for each to be healthy
  echo "Starting batchers..."
  for i in $(seq 1 $NUM_PARTIES); do
    for j in $(seq 1 $NUM_SHARDS); do
      ./bin/arma batcher \
        --config=${TEST_DIR}/config/party${i}/local_config_batcher${j}.yaml \
        >> batcher${i}-${j}.log 2>&1 &
      local PID=$!
      echo ${PID} > ${PID_DIR}/batcher${i}-${j}.pid
      echo "  Started batcher ${i}-${j} (PID: ${PID})"
      wait_for_healthz "batcher${i}-${j}.log" "Batcher ${i}-${j}" "${TEST_DIR}"
    done
  done

  # Start assemblers and wait for each to be healthy
  echo "Starting assemblers..."
  for i in $(seq 1 $NUM_PARTIES); do
    ./bin/arma assembler \
      --config=${TEST_DIR}/config/party${i}/local_config_assembler.yaml \
      >> assembler${i}.log 2>&1 &
    local PID=$!
    echo ${PID} > ${PID_DIR}/assembler${i}.pid
    echo "  Started assembler ${i} (PID: ${PID})"
    wait_for_healthz "assembler${i}.log" "Assembler ${i}" "${TEST_DIR}"
  done

  # Start routers and wait for each to be healthy
  echo "Starting routers..."
  for i in $(seq 1 $NUM_PARTIES); do
    ./bin/arma router \
      --config=${TEST_DIR}/config/party${i}/local_config_router.yaml \
      >> router${i}.log 2>&1 &
    local PID=$!
    echo ${PID} > ${PID_DIR}/router${i}.pid
    echo "  Started router ${i} (PID: ${PID})"
    wait_for_healthz "router${i}.log" "Router ${i}" "${TEST_DIR}"
  done

  echo "=========================================="
  echo "✅ ARMA network started — all components healthy"
  echo "=========================================="
}

# ---------------------------------------------------------------------------
# run_failure_runner
#   Picks a random ARMA component from the full pool of all components on
#   every iteration and kills + restarts it, until the stop signal file is
#   created by monitor_completion.
#
#   The pool is built from NUM_PARTIES × (3 + NUM_SHARDS) entries:
#     assembler, consenter, router, batcher-shard-1 … batcher-shard-N
#   for every party.  Any component can be selected any number of times;
#   there is no guaranteed ordering.
#
#   A status snapshot signal is written to TEST_DIR after every
#   KILLS_PER_REPORT = (3 + NUM_SHARDS) kills, mirroring the per-party
#   reporting cadence of the deterministic test without enforcing order.
#
#   A running kill counter is maintained in ${TEST_DIR}/kill_counter so
#   monitor_completion can display cumulative totals in each snapshot.
#
# Args: TEST_DIR  NUM_PARTIES  NUM_SHARDS
# ---------------------------------------------------------------------------
run_failure_runner() {
  local TEST_DIR=$1
  local NUM_PARTIES=$2
  local NUM_SHARDS=$3

  # Read timing configuration from environment or use defaults
  local STOP_WAIT=${FAILURE_RUNNER_STOP_DURATION:-60}
  local START_WAIT=${FAILURE_RUNNER_RESTART_WAIT:-60}

  # Get PID directory and working directory
  local PID_DIR="${TEST_DIR}/pids"
  local WORK_DIR=$(cat ${PID_DIR}/work_dir.txt 2>/dev/null || pwd)

  # Stop signal file created by monitor_completion when the test ends
  local STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"

  # Number of kills between status snapshots: one "equivalent party's worth"
  local KILLS_PER_REPORT=$((3 + NUM_SHARDS))

  echo "=========================================="
  echo "Fully Randomized Failure Runner Started"
  echo "=========================================="
  echo "Configuration:"
  echo "  Stop duration: ${STOP_WAIT}s"
  echo "  Restart wait: ${START_WAIT}s"
  echo "  Kills per report: ${KILLS_PER_REPORT}"
  echo "  PID directory: ${PID_DIR}"
  echo "  Working directory: ${WORK_DIR}"
  echo "  Stop signal file: ${STOP_SIGNAL}"
  echo "=========================================="

  # No initial wait needed — start_arma_network already confirmed all components
  # are healthy via /healthz before returning.

  # Inner helper: kill and restart a single component
  # Args: component  party  shard(optional)  config_file  log_file
  _kill_and_restart() {
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
      kill $PID 2>/dev/null || true
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
        kill -9 $PID 2>/dev/null || true
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

  # Build flat pool of all components across all parties.
  # Each entry is a space-separated string: "type party [shard]"
  # Examples: "assembler 1"  "consensus 2"  "batcher 3 2"
  local POOL=()
  for p in $(seq 1 $NUM_PARTIES); do
    POOL+=("assembler $p")
    POOL+=("consensus $p")
    POOL+=("router $p")
    for s in $(seq 1 $NUM_SHARDS); do
      POOL+=("batcher $p $s")
    done
  done

  local POOL_SIZE=${#POOL[@]}
  echo "Component pool size: ${POOL_SIZE} (${NUM_PARTIES} parties × $((3 + NUM_SHARDS)) components each)"

  # Initialise kill counter
  echo "0" > "${TEST_DIR}/kill_counter"
  local kill_count=0

  # Main failure loop — run until stop signal is received
  while true; do
    # Check if we should stop (test completed)
    if [ -f "${STOP_SIGNAL}" ]; then
      echo "=========================================="
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop signal received - fully randomized failure runner exiting"
      echo "=========================================="
      break
    fi

    # Pick a random component from the pool
    local idx=$(($RANDOM % POOL_SIZE))
    local entry="${POOL[$idx]}"

    # Parse the entry: "type party [shard]"
    local comp party shard
    read -r comp party shard <<< "$entry"

    # Resolve config file and log file for this component
    local config_file log_file
    if [ "$comp" = "batcher" ]; then
      config_file="${TEST_DIR}/config/party${party}/local_config_batcher${shard}.yaml"
      log_file="batcher${party}-${shard}.log"
    elif [ "$comp" = "assembler" ]; then
      config_file="${TEST_DIR}/config/party${party}/local_config_assembler.yaml"
      log_file="assembler${party}.log"
    elif [ "$comp" = "consensus" ]; then
      config_file="${TEST_DIR}/config/party${party}/local_config_consenter.yaml"
      log_file="consenter${party}.log"
    elif [ "$comp" = "router" ]; then
      config_file="${TEST_DIR}/config/party${party}/local_config_router.yaml"
      log_file="router${party}.log"
    fi

    echo "----------------------------------------------------"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] 🎲 Random pick: ${comp} party ${party}${shard:+ shard ${shard}}"
    echo "----------------------------------------------------"

    _kill_and_restart "$comp" "$party" "$shard" "$config_file" "$log_file"

    # Increment kill counter
    kill_count=$((kill_count + 1))
    echo "${kill_count}" > "${TEST_DIR}/kill_counter"

    # Signal monitor every KILLS_PER_REPORT kills
    if [ $((kill_count % KILLS_PER_REPORT)) -eq 0 ]; then
      touch "${TEST_DIR}/failure_runner_batch_done"
    fi
  done

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Fully randomized failure runner finished (total kills: ${kill_count})"
}

# ---------------------------------------------------------------------------
# monitor_completion
#   Monitors test execution until the configured duration is reached or all
#   components finish early.
#
#   - In failure runner mode: prints a status snapshot after every
#     KILLS_PER_REPORT = (3 + NUM_SHARDS) kills.
#   - Without failure runner: prints a status snapshot every 5 minutes.
#   - Always: stops when the configured duration is reached or when the loader
#     and all receivers finish early.
#
#   After duration expires, stops the loader immediately then gives receivers a
#   30-second drain window before killing them.
#
# Args: NUM_PARTIES  TOTAL_TXS  TEST_DIR  DURATION_MINUTES
# ---------------------------------------------------------------------------
monitor_completion() {
  local NUM_PARTIES=$1
  local TOTAL_TXS=$2
  local TEST_DIR=$3
  local DURATION_MINUTES=$4

  # Calculate end time
  local START_TIME=$(date +%s)
  local END_TIME=$((START_TIME + DURATION_MINUTES * 60))

  # Kills per report matches the runner's cadence
  local KILLS_PER_REPORT=$((3 + NUM_SHARDS))

  echo "=========================================="
  echo "Monitoring Test Completion"
  echo "=========================================="
  echo "Test will run for ${DURATION_MINUTES} minutes"
  echo "Expected TXs: ${TOTAL_TXS}"
  echo "Status snapshot every ${KILLS_PER_REPORT} kills"
  echo "Start time: $(date -d @${START_TIME} '+%Y-%m-%d %H:%M:%S')"
  echo "End time: $(date -d @${END_TIME} '+%Y-%m-%d %H:%M:%S')"
  echo "=========================================="

  # Helper: check if time limit reached
  _time_limit_reached() {
    local CURRENT_TIME=$(date +%s)
    [ $CURRENT_TIME -ge $END_TIME ]
  }

  # Helper: print current stats snapshot
  _get_current_stats() {
    echo ""
    echo "=========================================="
    echo "Current Status at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="

    # Check loader
    # Actual log line: "Load command finished, sent N TXs in ..."
    if grep -q "Load command finished" loader.log 2>/dev/null; then
      local SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
      echo "Loader: ✅ Completed - Sent ${SENT:-unknown} txs total"
    else
      # Sum all per-10s Report lines per router to get exact cumulative sent count per router.
      # Actual log line: "BroadcastClient to Router 127.0.0.1:XXXX sent N transactions in the last 10s"
      echo "Loader: 🔄 Running"
      for i in $(seq 1 $NUM_PARTIES); do
        local SENT_ROUTER=$(grep "BroadcastClientToRouter${i}.*Report" loader.log 2>/dev/null \
          | grep -oP 'sent \K[0-9]+(?= transactions in the last)' \
          | awk '{sum+=$1} END {print sum}')
        echo "  → Router ${i}: ${SENT_ROUTER:-0} txs sent so far"
      done
    fi

    # Check receivers
    for i in $(seq 1 $NUM_PARTIES); do
      if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
        # Actual log line: "N txs were expected and overall N were successfully received"
        local RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K[0-9]+')
        echo "Party ${i}: ✅ Completed - Received ${RECEIVED:-unknown} txs"
      else
        # For running receivers, read cumulative txs from CSV (column 2 = num txs, column 3 = num blocks)
        if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
          local RECEIVED=$(tail -n +3 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$2} END {print sum}')
          echo "Party ${i}: 🔄 Running - Received ${RECEIVED:-0} txs so far"
        else
          echo "Party ${i}: 🔄 Running - Received 0 txs so far"
        fi
      fi
    done

    local CURRENT_TIME=$(date +%s)
    local ELAPSED=$((CURRENT_TIME - START_TIME))
    local REMAINING
    if [ $CURRENT_TIME -ge $END_TIME ]; then
      REMAINING=0
    else
      REMAINING=$((END_TIME - CURRENT_TIME))
    fi
    echo ""
    echo "Time elapsed: $((ELAPSED / 60)) minutes"
    echo "Time remaining: $((REMAINING / 60)) minutes"
    echo "=========================================="
  }

  # Determine if failure runner mode is active via a marker written by main()
  local FAILURE_RUNNER_MODE=false
  if [ -f "${TEST_DIR}/failure_runner_enabled" ]; then
    FAILURE_RUNNER_MODE=true
  fi

  # Monitor with timeout
  echo "Monitoring test progress..."
  local LAST_STATS_TIME=$START_TIME

  while true; do
    local CURRENT_TIME=$(date +%s)

    # Check if duration reached
    if _time_limit_reached; then
      echo ""
      echo "=========================================="
      echo "⏰ Duration limit reached (${DURATION_MINUTES} minutes)"
      echo "=========================================="
      _get_current_stats
      break
    fi

    # Check if all completed early
    local LOADER_DONE=false
    local ALL_RECEIVERS_DONE=true

    if grep -q "Load command finished" loader.log 2>/dev/null; then
      LOADER_DONE=true
    fi

    for i in $(seq 1 $NUM_PARTIES); do
      if ! grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
        ALL_RECEIVERS_DONE=false
        break
      fi
    done

    if [ "$LOADER_DONE" = "true" ] && [ "$ALL_RECEIVERS_DONE" = "true" ]; then
      echo ""
      echo "=========================================="
      echo "✅ All components completed before timeout!"
      echo "=========================================="
      _get_current_stats
      break
    fi

    if [ "$FAILURE_RUNNER_MODE" = "true" ]; then
      # In fully randomized mode: print stats after every KILLS_PER_REPORT kills
      local BATCH_SIGNAL="${TEST_DIR}/failure_runner_batch_done"
      if [ -f "$BATCH_SIGNAL" ]; then
        local TOTAL_KILLS=$(cat "${TEST_DIR}/kill_counter" 2>/dev/null || echo "?")
        echo ""
        echo "=========================================="
        echo "🎲 Randomized batch of ${KILLS_PER_REPORT} kills complete (total kills so far: ${TOTAL_KILLS})"
        echo "=========================================="
        _get_current_stats
        rm -f "$BATCH_SIGNAL"
      fi
    else
      # No failure runner: print stats every 5 minutes
      if [ $((CURRENT_TIME - LAST_STATS_TIME)) -ge 300 ]; then
        _get_current_stats
        LAST_STATS_TIME=$CURRENT_TIME
      fi
    fi

    sleep 5
  done

  # Final statistics
  echo ""
  echo "=========================================="
  echo "Final Test Statistics"
  echo "=========================================="

  # Loader final stats
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    local SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
    echo "Total Sent: ${SENT:-unknown} transactions"
  else
    local SENT=$(grep -oP 'sent \K[0-9]+(?= transactions in the last)' loader.log 2>/dev/null | tail -1)
    echo "Total Sent: ${SENT:-0} transactions (incomplete)"
  fi

  echo ""
  echo "Received per party:"
  for i in $(seq 1 $NUM_PARTIES); do
    if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      local RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K[0-9]+')
      echo "  Party ${i}: ${RECEIVED:-unknown} txs"
    else
      local RECEIVED=$(tail -n +3 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$2} END {print sum}')
      echo "  Party ${i}: ${RECEIVED:-0} txs (incomplete)"
    fi
  done

  echo "=========================================="

  # Signal failure runner and other processes to stop
  if [ -n "$TEST_DIR" ]; then
    local STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"
    touch "${STOP_SIGNAL}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Created stop signal: ${STOP_SIGNAL}"
  fi

  # Stop loader first — no more txs should be sent
  pkill -f "armageddon load" || true

  # Give receivers a drain window to pull remaining blocks from assemblers
  # before killing them.  30s is enough for the assembler backlog to clear.
  echo "Waiting 30s for receivers to drain remaining blocks..."
  local DRAIN_DEADLINE=$(( $(date +%s) + 30 ))
  local ALL_DONE=false
  while [ $(date +%s) -lt $DRAIN_DEADLINE ]; do
    ALL_DONE=true
    for i in $(seq 1 $NUM_PARTIES); do
      if ! grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
        ALL_DONE=false
        break
      fi
    done
    if [ "$ALL_DONE" = "true" ]; then
      echo "All receivers finished draining"
      break
    fi
    sleep 2
  done

  if [ "$ALL_DONE" = "false" ]; then
    echo "Drain window elapsed, stopping receivers"
  fi
  pkill -f "armageddon receive" || true

  echo "=========================================="
  echo "✅ Monitoring completed"
  echo "=========================================="
}

# ---------------------------------------------------------------------------
# collect_results
#   Cleans the test-results/ directory from any previous run, then extracts
#   loader and receiver statistics, copies all component and loader/receiver
#   logs into test-results/logs/ and compresses them with gzip, collects
#   receiver statistics CSV files, and creates a summary report.
#
#   In addition to summary.txt, writes summary-kills.txt that details how
#   many times each component was killed during the run, plus a total.
#
# Args: TEST_DIR  NUM_PARTIES  DURATION
# ---------------------------------------------------------------------------
collect_results() {
  local TEST_DIR=$1
  local NUM_PARTIES=$2
  local DURATION=$3

  echo "=========================================="
  echo "Collecting Results"
  echo "=========================================="

  # Clean and recreate results directories so previous run artifacts never mix in
  rm -rf test-results
  mkdir -p test-results/logs
  mkdir -p test-results/statistics

  # Extract statistics BEFORE compressing/moving logs
  echo "Extracting statistics from logs..."

  # Extract loader statistics
  local SENT LOADER_STATUS
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
    LOADER_STATUS="completed"
  else
    SENT=$(grep -o "Sent [0-9]* transactions" loader.log 2>/dev/null | tail -1 | awk '{print $2}')
    LOADER_STATUS="timeout"
  fi

  # Extract receiver statistics
  declare -a RECEIVER_STATS
  declare -a RECEIVER_STATUS
  for i in $(seq 1 $NUM_PARTIES); do
    if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      # Extract from "1800000 txs were expected and overall 1800186 were successfully received"
      local RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K\d+(?= were successfully received)')
      RECEIVER_STATS[$i]="${RECEIVED:-unknown}"
      RECEIVER_STATUS[$i]="completed"
    else
      # For stopped receivers, check the statistics CSV file
      if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
        local BLOCKS=$(tail -n +4 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$3} END {print sum+0}')
        local RECEIVED=$(tail -n +4 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$2} END {print sum+0}')
        RECEIVER_STATS[$i]="${RECEIVED:-0}"
        RECEIVER_STATUS[$i]="timeout"
      else
        RECEIVER_STATS[$i]="0"
        RECEIVER_STATUS[$i]="no_data"
      fi
    fi
  done

  echo "  Loader: ${SENT:-0} txs (${LOADER_STATUS})"
  for i in $(seq 1 $NUM_PARTIES); do
    echo "  Party ${i}: ${RECEIVER_STATS[$i]} txs (${RECEIVER_STATUS[$i]})"
  done

  # ---------------------------------------------------------------------------
  # Build per-component kill counts from failure_runner.log BEFORE logs are
  # compressed.  The _kill_and_restart helper writes its output to that file,
  # emitting lines like:
  #   "Stopping assembler (party 1) - was PID ..."
  #   "Stopping batcher (party 2 shard 1) - was PID ..."
  # ---------------------------------------------------------------------------
  echo "Counting per-component kills from failure_runner.log..."

  declare -A KILL_COUNTS
  local TOTAL_KILLS=0

  for i in $(seq 1 $NUM_PARTIES); do
    # Assembler
    local key="assembler_party${i}"
    local count; count=$(grep -c "Stopping assembler (party ${i})" failure_runner.log 2>/dev/null) || count=0
    KILL_COUNTS[$key]=$count
    TOTAL_KILLS=$((TOTAL_KILLS + count))

    # Consenter
    key="consenter_party${i}"
    count=$(grep -c "Stopping consensus (party ${i})" failure_runner.log 2>/dev/null) || count=0
    KILL_COUNTS[$key]=$count
    TOTAL_KILLS=$((TOTAL_KILLS + count))

    # Router
    key="router_party${i}"
    count=$(grep -c "Stopping router (party ${i})" failure_runner.log 2>/dev/null) || count=0
    KILL_COUNTS[$key]=$count
    TOTAL_KILLS=$((TOTAL_KILLS + count))

    # Batchers
    for j in $(seq 1 $NUM_SHARDS); do
      key="batcher_party${i}_shard${j}"
      count=$(grep -c "Stopping batcher (party ${i} shard ${j})" failure_runner.log 2>/dev/null) || count=0
      KILL_COUNTS[$key]=$count
      TOTAL_KILLS=$((TOTAL_KILLS + count))
    done
  done

  echo "  Total kills recorded: ${TOTAL_KILLS}"

  # Collect all logs — always, regardless of duration — and gzip them
  echo "Collecting and compressing logs..."
  cp consenter*.log test-results/logs/ 2>/dev/null || true
  cp batcher*.log test-results/logs/ 2>/dev/null || true
  cp assembler*.log test-results/logs/ 2>/dev/null || true
  cp router*.log test-results/logs/ 2>/dev/null || true
  cp loader.log test-results/logs/ 2>/dev/null || true
  cp receiver*.log test-results/logs/ 2>/dev/null || true
  cp failure_runner.log test-results/logs/ 2>/dev/null || true
  gzip test-results/logs/*.log 2>/dev/null || true
  echo "  All logs collected and compressed"

  # Collect statistics from receivers
  echo "Collecting statistics..."
  for i in $(seq 1 $NUM_PARTIES); do
    if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
      cp "${TEST_DIR}/output${i}/statistics.csv" test-results/statistics/party${i}_statistics.csv
      echo "  Collected statistics for party ${i}"
    fi
  done

  # ---------------------------------------------------------------------------
  # Create main summary report
  # ---------------------------------------------------------------------------
  echo "Creating summary report..."
  cat > test-results/summary.txt <<EOF
========================================
Fully Randomized Failure Test Summary
========================================
Date: $(date)
Duration: ${DURATION} minutes
TX Rate: ${TX_RATE} tx/s
TX Size: ${TX_SIZE} bytes
Total TXs Expected: $((DURATION * 60 * TX_RATE))
Parties: ${NUM_PARTIES}
Shards: ${NUM_SHARDS}
Failure Runner Enabled: ${FAILURE_RUNNER_ENABLED}

========================================
Loader Results
========================================
EOF

  if [ "$LOADER_STATUS" = "completed" ]; then
    echo "✅ Loader completed" >> test-results/summary.txt
    echo "Sent: ${SENT:-unknown} transactions" >> test-results/summary.txt
  else
    echo "⏰ Loader stopped by timeout" >> test-results/summary.txt
    echo "Sent: ${SENT:-0} transactions (incomplete)" >> test-results/summary.txt
  fi

  cat >> test-results/summary.txt <<EOF

========================================
Receiver Results
========================================
EOF

  local TOTAL_RECEIVED=0
  local REPRESENTATIVE_RECEIVED
  for i in $(seq 1 $NUM_PARTIES); do
    echo "Party ${i}:" >> test-results/summary.txt

    local RECEIVED="${RECEIVER_STATS[$i]}"
    local STATUS="${RECEIVER_STATUS[$i]}"

    if [ "$STATUS" = "completed" ]; then
      echo "  ✅ Completed - Received: ${RECEIVED} txs" >> test-results/summary.txt
      if [ -n "$RECEIVED" ] && [ "$RECEIVED" != "unknown" ] && [ "$RECEIVED" -gt 0 ] 2>/dev/null; then
        TOTAL_RECEIVED=$((TOTAL_RECEIVED + RECEIVED))
      fi
    elif [ "$STATUS" = "timeout" ]; then
      if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
        local BLOCKS=$(tail -n +4 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$3} END {print sum+0}')
        echo "  ⏰ Stopped by timeout - Received: ${RECEIVED} txs in ${BLOCKS} blocks" >> test-results/summary.txt
      else
        echo "  ⏰ Stopped by timeout - Received: ${RECEIVED} txs" >> test-results/summary.txt
      fi
      if [ -n "$RECEIVED" ] && [ "$RECEIVED" -gt 0 ] 2>/dev/null; then
        TOTAL_RECEIVED=$((TOTAL_RECEIVED + RECEIVED))
      fi
    else
      echo "  ❌ No statistics available" >> test-results/summary.txt
    fi
  done

  # Overall statistics: use the first party that has a non-zero received count
  # as the representative value (party 1 may have no data if it was mid-kill).
  local PARTY_SUCCESS_RATE=0
  local REPRESENTATIVE_RECEIVED=0
  for i in $(seq 1 $NUM_PARTIES); do
    local cand="${RECEIVER_STATS[$i]:-0}"
    if [ -n "$cand" ] && [ "$cand" != "unknown" ] && [ "$cand" -gt 0 ] 2>/dev/null; then
      REPRESENTATIVE_RECEIVED=$cand
      break
    fi
  done
  if [ -n "$SENT" ] && [ "$SENT" -gt 0 ]; then
    if [ "$REPRESENTATIVE_RECEIVED" -gt 0 ] 2>/dev/null; then
      PARTY_SUCCESS_RATE=$((REPRESENTATIVE_RECEIVED * 100 / SENT))
    fi
  fi

  cat >> test-results/summary.txt <<EOF

========================================
Overall Statistics
========================================
Sent: ${SENT:-0} transactions
Expected per party: ${SENT:-0} transactions
Received per party: ${REPRESENTATIVE_RECEIVED:-0} transactions
Success Rate: ${PARTY_SUCCESS_RATE}%
EOF

  echo "" >> test-results/summary.txt
  echo "========================================" >> test-results/summary.txt
  echo "Kill Summary (see summary-kills.txt for full detail)" >> test-results/summary.txt
  echo "========================================" >> test-results/summary.txt
  echo "Total kills during run: $(cat "${TEST_DIR}/kill_counter" 2>/dev/null || echo "${TOTAL_KILLS}")" >> test-results/summary.txt

  echo "" >> test-results/summary.txt
  echo "========================================" >> test-results/summary.txt
  echo "Test Status" >> test-results/summary.txt
  echo "========================================" >> test-results/summary.txt

  if [ -n "$SENT" ] && [ "$SENT" -gt 0 ] && [ "${REPRESENTATIVE_RECEIVED:-0}" -gt 0 ] 2>/dev/null; then
    echo "✅ PASSED - Test ran for ${DURATION} minutes" >> test-results/summary.txt
    echo "   Sent: ${SENT} txs, representative party received: ${REPRESENTATIVE_RECEIVED} txs (${PARTY_SUCCESS_RATE}%)" >> test-results/summary.txt
  elif [ -n "$SENT" ] && [ "$SENT" -gt 0 ]; then
    echo "⚠️  PARTIAL - Loader sent ${SENT} txs but no receiver completed (all were mid-kill at drain time)" >> test-results/summary.txt
  else
    echo "❌ FAILED - No transactions processed" >> test-results/summary.txt
  fi

  # ---------------------------------------------------------------------------
  # Create kill report: summary-kills.txt
  # ---------------------------------------------------------------------------
  echo "Creating kill report..."
  cat > test-results/summary-kills.txt <<EOF
========================================
Fully Randomized Failure Test — Kill Report
========================================
Date: $(date)
Duration: ${DURATION} minutes
Total components in pool: $((NUM_PARTIES * (3 + NUM_SHARDS))) (${NUM_PARTIES} parties × $((3 + NUM_SHARDS)) components each)

========================================
Per-Component Kill Counts
========================================
EOF

  for i in $(seq 1 $NUM_PARTIES); do
    echo "Party ${i}:" >> test-results/summary-kills.txt
    echo "  assembler  party ${i}:          ${KILL_COUNTS[assembler_party${i}]:-0} kills" >> test-results/summary-kills.txt
    echo "  consenter  party ${i}:          ${KILL_COUNTS[consenter_party${i}]:-0} kills" >> test-results/summary-kills.txt
    echo "  router     party ${i}:          ${KILL_COUNTS[router_party${i}]:-0} kills" >> test-results/summary-kills.txt
    for j in $(seq 1 $NUM_SHARDS); do
      echo "  batcher    party ${i} shard ${j}: ${KILL_COUNTS[batcher_party${i}_shard${j}]:-0} kills" >> test-results/summary-kills.txt
    done
  done

  cat >> test-results/summary-kills.txt <<EOF

========================================
Total kills: ${TOTAL_KILLS}
========================================
EOF

  echo "=========================================="
  echo "✅ Results collected in test-results/"
  echo "=========================================="

  # Display both summaries
  cat test-results/summary.txt
  echo ""
  cat test-results/summary-kills.txt
}

# ---------------------------------------------------------------------------
# main — orchestrates the full test run
# ---------------------------------------------------------------------------
main() {
  # Ensure all arma/armageddon processes are killed when the script exits for
  # any reason — normal completion, error, or external signal (Ctrl-C, SIGTERM).
  trap 'pkill -f "arma " 2>/dev/null || true; pkill -f armageddon 2>/dev/null || true' EXIT

  # Calculate total transactions
  local TOTAL_TXS=$((DURATION * 60 * TX_RATE))

  echo "=========================================="
  echo "Fully Randomized Failure Test Configuration"
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
  local TEST_DIR=$(mktemp -d -t fully-randomized-failure-test-XXXXXX)
  echo "Test directory: ${TEST_DIR}"

  # Generate config YAML
  local CONFIG_PATH="${TEST_DIR}/config.yaml"
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
  local DATA_DIR="${TEST_DIR}/data"
  mkdir -p ${DATA_DIR}
  echo "Created data directory: ${DATA_DIR}"

  # Fix FileStore Location in all generated config files to use writable temp directory
  # Each component needs its own data directory to avoid LevelDB lock conflicts
  echo "Updating FileStore Location in generated configs..."
  for config_file in ${TEST_DIR}/config/party*/local_config_*.yaml; do
    if [ -f "$config_file" ]; then
      local filename=$(basename "$config_file")
      local component=$(echo "$filename" | sed 's/local_config_//; s/\.yaml//')
      local party=$(echo "$config_file" | grep -oP 'party\K[0-9]+')

      local component_data="${DATA_DIR}/${component}_party${party}"
      mkdir -p "$component_data"

      sed -i "s|Location: /var/dec-trust.*|Location: ${component_data}|g" "$config_file"
      sed -i "s|WALDir: /var/dec-trust.*|WALDir: ${component_data}/wal|g" "$config_file"
      echo "  Updated: $config_file → $component_data"
    fi
  done
  echo "FileStore Location updated in all configs (each component has its own data)"

  # Remove log files from any previous run so the monitor does not read stale data
  echo "Cleaning up log files from previous runs..."
  rm -f loader.log failure_runner.log
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

  # Kill any stale arma/armageddon processes from previous runs before starting
  # new ones.  The fixed ports 8011-8045 mean any survivor will block startup.
  echo "Killing any stale arma/armageddon processes from previous runs..."
  pkill -f "/bin/arma " 2>/dev/null || true
  pkill -f "armageddon" 2>/dev/null || true
  sleep 1
  echo "Stale processes cleared"

  # Start ARMA network
  echo "Starting ARMA network..."
  start_arma_network "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}"

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
  local LOADER_PID=$!
  echo "Started loader (PID: ${LOADER_PID})"

  # Start failure runner (if enabled)
  local FAILURE_RUNNER_PID=""
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    echo "Starting fully randomized failure runner..."
    # Write marker so monitor_completion knows failure runner mode is active
    touch "${TEST_DIR}/failure_runner_enabled"
    { run_failure_runner "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}" 2>&1 | tee -a failure_runner.log; true; } &
    FAILURE_RUNNER_PID=$!
    echo "Started failure runner (PID: ${FAILURE_RUNNER_PID})"
  fi

  # Monitor completion (with duration timeout)
  echo "Monitoring test completion..."
  monitor_completion "${NUM_PARTIES}" "${TOTAL_TXS}" "${TEST_DIR}" "${DURATION}"

  # Wait a bit for failure runner to see the stop signal and exit gracefully
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ] && [ -n "$FAILURE_RUNNER_PID" ]; then
    echo "Waiting for failure runner to stop gracefully..."
    sleep 5

    if kill -0 ${FAILURE_RUNNER_PID} 2>/dev/null; then
      echo "Force stopping failure runner..."
      kill ${FAILURE_RUNNER_PID} 2>/dev/null || true
    else
      echo "Failure runner stopped gracefully"
    fi
  fi

  # Collect results
  echo "Collecting results..."
  collect_results "${TEST_DIR}" "${NUM_PARTIES}" "${DURATION}"

  # Disable exit-on-error for cleanup — background process exits are non-zero
  # by design (they are killed) and must not abort the script.
  set +e

  # Cleanup processes
  echo "Cleaning up processes..."
  pkill -f "/bin/arma " 2>/dev/null
  pkill -f "armageddon" 2>/dev/null

  echo "=========================================="
  echo "✅ Fully randomized failure test completed successfully!"
  echo "=========================================="
  exit 0
}

main
