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

# Max wait (not a delay) for the last txs; must exceed STOP_DURATION + ~25s recovery (measured)
SUBMIT_DRAIN_SECONDS=${SUBMIT_DRAIN_SECONDS:-120}

# Let submit connect to every router and assembler 1 first; it exits if one is down at startup
FAILURE_RUNNER_START_DELAY=${FAILURE_RUNNER_START_DELAY:-10}

# Export variables so subprocesses (submit, arma nodes) can access them
export DURATION TX_RATE TX_SIZE NUM_PARTIES NUM_SHARDS FAILURE_RUNNER_ENABLED
export SUBMIT_DRAIN_SECONDS FAILURE_RUNNER_START_DELAY

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
    echo "${reason}"
    mkdir -p test-results
    echo "${reason}" > test-results/startup_failure.txt
    exit 1
  fi

  echo "  ${label}: health URL found at ${url}"

  # Phase 2: poll the URL until HTTP 200
  elapsed=0
  while [ $elapsed -lt $phase_timeout ]; do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo "  ${label} healthy"
      return 0
    fi
    sleep 1
    elapsed=$((elapsed + 1))
  done

  local reason="${label} failed to start: /healthz at ${url} did not return HTTP 200 within ${phase_timeout}s"
  echo "${reason}"
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
  echo "ARMA network started, all components healthy"
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
  local START_DELAY=${FAILURE_RUNNER_START_DELAY:-10}

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
  echo "  Start delay: ${START_DELAY}s"
  echo "  Stop duration: ${STOP_WAIT}s"
  echo "  Restart wait: ${START_WAIT}s"
  echo "  Kills per report: ${KILLS_PER_REPORT}"
  echo "  PID directory: ${PID_DIR}"
  echo "  Working directory: ${WORK_DIR}"
  echo "  Stop signal file: ${STOP_SIGNAL}"
  echo "=========================================="

  # Short event line to the console (fd 3, saved by main); everything else goes to failure_runner.log
  _console() { printf '  %s  %s\n' "$(date '+%H:%M:%S')" "$*" >&3 2>/dev/null || true; }

  # Wait before the first kill so submit can connect (see FAILURE_RUNNER_START_DELAY)
  if [ "${START_DELAY}" -gt 0 ]; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${START_DELAY}s before the first kill..."
    _console "waiting ${START_DELAY}s before the first kill"
    sleep "${START_DELAY}"
  fi

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
    _console "${component} party ${party}${shard:+ shard ${shard}} down for ${STOP_WAIT}s"
    sleep $STOP_WAIT

    # Restart the component from the correct working directory
    cd ${WORK_DIR}
    ${WORK_DIR}/bin/arma ${component} --config=${config_file} >> ${log_file} 2>&1 &
    local NEW_PID=$!
    echo ${NEW_PID} > ${pid_file}
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ${component} (party ${party}${shard:+ shard ${shard}}) - PID ${NEW_PID}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}${shard:+ shard ${shard}}) UP — waiting ${START_WAIT} seconds"
    _console "${component} party ${party}${shard:+ shard ${shard}} back up"
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
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Random pick: ${comp} party ${party}${shard:+ shard ${shard}}"
    echo "----------------------------------------------------"
    _console "pick: ${comp} party ${party}${shard:+ shard ${shard}}"

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
#   Prints status snapshots (every KILLS_PER_REPORT kills, or every 5 minutes
#   without the runner) until the duration ends or submit finishes, then waits up
#   to SUBMIT_DRAIN_SECONDS for submit to exit.
#
# Args: NUM_PARTIES  TOTAL_TXS  TEST_DIR  DURATION_MINUTES  SUBMIT_PID
# ---------------------------------------------------------------------------
monitor_completion() {
  local NUM_PARTIES=$1
  local TOTAL_TXS=$2
  local TEST_DIR=$3
  local DURATION_MINUTES=$4
  local SUBMIT_PID=$5

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

  # Status snapshot built from submit.log; $1 is an optional headline
  _get_current_stats() {
    local HEADLINE="${1:-}"
    echo ""
    echo "=========================================="
    if [ -n "$HEADLINE" ]; then
      echo "$HEADLINE"
    fi
    echo "Current Status at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="

    if grep -q "txs were sent to the routers$" submit.log 2>/dev/null; then
      echo "Submit: all ${TOTAL_TXS} txs sent, waiting for the last blocks"
    else
      echo "Submit: sending (target ${TOTAL_TXS} txs)"
    fi

    # Txs sent per router (a killed router's count stays flat)
    for i in $(seq 1 $NUM_PARTIES); do
      local SENT_ROUTER
      SENT_ROUTER=$(grep "BroadcastClientToRouter${i}.*Report" submit.log 2>/dev/null \
        | grep -oP 'sent \K[0-9]+(?= transactions in the last)' \
        | awk '{sum+=$1} END {print sum+0}') || true
      echo "  → Router ${i}: ${SENT_ROUTER:-0} txs sent"
    done

    # submit's connection to assembler 1; DOWN when it lost more times than it reconnected
    local LOST RECOVERED LAST_EVENT
    LOST=$(grep -c "lost connection to assembler" submit.log 2>/dev/null) || LOST=0
    RECOVERED=$(grep -c "reconnected to assembler" submit.log 2>/dev/null) || RECOVERED=0
    if [ "${LOST:-0}" -gt "${RECOVERED:-0}" ]; then
      echo "  assembler 1 connection: DOWN now (lost ${LOST}, reconnected ${RECOVERED})"
      LAST_EVENT=$(grep "lost connection to assembler" submit.log 2>/dev/null | tail -1 | sed 's/^.*-> //') || true
      echo "  last error: ${LAST_EVENT}"
    else
      echo "  assembler 1 connection: lost ${LOST:-0}, reconnected ${RECOVERED:-0}"
    fi

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

  # Print the snapshot in one write so failure runner lines never land inside it
  _print_stats() {
    local BLOCK
    BLOCK=$(_get_current_stats "${1:-}")
    printf '%s\n' "$BLOCK"
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
      _print_stats "Duration limit reached (${DURATION_MINUTES} minutes)"
      break
    fi

    # submit logs this once assembler 1 confirmed every tx
    if grep -q "Submit Finished" submit.log 2>/dev/null; then
      _print_stats "Submit finished before the duration limit"
      break
    fi

    if [ "$FAILURE_RUNNER_MODE" = "true" ]; then
      # In fully randomized mode: print stats after every KILLS_PER_REPORT kills
      local BATCH_SIGNAL="${TEST_DIR}/failure_runner_batch_done"
      if [ -f "$BATCH_SIGNAL" ]; then
        local TOTAL_KILLS
        TOTAL_KILLS=$(cat "${TEST_DIR}/kill_counter" 2>/dev/null) || true
        if [ -z "$TOTAL_KILLS" ]; then
          TOTAL_KILLS="?"
        fi
        _print_stats "Randomized batch of ${KILLS_PER_REPORT} kills complete (total kills so far: ${TOTAL_KILLS})"
        rm -f "$BATCH_SIGNAL"
      fi
    else
      # No failure runner: print stats every 5 minutes
      if [ $((CURRENT_TIME - LAST_STATS_TIME)) -ge 300 ]; then
        _print_stats
        LAST_STATS_TIME=$CURRENT_TIME
      fi
    fi

    sleep 5
  done

  # Stop the failure runner before draining, so nothing goes down while we wait
  if [ -n "$TEST_DIR" ]; then
    local STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"
    touch "${STOP_SIGNAL}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Created stop signal: ${STOP_SIGNAL}"
  fi

  # Watchdog: submit waits forever for a lost tx, so stop it after the drain window
  echo "Waiting up to ${SUBMIT_DRAIN_SECONDS}s for submit to confirm the last txs..."
  ( sleep "${SUBMIT_DRAIN_SECONDS}"; kill "$SUBMIT_PID" 2>/dev/null ) &
  local WATCHDOG=$!

  set +e
  wait "$SUBMIT_PID"
  local SUBMIT_RC=$?
  set -e
  kill "$WATCHDOG" 2>/dev/null || true

  # Informational only; collect_results decides pass/fail from submit.log
  echo "Monitoring completed, submit exited with code ${SUBMIT_RC}"
}

# ---------------------------------------------------------------------------
# collect_results
#   Counts kills, reads the verdict from submit.log, gzips all logs into
#   test-results/logs/, writes summary.txt, summary-kills.txt (artifact only),
#   failure_reason.txt on failure, and the test outcome.
#
# Args: TEST_DIR  NUM_PARTIES  DURATION
# ---------------------------------------------------------------------------
collect_results() {
  local TEST_DIR=$1
  local NUM_PARTIES=$2
  local DURATION=$3

  # Start clean so artifacts from a previous run never mix in
  rm -rf test-results
  mkdir -p test-results/logs

  # Count kills from the runner's "Stopping <comp> (party N[ shard M])" lines
  declare -A KILL_COUNTS
  local TOTAL_KILLS=0
  for i in $(seq 1 $NUM_PARTIES); do
    local key count
    for comp in assembler consensus router; do
      case "$comp" in
        consensus) key="consenter_party${i}" ;;
        *)         key="${comp}_party${i}" ;;
      esac
      count=$(grep -c "Stopping ${comp} (party ${i})" failure_runner.log 2>/dev/null) || count=0
      KILL_COUNTS[$key]=$count
      TOTAL_KILLS=$((TOTAL_KILLS + count))
    done
    for j in $(seq 1 $NUM_SHARDS); do
      key="batcher_party${i}_shard${j}"
      count=$(grep -c "Stopping batcher (party ${i} shard ${j})" failure_runner.log 2>/dev/null) || count=0
      KILL_COUNTS[$key]=$count
      TOTAL_KILLS=$((TOTAL_KILLS + count))
    done
  done

  # Read the results from submit.log; grep results assigned to variables are `|| true` guarded so set -e can't abort the summary
  # The `$` anchor skips the final result line, which contains the same text
  local ALL_SENT=false
  if grep -q "txs were sent to the routers$" submit.log 2>/dev/null; then
    ALL_SENT=true
  fi

  # The only pass signal: a lost tx means submit never logs this line
  local VERDICT="none"
  if grep -q "received by assembler" submit.log 2>/dev/null; then
    VERDICT="passed"
  fi

  # Successful reconnects, as proof the failures happened ("as broken" lines overcount)
  local ROUTER_RECONNECTS ASSEMBLER_RECONNECTS
  ROUTER_RECONNECTS=$(grep -c "Reconnection to router: .* succeeded" submit.log 2>/dev/null) || ROUTER_RECONNECTS=0
  ASSEMBLER_RECONNECTS=$(grep -c "reconnected to assembler" submit.log 2>/dev/null) || ASSEMBLER_RECONNECTS=0

  # From submit's final SUCCESS line (only logged on a pass)
  local NUM_BLOCKS AVG_DELAY
  NUM_BLOCKS=$(grep -oP 'num of blocks: \K[0-9]+' submit.log 2>/dev/null | tail -1) || true
  AVG_DELAY=$(grep -oP 'avg\. tx delay: \K[0-9.]+' submit.log 2>/dev/null | tail -1) || true

  # Collect and compress logs
  cp consenter*.log test-results/logs/ 2>/dev/null || true
  cp batcher*.log test-results/logs/ 2>/dev/null || true
  cp assembler*.log test-results/logs/ 2>/dev/null || true
  cp router*.log test-results/logs/ 2>/dev/null || true
  cp submit.log test-results/logs/ 2>/dev/null || true
  cp failure_runner.log test-results/logs/ 2>/dev/null || true
  gzip test-results/logs/*.log 2>/dev/null || true

  # Summary report, also shown in the GitHub job summary
  local VERDICT_LINE="PASSED: assembler 1 confirmed all ${TOTAL_TXS} txs"
  if [ "$VERDICT" != "passed" ]; then
    VERDICT_LINE="FAILED: submit logged no verification result, so a tx it sent was never confirmed, or it exited early"
  fi

  local SENT_NOTE="all sent"
  if [ "$ALL_SENT" != "true" ]; then
    SENT_NOTE="stopped while still sending"
  fi

  local RUNNER_NOTE="failure runner disabled"
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    RUNNER_NOTE="failure runner enabled"
  fi

  local BLOCKS_NOTE="unknown, submit did not finish"
  if [ -n "$NUM_BLOCKS" ]; then
    BLOCKS_NOTE="${NUM_BLOCKS}, avg tx delay $(printf '%.1f' "${AVG_DELAY:-0}")s"
  fi

  {
    echo "Fully Randomized Failure Test - Summary"
    echo "======================================"
    echo "Date      : $(date)"
    echo "Duration  : ${DURATION} minutes"
    echo "Load      : ${TOTAL_TXS} txs at ${TX_RATE} tx/s, ${TX_SIZE} bytes each, ${SENT_NOTE}"
    echo "Network   : ${NUM_PARTIES} parties, ${NUM_SHARDS} shards, ${RUNNER_NOTE}"
    echo "Kills     : ${TOTAL_KILLS} total, see summary-kills.txt for the full report"
    echo "Reconnects: ${ROUTER_RECONNECTS:-0} router, ${ASSEMBLER_RECONNECTS:-0} assembler"
    echo "Blocks    : ${BLOCKS_NOTE}"
    echo ""
    echo "Per-component kill counts:"
    for i in $(seq 1 $NUM_PARTIES); do
      echo "  Party ${i}: assembler ${KILL_COUNTS[assembler_party${i}]:-0}, consenter ${KILL_COUNTS[consenter_party${i}]:-0}, router ${KILL_COUNTS[router_party${i}]:-0}$(for j in $(seq 1 $NUM_SHARDS); do printf ", batcher-%s %s" "${j}" "${KILL_COUNTS[batcher_party${i}_shard${j}]:-0}"; done)"
    done
    echo ""
    echo "${VERDICT_LINE}"
  } > test-results/summary.txt

  # Kill report: artifact only, not printed (the workflow adds it to the job summary)
  {
    echo "Fully Randomized Failure Test - Kill Report"
    echo "=========================================="
    echo "Date      : $(date)"
    echo "Duration  : ${DURATION} minutes"
    echo "Pool      : $((NUM_PARTIES * (3 + NUM_SHARDS))) components, ${NUM_PARTIES} parties with $((3 + NUM_SHARDS)) each"
    echo ""
    for i in $(seq 1 $NUM_PARTIES); do
      echo "Party ${i}:"
      echo "  assembler  party ${i}:          ${KILL_COUNTS[assembler_party${i}]:-0} kills"
      echo "  consenter  party ${i}:          ${KILL_COUNTS[consenter_party${i}]:-0} kills"
      echo "  router     party ${i}:          ${KILL_COUNTS[router_party${i}]:-0} kills"
      for j in $(seq 1 $NUM_SHARDS); do
        echo "  batcher    party ${i} shard ${j}: ${KILL_COUNTS[batcher_party${i}_shard${j}]:-0} kills"
      done
    done
    echo ""
    echo "Total kills: ${TOTAL_KILLS}"
  } > test-results/summary-kills.txt

  # One-line reason for the Slack notification
  if [ "$VERDICT" != "passed" ]; then
    echo "${VERDICT_LINE}" > test-results/failure_reason.txt
  fi

  # Test outcome for main() to exit with
  if [ "$VERDICT" = "passed" ]; then
    echo 0 > "${TEST_DIR}/test_rc"
  else
    echo 1 > "${TEST_DIR}/test_rc"
  fi

  # Remove the working-directory logs, already gzipped under test-results/logs/
  rm -f submit.log failure_runner.log
  for i in $(seq 1 $NUM_PARTIES); do
    rm -f consenter${i}.log assembler${i}.log router${i}.log
    for j in $(seq 1 $NUM_SHARDS); do
      rm -f batcher${i}-${j}.log
    done
  done

  echo ""
  cat test-results/summary.txt
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
  rm -f submit.log failure_runner.log
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

  # Start submit (background): sends txs to every router and verifies each one in assembler 1's blocks
  echo "Starting submit (load + verify)..."
  ./bin/armageddon submit \
    --config=${TEST_DIR}/config/party1/user_config.yaml \
    --transactions=${TOTAL_TXS} \
    --rate=${TX_RATE} \
    --txSize=${TX_SIZE} \
    >> submit.log 2>&1 &
  local SUBMIT_PID=$!
  echo "Started submit (PID: ${SUBMIT_PID})"

  # Start failure runner (if enabled)
  local FAILURE_RUNNER_PID=""
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    echo "Starting fully randomized failure runner..."
    # Write marker so monitor_completion knows failure runner mode is active
    touch "${TEST_DIR}/failure_runner_enabled"
    # fd 3 = console for the runner's short event lines; its verbose output goes to the log
    exec 3>&1
    run_failure_runner "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}" >> failure_runner.log 2>&1 &
    FAILURE_RUNNER_PID=$!
    echo "Started failure runner (PID: ${FAILURE_RUNNER_PID}) — verbose output in failure_runner.log"
  fi

  # Monitor completion (with duration timeout)
  echo "Monitoring test completion..."
  monitor_completion "${NUM_PARTIES}" "${TOTAL_TXS}" "${TEST_DIR}" "${DURATION}" "${SUBMIT_PID}"

  # Wait a bit for failure runner to see the stop signal and exit gracefully
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ] && [ -n "$FAILURE_RUNNER_PID" ]; then
    echo "Stopping failure runner"
    sleep 5
    kill ${FAILURE_RUNNER_PID} 2>/dev/null || true
  fi

  # Collect results
  echo "Collecting results into test-results/ (logs gzipped)"
  collect_results "${TEST_DIR}" "${NUM_PARTIES}" "${DURATION}"

  # Disable exit-on-error for cleanup — background process exits are non-zero
  # by design (they are killed) and must not abort the script.
  set +e

  # Cleanup processes
  pkill -f "/bin/arma " 2>/dev/null
  pkill -f "armageddon" 2>/dev/null

  # Exit with the test outcome, so a lost tx turns CI red; a missing result counts as failure
  RC=$(cat "${TEST_DIR}/test_rc" 2>/dev/null) || true
  if [ -z "$RC" ]; then
    RC=1
  fi

  exit "$RC"
}

main
