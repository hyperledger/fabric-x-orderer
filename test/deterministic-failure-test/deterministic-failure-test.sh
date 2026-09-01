#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Deterministic failure test — single entry-point script.
#
# Previously split across five files:
#   deterministic-failure-test.sh   (main orchestration)
#   start-arma-network.sh           (network startup)
#   deterministic-failure-runner.sh (failure injection loop)
#   monitor-completion.sh           (progress monitor)
#   collect-results.sh              (result collection)
#
# All functions are now defined here and called from the main() function at the
# bottom of this file.  The GitHub Actions workflow and local usage are
# unchanged — just run this script directly.

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

# How long submit keeps pulling blocks after it finished sending, waiting for
# the last in-flight transactions to appear in every party's ledger.  This is a
# deadline, not a delay: submit exits as soon as every party has confirmed every
# transaction, so a generous value costs nothing on a healthy run.  It must be
# larger than FAILURE_RUNNER_STOP_DURATION + FAILURE_RUNNER_RESTART_WAIT, because
# the failure runner finishes the component it is on after being told to stop.
SUBMIT_DRAIN_SECONDS=${SUBMIT_DRAIN_SECONDS:-420}

# How long the failure runner waits before its first kill.  Without it the first
# component goes down in the same second submit starts, so the run never has a
# healthy baseline and the first status snapshot is meaningless.
FAILURE_RUNNER_START_DELAY=${FAILURE_RUNNER_START_DELAY:-60}

# Export variables so subprocesses (submit, arma nodes) can access them
export DURATION TX_RATE TX_SIZE NUM_PARTIES NUM_SHARDS FAILURE_RUNNER_ENABLED
export SUBMIT_DRAIN_SECONDS FAILURE_RUNNER_START_DELAY

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
#   Stops and restarts ARMA components one party at a time in a continuous
#   loop until the stop signal file is created by monitor_completion.
#
#   For each party it kills and restarts: assembler, consenter, router, then
#   all batchers in shard order.  After each full party cycle it writes a
#   signal file so monitor_completion knows to print a status snapshot.
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
  local START_DELAY=${FAILURE_RUNNER_START_DELAY:-60}

  # Get PID directory and working directory
  local PID_DIR="${TEST_DIR}/pids"
  local WORK_DIR=$(cat ${PID_DIR}/work_dir.txt 2>/dev/null || pwd)

  # Stop signal file created by monitor_completion when the test ends
  local STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"

  echo "=========================================="
  echo "Failure Runner Started"
  echo "=========================================="
  echo "Configuration:"
  echo "  Start delay: ${START_DELAY}s"
  echo "  Stop duration: ${STOP_WAIT}s"
  echo "  Restart wait: ${START_WAIT}s"
  echo "  PID directory: ${PID_DIR}"
  echo "  Working directory: ${WORK_DIR}"
  echo "  Stop signal file: ${STOP_SIGNAL}"
  echo "=========================================="

  # Short one-line events for the console.  Everything this function prints
  # normally goes to failure_runner.log (the caller redirects it there); fd 3 is
  # the console, saved by main() before the runner is started.  The verbose
  # detail — PIDs, waits, force-kills — stays in the log.
  _console() { printf '  %s  %s\n' "$(date '+%H:%M:%S')" "$*" >&3 2>/dev/null || true; }

  # Let the network reach a healthy steady state and submit connect to every
  # assembler before the first kill.  start_arma_network already confirmed
  # /healthz on every component, so this is only about giving traffic a moment to
  # flow — without it the first component goes down in the same second submit
  # starts, and the run has no healthy baseline to compare against.
  if [ "${START_DELAY}" -gt 0 ] 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Waiting ${START_DELAY}s before the first kill..."
    _console "⏳ waiting ${START_DELAY}s before the first kill"
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
    _console "🔻 ${component} party ${party}${shard:+ shard ${shard}} down (${STOP_WAIT}s)"
    sleep $STOP_WAIT

    # Restart the component from the correct working directory
    cd ${WORK_DIR}
    ${WORK_DIR}/bin/arma ${component} --config=${config_file} >> ${log_file} 2>&1 &
    local NEW_PID=$!
    echo ${NEW_PID} > ${pid_file}
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Starting ${component} (party ${party}${shard:+ shard ${shard}}) - PID ${NEW_PID}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ${component} (party ${party}${shard:+ shard ${shard}}) UP — waiting ${START_WAIT} seconds"
    _console "🔺 ${component} party ${party}${shard:+ shard ${shard}} up"
    sleep $START_WAIT
  }

  # Main failure loop — run until stop signal is received
  while true; do
    # Check if we should stop (test completed)
    if [ -f "${STOP_SIGNAL}" ]; then
      echo "=========================================="
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop signal received - failure runner exiting"
      echo "=========================================="
      break
    fi

    for party in $(seq 1 $NUM_PARTIES); do
      # Check stop signal before each party
      if [ -f "${STOP_SIGNAL}" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] Stop signal received - exiting failure loop"
        break 2
      fi

      echo "----------------------------------------------------"
      echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — starting failure sequence"
      echo "----------------------------------------------------"
      _console "🔥 party ${party} — failure sequence starting"

      # 1. Assembler
      _kill_and_restart "assembler" ${party} "" \
        "${TEST_DIR}/config/party${party}/local_config_assembler.yaml" \
        "assembler${party}.log"

      # 2. Consenter
      _kill_and_restart "consensus" ${party} "" \
        "${TEST_DIR}/config/party${party}/local_config_consenter.yaml" \
        "consenter${party}.log"

      # 3. Router
      _kill_and_restart "router" ${party} "" \
        "${TEST_DIR}/config/party${party}/local_config_router.yaml" \
        "router${party}.log"

      # 4. Batchers (in shard order)
      for shard in $(seq 1 $NUM_SHARDS); do
        _kill_and_restart "batcher" ${party} ${shard} \
          "${TEST_DIR}/config/party${party}/local_config_batcher${shard}.yaml" \
          "batcher${party}-${shard}.log"
      done

      echo "[$(date '+%Y-%m-%d %H:%M:%S')] PARTY ${party} — failure sequence DONE"
      _console "🔥 party ${party} — failure sequence done"
      # Signal monitor that this party's failure cycle is complete
      touch "${TEST_DIR}/failure_runner_party${party}_done"
    done
  done

  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Failure runner finished"
}

# ---------------------------------------------------------------------------
# monitor_completion
#   Monitors test execution until the configured duration is reached or submit
#   finishes early.  All progress numbers are read from submit.log.
#
#   - In failure runner mode: prints a status snapshot after each party's full
#     failure cycle completes.
#   - Without failure runner: prints a status snapshot every 5 minutes.
#   - Always: stops when the configured duration is reached, or as soon as
#     submit logs "Submit Finished" (it exits once every party has confirmed
#     every transaction).
#
#   Afterwards it signals the failure runner to stop and gives submit up to
#   SUBMIT_DRAIN_SECONDS to confirm the transactions still in flight.  submit
#   exits by itself once every party has confirmed everything; if it is still
#   waiting when the window closes it is sent SIGTERM, which makes it print the
#   per-party results and the verdict.  Its exit code is written to
#   ${TEST_DIR}/submit_rc for main() to propagate.
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

  echo "=========================================="
  echo "Monitoring Test Completion"
  echo "=========================================="
  echo "Test will run for ${DURATION_MINUTES} minutes"
  echo "Expected TXs: ${TOTAL_TXS}"
  echo "Start time: $(date -d @${START_TIME} '+%Y-%m-%d %H:%M:%S')"
  echo "End time: $(date -d @${END_TIME} '+%Y-%m-%d %H:%M:%S')"
  echo "=========================================="

  # Helper: check if time limit reached
  _time_limit_reached() {
    local CURRENT_TIME=$(date +%s)
    [ $CURRENT_TIME -ge $END_TIME ]
  }

  # Helper: print current stats snapshot.
  # Every number comes from submit.log, which submit writes as it runs:
  #   "submit: all N txs sent, waiting for ledger confirmation"
  #   "BroadcastClientToRouter<P> ... sent N transactions in the last 10s"
  #
  # submit reports per-party confirmation only at the end, so mid-run this shows
  # how much each router has accepted.  A router that was killed shows its count
  # go flat, which is the live signal worth having.
  #
  # $1 is an optional headline (e.g. which failure cycle just finished) folded
  # into the snapshot's own banner, so each snapshot is one block rather than two
  # stacked banners.
  _get_current_stats() {
    local HEADLINE="${1:-}"
    echo ""
    echo "=========================================="
    if [ -n "$HEADLINE" ]; then
      echo "$HEADLINE"
    fi
    echo "Current Status at $(date '+%Y-%m-%d %H:%M:%S')"
    echo "=========================================="

    if grep -q "txs sent, waiting for ledger confirmation" submit.log 2>/dev/null; then
      echo "Submit: ✅ all ${TOTAL_TXS} txs sent — waiting for ledger confirmation"
    else
      echo "Submit: 🔄 sending (target ${TOTAL_TXS} txs)"
    fi

    # Per-router accepted counts, summed from the 10-second report lines.
    for i in $(seq 1 $NUM_PARTIES); do
      local SENT_ROUTER
      SENT_ROUTER=$(grep "BroadcastClientToRouter${i}.*Report" submit.log 2>/dev/null \
        | grep -oP 'sent \K[0-9]+(?= transactions in the last)' \
        | awk '{sum+=$1} END {print sum+0}') || true
      echo "  → Router ${i}: ${SENT_ROUTER:-0} txs accepted"
    done

    # Any assembler submit has lost contact with right now.
    local LOST
    LOST=$(grep -c "cannot pull from assembler" submit.log 2>/dev/null) || LOST=0
    if [ "${LOST:-0}" -gt 0 ]; then
      grep "cannot pull from assembler" submit.log 2>/dev/null | tail -1 | sed 's/^.*-> /  /' || true
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

  # Emit a snapshot as a single write.  The failure runner prints short event
  # lines to the same console concurrently; capturing the whole snapshot first
  # means those lines can land between snapshots but never inside one.
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
      _print_stats "⏰ Duration limit reached (${DURATION_MINUTES} minutes)"
      break
    fi

    # Check if submit finished early: it exits as soon as every party has
    # confirmed every transaction it sent, and prints this sentinel on every
    # exit path (natural completion, duration cap, or signal).
    if grep -q "Submit Finished" submit.log 2>/dev/null; then
      _print_stats "✅ Submit finished before the duration limit"
      break
    fi

    if [ "$FAILURE_RUNNER_MODE" = "true" ]; then
      # In failure runner mode: print stats after each party's full failure cycle completes
      for party in $(seq 1 $NUM_PARTIES); do
        local SIGNAL_FILE="${TEST_DIR}/failure_runner_party${party}_done"
        if [ -f "$SIGNAL_FILE" ]; then
          _print_stats "🔥 Party ${party} failure cycle complete"
          rm -f "$SIGNAL_FILE"
        fi
      done
    else
      # No failure runner: print stats every 5 minutes
      if [ $((CURRENT_TIME - LAST_STATS_TIME)) -ge 300 ]; then
        _print_stats
        LAST_STATS_TIME=$CURRENT_TIME
      fi
    fi

    sleep 5
  done

  # Signal the failure runner to stop before we start waiting for submit to
  # drain, so no new components go down during the drain window.
  if [ -n "$TEST_DIR" ]; then
    local STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"
    touch "${STOP_SIGNAL}"
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Created stop signal: ${STOP_SIGNAL}"
  fi

  # Give submit the drain window to confirm the transactions that were still in
  # flight when the clock ran out.  It exits by itself as soon as every party has
  # confirmed everything; if it is still waiting when the window closes, SIGTERM
  # makes it print the per-party results and the verdict before exiting.  SIGKILL
  # is only a backstop for a wedged process — it loses the verdict.
  echo "Waiting up to ${SUBMIT_DRAIN_SECONDS}s for submit to confirm the last txs..."
  ( sleep "${SUBMIT_DRAIN_SECONDS}"
    kill -TERM "$SUBMIT_PID" 2>/dev/null || true
    sleep 60
    kill -KILL "$SUBMIT_PID" 2>/dev/null || true ) &
  local WATCHDOG=$!

  set +e
  wait "$SUBMIT_PID"
  local SUBMIT_RC=$?
  set -e
  kill "$WATCHDOG" 2>/dev/null || true

  echo "$SUBMIT_RC" > "${TEST_DIR}/submit_rc"
  echo "submit exited with code ${SUBMIT_RC}"

  echo "=========================================="
  echo "✅ Monitoring completed"
  echo "=========================================="
}

# ---------------------------------------------------------------------------
# collect_results
#   Cleans the test-results/ directory from any previous run, extracts the
#   per-party results and the verdict from submit.log, copies all component
#   logs plus submit.log into test-results/logs/ and gzips them, writes a
#   single-block summary.txt (plus failure_reason.txt when the verdict is not
#   a pass), and finally deletes the working-directory logs now that the
#   compressed copies exist.
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

  # Clean and recreate the results directory so previous run artifacts never mix in
  rm -rf test-results
  mkdir -p test-results/logs

  # -------------------------------------------------------------------------
  # Extract the verification result from submit.log, before the logs are
  # compressed and removed.  submit reports, per assembler, exactly one of:
  #   "submit: assembler N: all M txs confirmed"
  #   "submit: assembler N: X txs were never confirmed (out of M requested)"
  #   "submit: assembler N: confirmed nothing of M txs - ..."
  # followed by one overall VERIFICATION PASSED / VERIFICATION FAILED line.
  #
  # NOTE: each grep is `local X=$(...)` or `|| true` guarded.  A bare assignment
  # from a grep that matches nothing aborts the script under `set -e`, which
  # would destroy the summary on exactly the runs that matter.
  # -------------------------------------------------------------------------
  echo "Extracting the verification result from submit.log..."

  # Did submit finish sending before it was asked to report?
  local ALL_SENT=false
  if grep -q "txs sent, waiting for ledger confirmation" submit.log 2>/dev/null; then
    ALL_SENT=true
  fi

  # Was submit stopped by the drain deadline rather than finishing on its own?
  # Transactions still outstanding at that moment may simply have been in flight.
  local CUT_SHORT=false
  if grep -q "stopping on signal" submit.log 2>/dev/null; then
    CUT_SHORT=true
  fi

  # Per-assembler outcome.
  declare -a P_STATE P_MISSING
  local TOTAL_MISSING=0
  local MISSING_PARTIES=""
  # Parties that confirmed nothing at all are tracked separately: that is almost
  # always an assembler we could not reach, not the network losing every tx, and
  # the verdict must not blame the ordering service for it.
  local UNREACHABLE_PARTIES=""
  for i in $(seq 1 $NUM_PARTIES); do
    P_STATE[$i]="nodata"
    P_MISSING[$i]=0

    if grep -q "submit: assembler ${i}: all ${TOTAL_TXS} txs confirmed" submit.log 2>/dev/null; then
      P_STATE[$i]="ok"
      continue
    fi

    if grep -q "submit: assembler ${i}: confirmed nothing" submit.log 2>/dev/null; then
      P_STATE[$i]="unreachable"
      P_MISSING[$i]=${TOTAL_TXS}
      UNREACHABLE_PARTIES="${UNREACHABLE_PARTIES}${i} "
      continue
    fi

    local MISS
    MISS=$(grep -oP "submit: assembler ${i}: \K[0-9]+(?= txs were never confirmed)" submit.log 2>/dev/null | tail -1) || true
    if [ -n "$MISS" ]; then
      P_STATE[$i]="missing"
      P_MISSING[$i]=$MISS
      TOTAL_MISSING=$((TOTAL_MISSING + MISS))
      MISSING_PARTIES="${MISSING_PARTIES}party ${i}: ${MISS} missing; "
    fi
  done

  # Overall verdict as submit reported it.
  local VERDICT="none"
  if grep -q "VERIFICATION PASSED" submit.log 2>/dev/null; then
    VERDICT="passed"
  elif grep -q "VERIFICATION FAILED" submit.log 2>/dev/null; then
    VERDICT="failed"
  fi

  echo "  Verdict: ${VERDICT}, transactions never confirmed: ${TOTAL_MISSING}"

  # -------------------------------------------------------------------------
  # Collect and compress logs
  # -------------------------------------------------------------------------
  echo "Collecting and compressing logs..."
  cp consenter*.log test-results/logs/ 2>/dev/null || true
  cp batcher*.log test-results/logs/ 2>/dev/null || true
  cp assembler*.log test-results/logs/ 2>/dev/null || true
  cp router*.log test-results/logs/ 2>/dev/null || true
  cp submit.log test-results/logs/ 2>/dev/null || true
  cp failure_runner.log test-results/logs/ 2>/dev/null || true
  gzip test-results/logs/*.log 2>/dev/null || true
  echo "  All logs collected and compressed"

  # -------------------------------------------------------------------------
  # Summary report — one block, no repeated banners.  This is the file the
  # workflow prints into the GitHub job summary.
  # -------------------------------------------------------------------------
  echo "Creating summary report..."

  local VERDICT_LINE
  case "$VERDICT" in
    passed)
      VERDICT_LINE="✅ PASSED — every assembler confirmed all ${TOTAL_TXS} txs"
      ;;
    failed)
      if [ -n "$UNREACHABLE_PARTIES" ] && [ "$TOTAL_MISSING" -eq 0 ]; then
        VERDICT_LINE="⚠️  INCONCLUSIVE — assembler(s) ${UNREACHABLE_PARTIES% } confirmed nothing and were probably unreachable; every other assembler confirmed all ${TOTAL_TXS} txs"
      elif [ -n "$UNREACHABLE_PARTIES" ]; then
        VERDICT_LINE="❌ FAILED — ${TOTAL_MISSING} txs were never confirmed (${MISSING_PARTIES%; }); assembler(s) ${UNREACHABLE_PARTIES% } confirmed nothing and were probably unreachable"
      elif [ "$CUT_SHORT" = "true" ]; then
        VERDICT_LINE="⚠️  INCONCLUSIVE — submit was stopped at the drain deadline with ${TOTAL_MISSING} txs still unconfirmed (${MISSING_PARTIES%; })"
      else
        VERDICT_LINE="❌ FAILED — ${TOTAL_MISSING} txs were never confirmed (${MISSING_PARTIES%; })"
      fi
      ;;
    *)
      VERDICT_LINE="❌ FAILED — submit produced no verdict (it could not run, crashed, or was killed)"
      ;;
  esac

  {
    echo "Deterministic Failure Test — Summary"
    echo "===================================="
    echo "Date     : $(date)"
    echo "Duration : ${DURATION} minutes  |  Rate: ${TX_RATE} tx/s  |  Size: ${TX_SIZE} bytes"
    echo "Network  : ${NUM_PARTIES} parties, ${NUM_SHARDS} shards  |  Failure runner: ${FAILURE_RUNNER_ENABLED}"
    echo ""
    if [ "$ALL_SENT" = "true" ]; then
      echo "Sent     : all ${TOTAL_TXS} transactions"
    else
      echo "Sent     : fewer than ${TOTAL_TXS} transactions (submit was stopped while still sending)"
    fi
    echo ""
    for i in $(seq 1 $NUM_PARTIES); do
      case "${P_STATE[$i]}" in
        ok)          echo "Party ${i}  ✅  confirmed all ${TOTAL_TXS} txs" ;;
        missing)     echo "Party ${i}  ❌  ${P_MISSING[$i]} txs never confirmed" ;;
        unreachable) echo "Party ${i}  ⚠️   confirmed nothing — its assembler was probably unreachable" ;;
        *)           echo "Party ${i}  ❓  no result reported by submit" ;;
      esac
    done
    echo ""
    echo "${VERDICT_LINE}"
  } > test-results/summary.txt

  # Machine-readable reason for the Slack notification step.
  if [ "$VERDICT" != "passed" ]; then
    echo "${VERDICT_LINE}" > test-results/failure_reason.txt
  fi

  # -------------------------------------------------------------------------
  # Remove the working-directory logs.  They are already preserved under
  # test-results/logs/ (gzipped), so keeping the originals doubles the disk
  # used by a multi-hour run and leaves them behind in the checkout.
  # -------------------------------------------------------------------------
  echo "Removing working-directory logs (already preserved under test-results/logs/)..."
  rm -f submit.log failure_runner.log
  for i in $(seq 1 $NUM_PARTIES); do
    rm -f consenter${i}.log assembler${i}.log router${i}.log
    for j in $(seq 1 $NUM_SHARDS); do
      rm -f batcher${i}-${j}.log
    done
  done

  echo "=========================================="
  echo "✅ Results collected in test-results/"
  echo "=========================================="

  # Display the summary (the only place these numbers are printed)
  cat test-results/summary.txt
}

# ---------------------------------------------------------------------------
# main — orchestrates the full test run
# ---------------------------------------------------------------------------
main() {
  # Ensure arma/armageddon processes are cleaned up even if the script exits early
  # (e.g. due to set -e tripping on an unexpected error before the final pkill lines).
  trap 'pkill -f "arma " 2>/dev/null || true; pkill -f armageddon 2>/dev/null || true' EXIT

  # Calculate total transactions
  local TOTAL_TXS=$((DURATION * 60 * TX_RATE))

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
  local TEST_DIR=$(mktemp -d -t deterministic-failure-test-XXXXXX)
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
      # Extract component type and party from filename
      # e.g., local_config_assembler.yaml → assembler
      # e.g., local_config_batcher1.yaml → batcher1
      local filename=$(basename "$config_file")
      local component=$(echo "$filename" | sed 's/local_config_//; s/\.yaml//')
      local party=$(echo "$config_file" | grep -oP 'party\K[0-9]+')

      # Create unique data path for this component
      local component_data="${DATA_DIR}/${component}_party${party}"
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
  rm -f submit.log failure_runner.log
  for i in $(seq 1 $NUM_PARTIES); do
    rm -f consenter${i}.log assembler${i}.log router${i}.log
    for j in $(seq 1 $NUM_SHARDS); do
      rm -f batcher${i}-${j}.log
    done
  done
  echo "Log files cleaned up"

  # Start ARMA network
  echo "Starting ARMA network..."
  start_arma_network "${TEST_DIR}" "${NUM_PARTIES}" "${NUM_SHARDS}"

  # Start submit (background).  One submit replaces the loader and all N
  # receivers: it sends to every router and verifies that every assembler
  # confirmed every transaction it sent.
  #   --pullFromPartyId=0  verify against every party (party1's user config
  #                        already lists all the assembler endpoints)
  # submit runs until every party has confirmed every transaction, so this script
  # owns the deadline: it signals submit when the drain window closes and submit
  # reports whatever it has confirmed by then.
  echo "Starting submit (load + verify)..."
  ./bin/armageddon submit \
    --config=${TEST_DIR}/config/party1/user_config.yaml \
    --transactions=${TOTAL_TXS} \
    --rate=${TX_RATE} \
    --txSize=${TX_SIZE} \
    --pullFromPartyId=0 \
    >> submit.log 2>&1 &
  local SUBMIT_PID=$!
  echo "Started submit (PID: ${SUBMIT_PID})"

  # Start failure runner (if enabled)
  local FAILURE_RUNNER_PID=""
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    echo "Starting failure runner..."
    # Write marker so monitor_completion knows failure runner mode is active
    touch "${TEST_DIR}/failure_runner_enabled"
    # Save the console on fd 3, then send the runner's verbose output to a log.
    # The runner writes only short one-line events to fd 3, which keeps the
    # console readable and stops it cutting into status snapshots.
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
  collect_results "${TEST_DIR}" "${NUM_PARTIES}" "${DURATION}"

  # Cleanup processes
  echo "Cleaning up processes..."
  pkill -f "arma " || true
  pkill -f "armageddon" || true

  # Propagate submit's verdict as the script's exit code so a lost transaction
  # turns the CI job red and triggers the Slack notification:
  #   0 = every sent tx was confirmed by every assembler
  #   1 = txs were sent but never confirmed  (the bug we are hunting)
  #   3 = infrastructure failure, verification inconclusive
  local RC
  RC=$(cat "${TEST_DIR}/submit_rc" 2>/dev/null) || true
  : "${RC:=1}"

  echo "=========================================="
  case "$RC" in
    0) echo "✅ Deterministic failure test completed — all txs verified" ;;
    1) echo "❌ Deterministic failure test FAILED — txs were sent but never confirmed" ;;
    3) echo "⚠️  Deterministic failure test INCONCLUSIVE — infrastructure problem, see summary" ;;
    *) echo "❌ Deterministic failure test FAILED — submit exited unexpectedly (code ${RC})" ;;
  esac
  echo "=========================================="

  exit "$RC"
}

main
