# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Watching a running failure test: status snapshots, then waiting for submit to confirm the last txs.

watch_until_done() {
  RUN_START_TIME=$(date +%s)
  RUN_END_TIME=$((RUN_START_TIME + DURATION * 60))
  local status_rule="$STATUS_RULE"
  if [ "$FAILURE_RUNNER_ENABLED" != "true" ]; then
    status_rule="every 5 minutes"
  fi
  cat <<EOF
==========================================
Test running for ${DURATION} minutes: $(date '+%H:%M:%S') → $(clock_time "$RUN_END_TIME")
Status report: ${status_rule}
At the end: up to ${SUBMIT_DRAIN_SECONDS}s for the last txs to be confirmed
==========================================
EOF

  SUBMIT_END=""
  local now last_snapshot=$RUN_START_TIME
  while true; do
    now=$(date +%s)
    if [ "$now" -ge "$RUN_END_TIME" ]; then
      print_snapshot "Duration limit reached (${DURATION} minutes)"
      break
    fi
    if [ -f "${TEST_DIR}/node_failure" ]; then
      print_snapshot "Stopping early: a node failed (see the reason below)"
      break
    fi
    if ! kill -0 "$SUBMIT_PID" 2>/dev/null; then
      print_snapshot "Submit exited before the duration limit"
      SUBMIT_END=early
      break
    fi
    if [ -f "${TEST_DIR}/snapshot_request" ]; then
      print_snapshot "$(cat "${TEST_DIR}/snapshot_request")"
      rm -f "${TEST_DIR}/snapshot_request"
    elif [ "$FAILURE_RUNNER_ENABLED" != "true" ] && [ $((now - last_snapshot)) -ge 300 ]; then
      print_snapshot
      last_snapshot=$now
    fi
    sleep 5
  done

  touch "$STOP_SIGNAL"
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    echo "The failure runner starts no new kills from now on"
  fi

  local drain_start waited
  drain_start=$(date +%s)
  waited=0
  if [ -f "${TEST_DIR}/node_failure" ]; then
    echo "Not waiting for submit: a node failed, so the result is already known"
  else
    # Submit waits forever for a lost tx, so give up after SUBMIT_DRAIN_SECONDS
    echo "Waiting up to ${SUBMIT_DRAIN_SECONDS}s for submit to confirm the last txs..."
    while kill -0 "$SUBMIT_PID" 2>/dev/null && [ "$waited" -lt "$SUBMIT_DRAIN_SECONDS" ]; do
      sleep 1
      waited=$(( $(date +%s) - drain_start ))
    done
  fi
  if kill -0 "$SUBMIT_PID" 2>/dev/null; then
    kill "$SUBMIT_PID" 2>/dev/null || true
    SUBMIT_END=gave_up
  elif [ -z "$SUBMIT_END" ]; then
    SUBMIT_END=exited
  fi
  SUBMIT_RC=0
  wait "$SUBMIT_PID" || SUBMIT_RC=$?

  if [ -f "${TEST_DIR}/node_failure" ]; then
    echo "Submit stopped (exit code ${SUBMIT_RC})"
  elif [ "$SUBMIT_END" = gave_up ]; then
    echo "Gave up after ${SUBMIT_DRAIN_SECONDS}s: some txs were still not confirmed, submit stopped"
  else
    echo "Submit exited after ${waited}s (exit code ${SUBMIT_RC})"
  fi
}

# Prints an epoch time (seconds) as HH:MM:SS. GNU date uses -d @, macOS date uses -r
clock_time() {
  date -d "@$1" '+%H:%M:%S' 2>/dev/null || date -r "$1" '+%H:%M:%S'
}

# Built first and printed in one write, so failure runner lines never land inside it
print_snapshot() {
  local snapshot
  snapshot=$(snapshot_text "${1:-}")
  echo "$snapshot"
}

snapshot_text() {
  echo ""
  echo "=========================================="
  if [ -n "$1" ]; then
    echo "$1"
  fi
  echo "Current Status at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=========================================="

  if grep -q "txs were sent to the routers$" submit.log 2>/dev/null; then
    echo "Submit: all ${TOTAL_TXS} txs sent, waiting for the last blocks"
  else
    echo "Submit: sending (target ${TOTAL_TXS} txs)"
  fi

  local i sent lost reconnected
  for i in $(seq 1 "$NUM_PARTIES"); do
    sent=$(grep "\[BroadcastClientToRouter${i}\].*Report" submit.log 2>/dev/null \
      | sed -nE 's|.* sent ([0-9]+) transactions in the last.*|\1|p' \
      | awk '{sum+=$1} END {print sum+0}') || true
    echo "  → Router ${i}: ${sent:-0} txs sent"
  done

  lost=$(grep -c "lost connection to assembler" submit.log 2>/dev/null) || lost=0
  reconnected=$(grep -c "reconnected to assembler" submit.log 2>/dev/null) || reconnected=0
  if [ "$lost" -gt "$reconnected" ]; then
    echo "  assembler 1 connection: DOWN now (lost ${lost}, reconnected ${reconnected})"
    echo "  last error: $(grep "lost connection to assembler" submit.log | tail -1 | sed 's/^.*-> //')"
  else
    echo "  assembler 1 connection: lost ${lost}, reconnected ${reconnected}"
  fi

  local now remaining
  now=$(date +%s)
  remaining=$((RUN_END_TIME - now))
  if [ "$remaining" -lt 0 ]; then
    remaining=0
  fi
  echo ""
  echo "Time elapsed: $(( (now - RUN_START_TIME) / 60 )) minutes"
  echo "Time remaining: $((remaining / 60)) minutes"
  echo "=========================================="
}
