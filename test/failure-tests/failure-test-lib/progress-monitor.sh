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
Test running for ${DURATION} minutes: $(date -d "@${RUN_START_TIME}" '+%H:%M:%S') → $(date -d "@${RUN_END_TIME}" '+%H:%M:%S')
Status report: ${status_rule}
At the end: up to ${SUBMIT_DRAIN_SECONDS}s for the last txs to be confirmed
==========================================
EOF

  local now last_snapshot=$RUN_START_TIME
  while true; do
    now=$(date +%s)
    if [ "$now" -ge "$RUN_END_TIME" ]; then
      print_snapshot "Duration limit reached (${DURATION} minutes)"
      break
    fi
    if ! kill -0 "$SUBMIT_PID" 2>/dev/null; then
      print_snapshot "Submit exited before the duration limit"
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

  # Watchdog: submit waits forever for a lost tx, so stop it after the drain window
  echo "Waiting up to ${SUBMIT_DRAIN_SECONDS}s for submit to confirm the last txs..."
  local drain_start submit_rc=0
  drain_start=$(date +%s)
  ( sleep "$SUBMIT_DRAIN_SECONDS"; kill "$SUBMIT_PID" 2>/dev/null ) &
  local watchdog=$!
  wait "$SUBMIT_PID" || submit_rc=$?
  kill "$watchdog" 2>/dev/null || true

  local drain_seconds=$(( $(date +%s) - drain_start ))
  if [ "$drain_seconds" -ge "$SUBMIT_DRAIN_SECONDS" ]; then
    echo "Submit was stopped by the ${SUBMIT_DRAIN_SECONDS}s deadline (exit code ${submit_rc})"
  else
    echo "Submit exited after ${drain_seconds}s (exit code ${submit_rc})"
  fi
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
    sent=$(grep "BroadcastClientToRouter${i}.*Report" submit.log 2>/dev/null \
      | grep -oP 'sent \K[0-9]+(?= transactions in the last)' \
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
