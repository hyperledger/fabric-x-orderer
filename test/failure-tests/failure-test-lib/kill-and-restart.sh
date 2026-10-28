# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Killing and restarting one node, used by the failure runner of both tests.

console_event() {
  echo "  $(date '+%H:%M:%S')  $*" >&3 || true
}

runner_log() {
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*"
}

wait_before_first_kill() {
  runner_log "Failure runner started: stop ${STOP_DURATION}s, restart wait ${RESTART_WAIT}s, first kill in ${FAILURE_RUNNER_START_DELAY}s"
  console_event "waiting ${FAILURE_RUNNER_START_DELAY}s before the first kill"
  sleep "$FAILURE_RUNNER_START_DELAY"
}

kill_and_restart() {
  local component=$1 party=$2 shard=$3
  local name="${component} (party ${party}${shard:+ shard ${shard}})"
  if [ -f "$STOP_SIGNAL" ]; then
    return
  fi
  if ! check_all_nodes_alive; then
    return
  fi

  set_node_files "$component" "$party" "$shard"
  local pid attempt
  pid=$(cat "$NODE_PID_FILE")
  kill "$pid" 2>/dev/null || true
  runner_log "Stopping ${name} - was PID ${pid}"
  for attempt in $(seq 1 10); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  if kill -0 "$pid" 2>/dev/null; then
    runner_log "Force killing ${name}"
    kill -9 "$pid" 2>/dev/null || true
  fi

  runner_log "${name} DOWN, waiting ${STOP_DURATION} seconds"
  console_event "${component} party ${party}${shard:+ shard ${shard}} down for ${STOP_DURATION}s"
  sleep "$STOP_DURATION"

  local old_log_size
  old_log_size=$(wc -c < "$NODE_LOG" 2>/dev/null | tr -d ' ') || old_log_size=0
  launch_node "$component" "$party" "$shard"
  if ! wait_for_healthz "$NODE_LOG" "$name" "$old_log_size"; then
    report_node_failure "${name} was restarted by the test but did not start again (${HEALTH_ERROR})"
    return
  fi
  runner_log "${name} UP and healthy (PID ${NODE_PID}), waiting ${RESTART_WAIT} seconds"
  console_event "${component} party ${party}${shard:+ shard ${shard}} back up (healthy)"
  sleep "$RESTART_WAIT"
}

# A node that died on its own stops the run; its panic line, if any, says why
check_all_nodes_alive() {
  local pid_file node log_file panic_line
  for pid_file in "${TEST_DIR}"/pids/*.pid; do
    if ! kill -0 "$(cat "$pid_file")" 2>/dev/null; then
      node=$(basename "$pid_file" .pid)
      # The pid file of a consenter is named consensusN, but its log is consenterN.log
      log_file=$(echo "${node}.log" | sed 's/^consensus/consenter/')
      panic_line=$(grep "^panic:" "$log_file" 2>/dev/null | tail -1) || true
      report_node_failure "${node} stopped running on its own during the test${panic_line:+: ${panic_line}}"
      return 1
    fi
  done
}

# Tells the monitor to end the run: a node that can't come back means something is broken
report_node_failure() {
  runner_log "NODE FAILURE: $1"
  console_event "NODE FAILURE: $1"
  echo "$1" > "${TEST_DIR}/node_failure"
  touch "$STOP_SIGNAL"
}

request_snapshot() {
  echo "$1" > "${TEST_DIR}/snapshot_request"
}

# Also stops the command the runner is busy with (usually a sleep), so nothing is left running
stop_failure_runner() {
  pkill -P "$FAILURE_RUNNER_PID" 2>/dev/null || true
  kill "$FAILURE_RUNNER_PID" 2>/dev/null || true
}
