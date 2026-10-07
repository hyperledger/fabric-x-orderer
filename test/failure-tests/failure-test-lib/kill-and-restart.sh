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

  set_node_files "$component" "$party" "$shard"
  local pid i
  pid=$(cat "$NODE_PID_FILE" 2>/dev/null) || true
  if [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null; then
    kill "$pid" 2>/dev/null || true
    runner_log "Stopping ${name} - was PID ${pid}"
    for i in $(seq 1 10); do
      if ! kill -0 "$pid" 2>/dev/null; then
        break
      fi
      sleep 1
    done
    if kill -0 "$pid" 2>/dev/null; then
      runner_log "Force killing ${name}"
      kill -9 "$pid" 2>/dev/null || true
    fi
  else
    runner_log "WARNING: ${name} not running (PID: ${pid:-unknown}), restarting it anyway"
    console_event "${component} party ${party}${shard:+ shard ${shard}} was already down (crashed?), restarting it"
  fi

  runner_log "${name} DOWN, waiting ${STOP_DURATION} seconds"
  console_event "${component} party ${party}${shard:+ shard ${shard}} down for ${STOP_DURATION}s"
  sleep "$STOP_DURATION"

  launch_node "$component" "$party" "$shard"
  runner_log "${name} UP (PID ${NODE_PID}), waiting ${RESTART_WAIT} seconds"
  console_event "${component} party ${party}${shard:+ shard ${shard}} back up"
  sleep "$RESTART_WAIT"
}

request_snapshot() {
  echo "$1" > "${TEST_DIR}/snapshot_request"
}
