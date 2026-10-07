#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Deterministic failure test: kills and restarts every node, one party at a time in a fixed
# order, while armageddon submit sends txs. Passes only if assembler 1 confirms every tx.

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
source test/failure-tests/failure-test-lib/test-settings.sh
source test/failure-tests/failure-test-lib/arma-network.sh
source test/failure-tests/failure-test-lib/kill-and-restart.sh
source test/failure-tests/failure-test-lib/progress-monitor.sh
source test/failure-tests/failure-test-lib/test-report.sh

TEST_NAME="Deterministic Failure Test"
STATUS_RULE="after each party's failure cycle"

run_failure_runner() {
  wait_before_first_kill
  while true; do
    for party in $(seq 1 "$NUM_PARTIES"); do
      if [ -f "$STOP_SIGNAL" ]; then
        return
      fi
      console_event "party ${party} failure sequence starting"
      kill_and_restart assembler "$party"
      kill_and_restart consensus "$party"
      kill_and_restart router "$party"
      for shard in $(seq 1 "$NUM_SHARDS"); do
        kill_and_restart batcher "$party" "$shard"
      done
      console_event "party ${party} failure sequence done"
      request_snapshot "Party ${party} failure cycle complete"
    done
  done
}

main() {
  # trap means whenever this script ends, normally or by a crash, run this line as well
  trap 'kill "$FAILURE_RUNNER_PID" "$SUBMIT_PID" 2>/dev/null || true; pkill -f "/bin/arma " || true' EXIT
  TEST_DIR=$(mktemp -d -t deterministic-failure-test-XXXXXX)
  STOP_SIGNAL="${TEST_DIR}/failure_runner_stop_signal"

  print_test_configuration
  remove_leftovers_of_previous_run
  generate_network_config
  start_arma_network

  ./bin/armageddon submit \
    --config="${TEST_DIR}/config/party1/user_config.yaml" \
    --transactions="$TOTAL_TXS" --rate="$TX_RATE" --txSize="$TX_SIZE" \
    >> submit.log 2>&1 &
  SUBMIT_PID=$!
  echo "Started submit (PID ${SUBMIT_PID}), log: submit.log"

  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    exec 3>&1   # fd 3 = the screen, for the runner's short event lines
    run_failure_runner >> failure_runner.log 2>&1 &
    FAILURE_RUNNER_PID=$!
    echo "Started failure runner (PID ${FAILURE_RUNNER_PID}), details in failure_runner.log"
  fi

  watch_until_done
  if [ -n "$FAILURE_RUNNER_PID" ]; then
    echo "Stopping failure runner"
    kill "$FAILURE_RUNNER_PID" 2>/dev/null || true
  fi
  write_test_report
  exit "$TEST_RC"
}

main
