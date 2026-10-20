# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# The steps every failure test runs. Each test defines run_failure_runner (its kill order) and calls this.

run_failure_test() {
  local test_dir_name=$1
  # trap means whenever this script ends, normally or by a crash, run this line as well
  trap 'stop_failure_runner; kill "$SUBMIT_PID" 2>/dev/null || true; stop_all_nodes' EXIT
  TEST_DIR=$(mktemp -d "${TMPDIR:-/tmp}/${test_dir_name}-XXXXXX")
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
    stop_failure_runner
  fi
  write_test_report
  exit "$TEST_RC"
}
