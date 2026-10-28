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
source test/failure-tests/failure-test-lib/run-failure-test.sh

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
      if [ -f "$STOP_SIGNAL" ]; then
        return
      fi
      console_event "party ${party} failure sequence done"
      request_snapshot "Party ${party} failure cycle complete"
    done
  done
}

run_failure_test deterministic-failure-test
