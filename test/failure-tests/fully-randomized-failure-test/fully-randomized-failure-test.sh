#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Fully randomized failure test: kills and restarts a randomly picked node, again and again,
# while armageddon submit sends txs. Passes only if assembler 1 confirms every tx.

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
source test/failure-tests/failure-test-lib/test-settings.sh
source test/failure-tests/failure-test-lib/arma-network.sh
source test/failure-tests/failure-test-lib/kill-and-restart.sh
source test/failure-tests/failure-test-lib/progress-monitor.sh
source test/failure-tests/failure-test-lib/test-report.sh
source test/failure-tests/failure-test-lib/run-failure-test.sh

TEST_NAME="Fully Randomized Failure Test"
KILLS_PER_SNAPSHOT=$((3 + NUM_SHARDS))
STATUS_RULE="every ${KILLS_PER_SNAPSHOT} kills"

RANDOM_SEED=${RANDOM_SEED:-$(date +%Y%m%d%H%M%S | tail -c 9)}
echo "Random seed: ${RANDOM_SEED} (replay with RANDOM_SEED=${RANDOM_SEED})"
EXTRA_SUMMARY="Seed      : ${RANDOM_SEED}"

run_failure_runner() {
  # The runner runs in the background, which gets a new random seed, so set it here
  RANDOM=$RANDOM_SEED
  wait_before_first_kill
  local pool=() kills=0
  for party in $(seq 1 "$NUM_PARTIES"); do
    pool+=("assembler $party" "consensus $party" "router $party")
    for shard in $(seq 1 "$NUM_SHARDS"); do
      pool+=("batcher $party $shard")
    done
  done
  runner_log "Component pool size: ${#pool[@]}, status snapshot every ${KILLS_PER_SNAPSHOT} kills"

  local index component party shard
  while [ ! -f "$STOP_SIGNAL" ]; do
    index=$((RANDOM % ${#pool[@]}))
    read -r component party shard <<< "${pool[$index]}"
    console_event "pick: ${component} party ${party}${shard:+ shard ${shard}}"
    kill_and_restart "$component" "$party" "$shard"
    kills=$((kills + 1))
    if [ $((kills % KILLS_PER_SNAPSHOT)) -eq 0 ]; then
      request_snapshot "Randomized batch of ${KILLS_PER_SNAPSHOT} kills complete (total kills so far: ${kills})"
    fi
  done
}

run_failure_test fully-randomized-failure-test
