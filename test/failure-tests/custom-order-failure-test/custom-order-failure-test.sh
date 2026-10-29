#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Custom order failure test: kills and restarts nodes in the order given by KILL_ORDER,
# while armageddon submit sends txs. Passes only if assembler 1 confirms every tx.

set -e
cd "$(dirname "${BASH_SOURCE[0]}")/../../.."
source test/failure-tests/failure-test-lib/test-settings.sh
source test/failure-tests/failure-test-lib/arma-network.sh
source test/failure-tests/failure-test-lib/kill-and-restart.sh
source test/failure-tests/failure-test-lib/progress-monitor.sh
source test/failure-tests/failure-test-lib/test-report.sh
source test/failure-tests/failure-test-lib/run-failure-test.sh

TEST_NAME="Custom Order Failure Test"
STATUS_RULE="after each round of the kill order"

# Reproduces the assembler crash of #1353 on party 3 (see README)
KILL_ORDER=${KILL_ORDER:-C -> A -> R -> C -> C -> B1 -> B2}
EXTRA_SUMMARY="Order     : ${KILL_ORDER}"

# Turns KILL_ORDER into KILLS, one "component party shard" entry per kill; a bad order stops the test
parse_kill_order() {
  local items=() item party name with_party=0 without_party=0
  read -r -a items <<< "$(echo "$KILL_ORDER" | sed 's/->/ /g; s/,/ /g')"
  if [ "${#items[@]}" -eq 0 ]; then
    startup_failed "KILL_ORDER is empty"
  fi
  for item in "${items[@]}"; do
    if [[ "$item" == p*:* ]]; then
      with_party=$((with_party + 1))
    else
      without_party=$((without_party + 1))
    fi
  done
  if [ "$with_party" -gt 0 ] && [ "$without_party" -gt 0 ]; then
    startup_failed "KILL_ORDER mixes items with a party (p1:C) and without one (C); use one form only"
  fi

  KILLS=()
  if [ "$with_party" -gt 0 ]; then
    for item in "${items[@]}"; do
      party=${item%%:*}
      party=${party#p}
      add_kill "${item#*:}" "$party"
    done
  else
    for party in $(seq 1 "$NUM_PARTIES"); do
      for name in "${items[@]}"; do
        add_kill "$name" "$party"
      done
    done
  fi
}

# add_kill <C|A|R|B<shard>> <party>: checks one kill and appends it to KILLS
add_kill() {
  local name=$1 party=$2
  if ! [[ "$party" =~ ^[0-9]+$ ]] || [ "$party" -lt 1 ] || [ "$party" -gt "$NUM_PARTIES" ]; then
    startup_failed "KILL_ORDER: party '${party}' must be between 1 and NUM_PARTIES (${NUM_PARTIES})"
  fi
  case "$name" in
    C) KILLS+=("consensus $party") ;;
    A) KILLS+=("assembler $party") ;;
    R) KILLS+=("router $party") ;;
    B[0-9]*)
      local shard=${name#B}
      if ! [[ "$shard" =~ ^[0-9]+$ ]] || [ "$shard" -lt 1 ] || [ "$shard" -gt "$NUM_SHARDS" ]; then
        startup_failed "KILL_ORDER: '${name}' must name a shard between 1 and NUM_SHARDS (${NUM_SHARDS})"
      fi
      KILLS+=("batcher $party $shard")
      ;;
    *)
      startup_failed "KILL_ORDER: unknown item '${name}', use C, A, R or B<shard>"
      ;;
  esac
}

run_failure_runner() {
  wait_before_first_kill
  runner_log "Kill order: ${KILL_ORDER} (${#KILLS[@]} kills per round)"
  local round=0 kill component party shard
  while true; do
    round=$((round + 1))
    for kill in "${KILLS[@]}"; do
      if [ -f "$STOP_SIGNAL" ]; then
        return
      fi
      read -r component party shard <<< "$kill"
      kill_and_restart "$component" "$party" "$shard"
    done
    if [ -f "$STOP_SIGNAL" ]; then
      return
    fi
    request_snapshot "Round ${round} of the kill order complete"
  done
}

parse_kill_order
echo "Kill order: ${KILL_ORDER} (${#KILLS[@]} kills per round)"
run_failure_test custom-order-failure-test
