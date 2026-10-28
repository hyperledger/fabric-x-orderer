# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# The result of a failure test: summary.txt, summary-kills.txt, failure_reason.txt, gzipped logs and TEST_RC.

write_test_report() {
  echo "Collecting results into ${RESULTS_DIR}/ (logs gzipped)"
  rm -rf "$RESULTS_DIR"
  mkdir -p "${RESULTS_DIR}/logs"

  # The `$` anchor skips the final result line, which contains the same text
  local all_sent=no sent_note="stopped while still sending" sent_time sent_epoch
  sent_time=$(grep "txs were sent to the routers$" submit.log 2>/dev/null \
    | sed -nE 's|.*([0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2})\.[0-9]+ UTC.*|\1|p' | tail -1) || true
  if [ -n "$sent_time" ]; then
    all_sent=yes
    sent_epoch=$(epoch_of "$sent_time")
    sent_note="all sent ($((sent_epoch - RUN_END_TIME))s after the end of the run)"
  fi

  # Txs sent so far, from router 1's report lines (every router gets every tx)
  local sent_count
  sent_count=$(grep "\[BroadcastClientToRouter1\].*Report" submit.log 2>/dev/null \
    | sed -nE 's|.* sent ([0-9]+) transactions in the last.*|\1|p' \
    | awk '{sum+=$1} END {print sum+0}') || sent_count=0

  # An exit code above 128 means submit was killed by a signal (130 = Ctrl-C, 143 = stop, 137 = kill -9)
  local verdict_line
  TEST_RC=1
  if [ -f "${TEST_DIR}/node_failure" ]; then
    verdict_line="FAILED: $(cat "${TEST_DIR}/node_failure")"
  elif grep -q "received by assembler" submit.log 2>/dev/null; then
    verdict_line="PASSED: all ${TOTAL_TXS} txs were sent and appeared in assembler 1's blocks"
    TEST_RC=0
  elif [ "$SUBMIT_RC" -gt 128 ] && [ "$SUBMIT_END" != gave_up ]; then
    verdict_line="FAILED: armageddon submit was stopped from outside the test (exit code ${SUBMIT_RC}), the result is unknown"
  elif [ "$SUBMIT_END" = early ]; then
    verdict_line="FAILED: armageddon submit exited with an error (exit code ${SUBMIT_RC}) after sending ${sent_count} of ${TOTAL_TXS} txs, see submit.log"
  elif [ "$SUBMIT_END" = gave_up ] && [ "$all_sent" = yes ]; then
    verdict_line="FAILED: all ${TOTAL_TXS} txs were sent, but some never appeared in assembler 1's blocks within ${SUBMIT_DRAIN_SECONDS}s after the run ended, see the node logs"
  elif [ "$SUBMIT_END" = gave_up ]; then
    verdict_line="FAILED: armageddon submit was still sending when the ${SUBMIT_DRAIN_SECONDS}s wait ran out (${sent_count} of ${TOTAL_TXS} txs sent), see submit.log"
  else
    verdict_line="FAILED: armageddon submit exited (exit code ${SUBMIT_RC}) before every tx appeared in assembler 1's blocks, see submit.log"
  fi
  if [ "$TEST_RC" = 1 ]; then
    echo "$verdict_line" > "${RESULTS_DIR}/failure_reason.txt"
  fi

  local runner_note="failure runner disabled"
  if [ "$FAILURE_RUNNER_ENABLED" = "true" ]; then
    runner_note="failure runner enabled"
  fi

  # Successful reconnects, as proof the failures happened ("as broken" lines overcount)
  local router_reconnects assembler_reconnects
  router_reconnects=$(grep -c "Reconnection to router: .* succeeded" submit.log 2>/dev/null) || router_reconnects=0
  assembler_reconnects=$(grep -c "reconnected to assembler" submit.log 2>/dev/null) || assembler_reconnects=0

  # From submit's final SUCCESS line, which it only logs on a pass
  local num_blocks avg_delay blocks_note="unknown, submit did not finish"
  num_blocks=$(sed -nE 's|.*num of blocks: ([0-9]+).*|\1|p' submit.log 2>/dev/null | tail -1) || true
  avg_delay=$(sed -nE 's|.*avg\. tx delay: ([0-9.]+)s.*|\1|p' submit.log 2>/dev/null | tail -1) || true
  if [ -n "$num_blocks" ]; then
    blocks_note="${num_blocks}, avg tx delay $(printf '%.1f' "${avg_delay:-0}")s"
  fi

  local total_kills=0 party component shard count
  {
    echo "${TEST_NAME} - Kill Report"
    echo "=========================================="
    echo "Date      : $(date)"
    echo "Duration  : ${DURATION} minutes"
    echo "Nodes     : $((NUM_PARTIES * (3 + NUM_SHARDS))) (${NUM_PARTIES} parties × $((3 + NUM_SHARDS)))"
    for party in $(seq 1 "$NUM_PARTIES"); do
      echo ""
      echo "Party ${party}:"
      for component in assembler consensus router; do
        count=$(grep -c "Stopping ${component} (party ${party})" failure_runner.log 2>/dev/null) || count=0
        total_kills=$((total_kills + count))
        echo "  ${component}: ${count} kills"
      done
      for shard in $(seq 1 "$NUM_SHARDS"); do
        count=$(grep -c "Stopping batcher (party ${party} shard ${shard})" failure_runner.log 2>/dev/null) || count=0
        total_kills=$((total_kills + count))
        echo "  batcher shard ${shard}: ${count} kills"
      done
    done
    echo ""
    echo "Total kills: ${total_kills}"
  } > "${RESULTS_DIR}/summary-kills.txt"

  {
    echo "${TEST_NAME} - Summary"
    echo "=========================================="
    echo "Date      : $(date)"
    echo "Duration  : ${DURATION} minutes"
    echo "Load      : ${TOTAL_TXS} txs at ${TX_RATE} tx/s, ${TX_SIZE} bytes each, ${sent_note}"
    echo "Network   : ${NUM_PARTIES} parties, ${NUM_SHARDS} shards, ${runner_note}"
    echo "Kills     : ${total_kills} total, see summary-kills.txt for the full report"
    echo "Reconnects: ${router_reconnects} router, ${assembler_reconnects} assembler"
    echo "Blocks    : ${blocks_note}"
    if [ -n "$EXTRA_SUMMARY" ]; then
      echo "$EXTRA_SUMMARY"
    fi
    echo ""
    echo "$verdict_line"
  } > "${RESULTS_DIR}/summary.txt"

  cp submit.log failure_runner.log consenter*.log batcher*.log assembler*.log router*.log "${RESULTS_DIR}/logs/" 2>/dev/null || true
  gzip "${RESULTS_DIR}"/logs/*.log 2>/dev/null || true
  rm -f submit.log failure_runner.log consenter*.log batcher*.log assembler*.log router*.log

  echo ""
  cat "${RESULTS_DIR}/summary.txt"
}

# Turns "2026-10-19 16:30:52" into seconds. GNU date uses -d, macOS date uses -j -f
epoch_of() {
  date -d "$1" +%s 2>/dev/null || date -j -f '%Y-%m-%d %H:%M:%S' "$1" +%s
}
