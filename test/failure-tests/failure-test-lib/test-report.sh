# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# The result of a failure test: summary.txt, summary-kills.txt, failure_reason.txt, gzipped logs and TEST_RC.

write_test_report() {
  echo "Collecting results into ${RESULTS_DIR}/ (logs gzipped)"
  rm -rf "$RESULTS_DIR"
  mkdir -p "${RESULTS_DIR}/logs"

  # The only pass signal: a lost tx means submit never logs this line
  local verdict_line
  if grep -q "received by assembler" submit.log 2>/dev/null; then
    verdict_line="PASSED: assembler 1 confirmed all ${TOTAL_TXS} txs"
    TEST_RC=0
  else
    verdict_line="FAILED: submit logged no verification result, so a tx it sent was never confirmed, or it exited early"
    TEST_RC=1
    echo "$verdict_line" > "${RESULTS_DIR}/failure_reason.txt"
  fi

  # The `$` anchor skips the final result line, which contains the same text
  local sent_note="all sent"
  if ! grep -q "txs were sent to the routers$" submit.log 2>/dev/null; then
    sent_note="stopped while still sending"
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
  num_blocks=$(grep -oP 'num of blocks: \K[0-9]+' submit.log 2>/dev/null | tail -1) || true
  avg_delay=$(grep -oP 'avg\. tx delay: \K[0-9.]+' submit.log 2>/dev/null | tail -1) || true
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
    echo ""
    echo "$verdict_line"
  } > "${RESULTS_DIR}/summary.txt"

  cp submit.log failure_runner.log consenter*.log batcher*.log assembler*.log router*.log "${RESULTS_DIR}/logs/" 2>/dev/null || true
  gzip "${RESULTS_DIR}"/logs/*.log 2>/dev/null || true
  rm -f submit.log failure_runner.log consenter*.log batcher*.log assembler*.log router*.log

  echo ""
  cat "${RESULTS_DIR}/summary.txt"
}
