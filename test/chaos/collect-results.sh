#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Collect test results and create summary

TEST_DIR=$1
NUM_PARTIES=$2
DURATION=$3

echo "=========================================="
echo "Collecting Results"
echo "=========================================="

# Clean and recreate results directories so previous run artifacts never mix in
rm -rf test-results
mkdir -p test-results/logs
mkdir -p test-results/statistics
mkdir -p test-results/summary

# Extract statistics BEFORE compressing/moving logs
echo "Extracting statistics from logs..."

# Extract loader statistics
if grep -q "Load command finished" loader.log 2>/dev/null; then
  SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
  LOADER_STATUS="completed"
else
  SENT=$(grep -o "Sent [0-9]* transactions" loader.log 2>/dev/null | tail -1 | awk '{print $2}')
  LOADER_STATUS="timeout"
fi

# Extract receiver statistics
declare -a RECEIVER_STATS
declare -a RECEIVER_STATUS
for i in $(seq 1 $NUM_PARTIES); do
  if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
    # Extract from "1800000 txs were expected and overall 1800186 were successfully received" example from the log
    RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K\d+(?= were successfully received)')
    RECEIVER_STATS[$i]="${RECEIVED:-unknown}"
    RECEIVER_STATUS[$i]="completed"
  else
    # For stopped receivers, check the statistics CSV file
    if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
      BLOCKS=$(tail -n +2 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | wc -l)
      RECEIVED=$(tail -n +2 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$3} END {print sum}')
      RECEIVER_STATS[$i]="${RECEIVED:-0}"
      RECEIVER_STATUS[$i]="timeout"
    else
      RECEIVER_STATS[$i]="0"
      RECEIVER_STATUS[$i]="no_data"
    fi
  fi
done

echo "  Loader: ${SENT:-0} txs (${LOADER_STATUS})"
for i in $(seq 1 $NUM_PARTIES); do
  echo "  Party ${i}: ${RECEIVER_STATS[$i]} txs (${RECEIVER_STATUS[$i]})"
done

# Collect all logs — always, regardless of duration — and gzip them
echo "Collecting and compressing logs..."
cp consenter*.log test-results/logs/ 2>/dev/null || true
cp batcher*.log test-results/logs/ 2>/dev/null || true
cp assembler*.log test-results/logs/ 2>/dev/null || true
cp router*.log test-results/logs/ 2>/dev/null || true
cp loader.log test-results/logs/ 2>/dev/null || true
cp receiver*.log test-results/logs/ 2>/dev/null || true
gzip test-results/logs/*.log 2>/dev/null || true
echo "  All logs collected and compressed"

# Collect statistics from receivers
echo "Collecting statistics..."
for i in $(seq 1 $NUM_PARTIES); do
  if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
    cp "${TEST_DIR}/output${i}/statistics.csv" test-results/statistics/party${i}_statistics.csv
    echo "  Collected statistics for party ${i}"
  fi
done

# Create summary report
echo "Creating summary report..."
cat > test-results/summary/summary.txt <<EOF
========================================
Chaos Test Summary
========================================
Date: $(date)
Duration: ${DURATION} minutes
TX Rate: ${TX_RATE} tx/s
TX Size: ${TX_SIZE} bytes
Total TXs Expected: $((DURATION * 60 * TX_RATE))
Parties: ${NUM_PARTIES}
Shards: ${NUM_SHARDS}
Chaos Enabled: ${CHAOS_ENABLED}

========================================
Loader Results
========================================
EOF

# Use pre-extracted statistics
if [ "$LOADER_STATUS" = "completed" ]; then
  echo "✅ Loader completed" >> test-results/summary/summary.txt
  echo "Sent: ${SENT:-unknown} transactions" >> test-results/summary/summary.txt
else
  echo "⏰ Loader stopped by timeout" >> test-results/summary/summary.txt
  echo "Sent: ${SENT:-0} transactions (incomplete)" >> test-results/summary/summary.txt
fi

cat >> test-results/summary/summary.txt <<EOF

========================================
Receiver Results
========================================
EOF

# Use pre-extracted receiver statistics
TOTAL_RECEIVED=0
for i in $(seq 1 $NUM_PARTIES); do
  echo "Party ${i}:" >> test-results/summary/summary.txt
  
  RECEIVED="${RECEIVER_STATS[$i]}"
  STATUS="${RECEIVER_STATUS[$i]}"
  
  if [ "$STATUS" = "completed" ]; then
    echo "  ✅ Completed - Received: ${RECEIVED} txs" >> test-results/summary/summary.txt
    if [ -n "$RECEIVED" ] && [ "$RECEIVED" != "unknown" ] && [ "$RECEIVED" -gt 0 ] 2>/dev/null; then
      TOTAL_RECEIVED=$((TOTAL_RECEIVED + RECEIVED))
    fi
  elif [ "$STATUS" = "timeout" ]; then
    # Get block count from CSV
    if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
      BLOCKS=$(tail -n +2 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | wc -l)
      echo "  ⏰ Stopped by timeout - Received: ${RECEIVED} txs in ${BLOCKS} blocks" >> test-results/summary/summary.txt
    else
      echo "  ⏰ Stopped by timeout - Received: ${RECEIVED} txs" >> test-results/summary/summary.txt
    fi
    if [ -n "$RECEIVED" ] && [ "$RECEIVED" -gt 0 ] 2>/dev/null; then
      TOTAL_RECEIVED=$((TOTAL_RECEIVED + RECEIVED))
    fi
  else
    echo "  ❌ No statistics available" >> test-results/summary/summary.txt
  fi
done

# Overall statistics: each party independently receives all sent txs, so the
# meaningful metric is per-party success rate, not a cross-party sum.
PARTY_SUCCESS_RATE=0
if [ -n "$SENT" ] && [ "$SENT" -gt 0 ]; then
  # Use the first completed/timeout party as representative received count
  REPRESENTATIVE_RECEIVED=${RECEIVER_STATS[1]:-0}
  if [ -n "$REPRESENTATIVE_RECEIVED" ] && [ "$REPRESENTATIVE_RECEIVED" -gt 0 ] 2>/dev/null; then
    PARTY_SUCCESS_RATE=$((REPRESENTATIVE_RECEIVED * 100 / SENT))
  fi
fi

cat >> test-results/summary/summary.txt <<EOF

========================================
Overall Statistics
========================================
Sent: ${SENT:-0} transactions
Expected per party: ${SENT:-0} transactions
Received per party: ${REPRESENTATIVE_RECEIVED:-0} transactions
Success Rate: ${PARTY_SUCCESS_RATE}%
EOF

# Create a simple pass/fail indicator
echo "" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt
echo "Test Status" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt

if [ -n "$SENT" ] && [ "$SENT" -gt 0 ] && [ "${REPRESENTATIVE_RECEIVED:-0}" -gt 0 ] 2>/dev/null; then
  echo "✅ PASSED - Test ran for ${DURATION} minutes" >> test-results/summary/summary.txt
  echo "   Sent: ${SENT} txs, each party received: ${REPRESENTATIVE_RECEIVED} txs (${PARTY_SUCCESS_RATE}%)" >> test-results/summary/summary.txt
else
  echo "❌ FAILED - No transactions processed" >> test-results/summary/summary.txt
fi

echo "=========================================="
echo "✅ Results collected in test-results/"
echo "=========================================="

# Display summary
cat test-results/summary/summary.txt
