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

# Create results directories
mkdir -p test-results/logs
mkdir -p test-results/statistics
mkdir -p test-results/summary

# Determine log collection strategy based on test duration
if [ "$DURATION" -le 10 ]; then
  LOG_MODE="full"
  echo "Short test (${DURATION} min) - collecting all logs"
elif [ "$DURATION" -le 60 ]; then
  LOG_MODE="standard"
  echo "Medium test (${DURATION} min) - collecting compressed logs"
elif [ "$DURATION" -le 240 ]; then
  LOG_MODE="errors_only"
  echo "Long test (${DURATION} min) - collecting errors and summaries only"
else
  LOG_MODE="minimal"
  echo "Extended test (${DURATION} min) - collecting minimal artifacts"
fi

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

# Collect component logs based on strategy
case "$LOG_MODE" in
  full)
    echo "Collecting all logs (uncompressed)..."
    cp consenter*.log test-results/logs/ 2>/dev/null || true
    cp batcher*.log test-results/logs/ 2>/dev/null || true
    cp assembler*.log test-results/logs/ 2>/dev/null || true
    cp router*.log test-results/logs/ 2>/dev/null || true
    cp loader.log test-results/logs/ 2>/dev/null || true
    cp receiver*.log test-results/logs/ 2>/dev/null || true
    echo "  Collected all logs without compression"
    ;;
    
  standard)
    echo "Collecting and compressing logs..."
    cp consenter*.log test-results/logs/ 2>/dev/null || true
    cp batcher*.log test-results/logs/ 2>/dev/null || true
    cp assembler*.log test-results/logs/ 2>/dev/null || true
    cp router*.log test-results/logs/ 2>/dev/null || true
    cp loader.log test-results/logs/ 2>/dev/null || true
    cp receiver*.log test-results/logs/ 2>/dev/null || true
    
    # Compress all logs
    echo "  Compressing logs..."
    gzip test-results/logs/*.log 2>/dev/null || true
    echo "  Logs compressed with gzip"
    ;;
    
  errors_only)
    echo "Extracting errors and keeping essential logs..."
    # Extract all errors/warnings to a single file (kept uncompressed for artifact upload)
    grep -h -E "ERROR|WARN|PANIC|FATAL" *.log 2>/dev/null > test-results/logs/errors.log || true
    
    # Keep loader and receiver logs (they contain final statistics)
    cp loader.log test-results/logs/ 2>/dev/null || true
    cp receiver*.log test-results/logs/ 2>/dev/null || true
    
    # Compress loader/receiver logs, but keep errors.log uncompressed (workflow uploads errors.log)
    gzip test-results/logs/loader.log test-results/logs/receiver*.log 2>/dev/null || true
    echo "  Collected errors and essential logs (compressed)"
    ;;
    
  minimal)
    echo "Collecting minimal artifacts..."
    # Only critical errors (kept uncompressed for artifact upload)
    grep -h -E "ERROR|PANIC|FATAL" *.log 2>/dev/null > test-results/logs/errors.log || true
    
    # First and last 100 lines of loader (shows start and completion)
    head -n 100 loader.log 2>/dev/null > test-results/logs/loader_start.log || true
    tail -n 100 loader.log 2>/dev/null > test-results/logs/loader_end.log || true
    
    # Last 50 lines of each receiver (shows completion status)
    for i in $(seq 1 $NUM_PARTIES); do
      if [ -f "receiver${i}.log" ]; then
        tail -n 50 "receiver${i}.log" 2>/dev/null > "test-results/logs/receiver${i}_end.log" || true
      fi
    done
    
     # Compress everything except errors.log (workflow uploads errors.log as plain text)
    for f in test-results/logs/*.log; do
      [ -e "$f" ] || continue
      [ "$(basename "$f")" = "errors.log" ] && continue
      gzip "$f" 2>/dev/null || true
    done
    echo "  Collected minimal artifacts (compressed)"
    ;;
esac

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

cat >> test-results/summary/summary.txt <<EOF

========================================
Overall Statistics
========================================
Total Sent: ${SENT:-0} transactions
Total Received (all parties): ${TOTAL_RECEIVED} transactions
EOF

# Calculate success rate if we have numbers
if [ -n "$SENT" ] && [ "$SENT" -gt 0 ]; then
  SUCCESS_RATE=$((TOTAL_RECEIVED * 100 / SENT))
  echo "Success Rate: ${SUCCESS_RATE}%" >> test-results/summary/summary.txt
fi

# Create a simple pass/fail indicator
echo "" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt
echo "Test Status" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt

# For duration-based tests, we consider it successful if it ran for the full duration
# and processed transactions (even if not all completed)
if [ -n "$SENT" ] && [ "$SENT" -gt 0 ] && [ "$TOTAL_RECEIVED" -gt 0 ]; then
  echo "✅ PASSED - Test ran for ${DURATION} minutes" >> test-results/summary/summary.txt
  echo "   Sent: ${SENT} txs, Received: ${TOTAL_RECEIVED} txs" >> test-results/summary/summary.txt
else
  echo "❌ FAILED - No transactions processed" >> test-results/summary/summary.txt
fi

echo "=========================================="
echo "✅ Results collected in test-results/"
echo "=========================================="

# Display summary
cat test-results/summary/summary.txt
