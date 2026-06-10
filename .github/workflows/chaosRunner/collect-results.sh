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

# Determine if this is a long test (>2 hours)
IS_LONG_TEST=false
if [ "$DURATION" -gt 120 ]; then
  IS_LONG_TEST=true
  echo "Long test detected (${DURATION} minutes) - collecting summaries only"
fi

# Collect component logs
if [ "$IS_LONG_TEST" = "false" ]; then
  echo "Collecting full component logs..."
  cp consenter*.log test-results/logs/ 2>/dev/null || true
  cp batcher*.log test-results/logs/ 2>/dev/null || true
  cp assembler*.log test-results/logs/ 2>/dev/null || true
  cp router*.log test-results/logs/ 2>/dev/null || true
  cp loader.log test-results/logs/ 2>/dev/null || true
  cp receiver*.log test-results/logs/ 2>/dev/null || true
else
  echo "Extracting error logs only..."
  # For long tests, only keep errors
  grep -h -E "ERROR|WARN|PANIC|FATAL" *.log 2>/dev/null > test-results/errors.log || true
fi

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

if grep -q "Load command finished" loader.log 2>/dev/null; then
  grep "Load command finished" loader.log >> test-results/summary/summary.txt
else
  echo "❌ Loader did not complete" >> test-results/summary/summary.txt
fi

cat >> test-results/summary/summary.txt <<EOF

========================================
Receiver Results
========================================
EOF

for i in $(seq 1 $NUM_PARTIES); do
  echo "Party ${i}:" >> test-results/summary/summary.txt
  if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
    grep "were successfully received" receiver${i}.log 2>/dev/null >> test-results/summary/summary.txt || echo "  Completed (no stats)" >> test-results/summary/summary.txt
  else
    echo "  ❌ Did not complete" >> test-results/summary/summary.txt
  fi
done

# Create a simple pass/fail indicator
echo "" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt
echo "Test Status" >> test-results/summary/summary.txt
echo "========================================" >> test-results/summary/summary.txt

ALL_PASSED=true
if ! grep -q "Load command finished" loader.log 2>/dev/null; then
  ALL_PASSED=false
fi

for i in $(seq 1 $NUM_PARTIES); do
  if ! grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
    ALL_PASSED=false
  fi
done

if [ "$ALL_PASSED" = "true" ]; then
  echo "✅ PASSED - All components completed successfully" >> test-results/summary/summary.txt
else
  echo "❌ FAILED - Some components did not complete" >> test-results/summary/summary.txt
fi

echo "=========================================="
echo "✅ Results collected in test-results/"
echo "=========================================="

# Display summary
cat test-results/summary/summary.txt
