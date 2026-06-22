#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Monitor test completion - wait for duration timeout OR all components finish

NUM_PARTIES=$1
TOTAL_TXS=$2
TEST_DIR=$3
DURATION_MINUTES=$4

# Calculate end time
START_TIME=$(date +%s)
END_TIME=$((START_TIME + DURATION_MINUTES * 60))

echo "=========================================="
echo "Monitoring Test Completion"
echo "=========================================="
echo "Test will run for ${DURATION_MINUTES} minutes"
echo "Expected TXs: ${TOTAL_TXS}"
echo "Start time: $(date -d @${START_TIME} '+%Y-%m-%d %H:%M:%S')"
echo "End time: $(date -d @${END_TIME} '+%Y-%m-%d %H:%M:%S')"
echo "=========================================="

# Function to check if time limit reached
time_limit_reached() {
  CURRENT_TIME=$(date +%s)
  [ $CURRENT_TIME -ge $END_TIME ]
}

# Function to get current stats
get_current_stats() {
  echo ""
  echo "=========================================="
  echo "Current Status at $(date '+%Y-%m-%d %H:%M:%S')"
  echo "=========================================="
  
  # Check loader
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    SENT=$(grep "transactions were sent" loader.log 2>/dev/null | tail -1 | awk '{print $1}')
    echo "Loader: ✅ Completed - Sent ${SENT:-unknown} txs"
  else
    SENT=$(grep -o "Sent [0-9]* transactions" loader.log 2>/dev/null | tail -1 | awk '{print $2}')
    echo "Loader: 🔄 Running - Sent ${SENT:-0} txs so far"
  fi
  
  # Check receivers
  for i in $(seq 1 $NUM_PARTIES); do
    if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      # Extract from "X transactions were successfully received"
      RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP '^\d+')
      echo "Party ${i}: ✅ Completed - Received ${RECEIVED:-unknown} txs"
    else
      # For running receivers, check CSV file for current count
      if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
        RECEIVED=$(tail -n +2 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$3} END {print sum}')
        echo "Party ${i}: 🔄 Running - Received ${RECEIVED:-0} txs so far"
      else
        echo "Party ${i}: 🔄 Running - Received 0 txs so far"
      fi
    fi
  done
  
  CURRENT_TIME=$(date +%s)
  ELAPSED=$((CURRENT_TIME - START_TIME))
  if [ $CURRENT_TIME -ge $END_TIME ]; then
     REMAINING=0
   else
     REMAINING=$((END_TIME - CURRENT_TIME))
   fi
  echo ""
  echo "Time elapsed: $((ELAPSED / 60)) minutes"
  echo "Time remaining: $((REMAINING / 60)) minutes"
  echo "=========================================="
}

# Monitor with timeout
echo "Monitoring test progress..."
LAST_STATS_TIME=$START_TIME

while true; do
  CURRENT_TIME=$(date +%s)
  
  # Check if duration reached
  if time_limit_reached; then
    echo ""
    echo "=========================================="
    echo "⏰ Duration limit reached (${DURATION_MINUTES} minutes)"
    echo "=========================================="
    get_current_stats
    break
  fi
  
  # Check if all completed early
  LOADER_DONE=false
  ALL_RECEIVERS_DONE=true
  
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    LOADER_DONE=true
  fi
  
  for i in $(seq 1 $NUM_PARTIES); do
    if ! grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      ALL_RECEIVERS_DONE=false
      break
    fi
  done
  
  if [ "$LOADER_DONE" = "true" ] && [ "$ALL_RECEIVERS_DONE" = "true" ]; then
    echo ""
    echo "=========================================="
    echo "✅ All components completed before timeout!"
    echo "=========================================="
    get_current_stats
    break
  fi
  
  # Print stats every 5 minutes
  if [ $((CURRENT_TIME - LAST_STATS_TIME)) -ge 300 ]; then
    get_current_stats
    LAST_STATS_TIME=$CURRENT_TIME
  fi
  
  sleep 30
done

# Final statistics
echo ""
echo "=========================================="
echo "Final Test Statistics"
echo "=========================================="

# Loader final stats
if grep -q "Load command finished" loader.log 2>/dev/null; then
  SENT=$(grep "transactions were sent" loader.log 2>/dev/null | tail -1 | awk '{print $1}')
  echo "Total Sent: ${SENT:-unknown} transactions"
else
  SENT=$(grep -o "Sent [0-9]* transactions" loader.log 2>/dev/null | tail -1 | awk '{print $2}')
  echo "Total Sent: ${SENT:-0} transactions (incomplete)"
fi

echo ""
echo "Received per party:"
for i in $(seq 1 $NUM_PARTIES); do
  if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
    RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | awk '{print $1}')
    echo "  Party ${i}: ${RECEIVED:-unknown} txs"
  else
    RECEIVED=$(grep -o "Received [0-9]* transactions" receiver${i}.log 2>/dev/null | tail -1 | awk '{print $2}')
    echo "  Party ${i}: ${RECEIVED:-0} txs (incomplete)"
  fi
done

echo "=========================================="

# Signal chaos runner and other processes to stop
if [ -n "$TEST_DIR" ]; then
  STOP_SIGNAL="${TEST_DIR}/chaos_stop_signal"
  touch "${STOP_SIGNAL}"
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Created stop signal: ${STOP_SIGNAL}"
fi

# Kill loader and receivers if still running
echo "Stopping loader and receivers..."
pkill -f "armageddon load" || true
pkill -f "armageddon receive" || true

echo "=========================================="
echo "✅ Monitoring completed"
echo "=========================================="
