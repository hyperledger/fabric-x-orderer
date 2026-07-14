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
  # Actual log line: "Load command finished, sent N TXs in ..."
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
    echo "Loader: ✅ Completed - Sent ${SENT:-unknown} txs total"
  else
    # Sum all per-10s Report lines per router to get exact cumulative sent count per router.
    # Actual log line: "BroadcastClient to Router 127.0.0.1:XXXX sent N transactions in the last 10s"
    echo "Loader: 🔄 Running"
    for i in $(seq 1 $NUM_PARTIES); do
      SENT_ROUTER=$(grep "BroadcastClientToRouter${i}.*Report" loader.log 2>/dev/null \
        | grep -oP 'sent \K[0-9]+(?= transactions in the last)' \
        | awk '{sum+=$1} END {print sum}')
      echo "  → Router ${i}: ${SENT_ROUTER:-0} txs sent so far"
    done
  fi
  
  # Check receivers
  for i in $(seq 1 $NUM_PARTIES); do
    if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      # Actual log line: "N txs were expected and overall N were successfully received"
      RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K[0-9]+')
      echo "Party ${i}: ✅ Completed - Received ${RECEIVED:-unknown} txs"
    else
      # For running receivers, read cumulative txs from CSV (column 2 = num txs, column 3 = num blocks)
      if [ -f "${TEST_DIR}/output${i}/statistics.csv" ]; then
        RECEIVED=$(tail -n +3 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$2} END {print sum}')
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

# Determine if chaos mode is active by checking for the chaos stop signal path
# (chaos-runner.sh is only started when CHAOS_ENABLED=true, and it creates party done files)
CHAOS_MODE=false
# We detect chaos mode by checking if the chaos runner is running (its stop signal file
# path exists means chaos was enabled - we use a marker written by chaos-test.sh)
if [ -f "${TEST_DIR}/chaos_enabled" ]; then
  CHAOS_MODE=true
fi

# Monitor with timeout
echo "Monitoring test progress..."
LAST_STATS_TIME=$START_TIME
# Track which party cycles we have already printed stats for
LAST_CHAOS_PARTY=0

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

  if [ "$CHAOS_MODE" = "true" ]; then
    # In chaos mode: print stats after each party's full chaos cycle completes
    for party in $(seq 1 $NUM_PARTIES); do
      SIGNAL_FILE="${TEST_DIR}/chaos_party${party}_done"
      if [ -f "$SIGNAL_FILE" ]; then
        echo ""
        echo "=========================================="
        echo "🔥 Party ${party} chaos cycle complete"
        echo "=========================================="
        get_current_stats
        rm -f "$SIGNAL_FILE"
      fi
    done
  else
    # No chaos: print stats every 5 minutes
    if [ $((CURRENT_TIME - LAST_STATS_TIME)) -ge 300 ]; then
      get_current_stats
      LAST_STATS_TIME=$CURRENT_TIME
    fi
  fi

  sleep 5
done

# Final statistics
echo ""
echo "=========================================="
echo "Final Test Statistics"
echo "=========================================="

# Loader final stats
if grep -q "Load command finished" loader.log 2>/dev/null; then
  SENT=$(grep "Load command finished" loader.log 2>/dev/null | tail -1 | grep -oP 'sent \K[0-9]+')
  echo "Total Sent: ${SENT:-unknown} transactions"
else
  SENT=$(grep -oP 'sent \K[0-9]+(?= transactions in the last)' loader.log 2>/dev/null | tail -1)
  echo "Total Sent: ${SENT:-0} transactions (incomplete)"
fi

echo ""
echo "Received per party:"
for i in $(seq 1 $NUM_PARTIES); do
  if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
    RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | grep -oP 'overall \K[0-9]+')
    echo "  Party ${i}: ${RECEIVED:-unknown} txs"
  else
    RECEIVED=$(tail -n +3 "${TEST_DIR}/output${i}/statistics.csv" 2>/dev/null | awk -F',' '{sum+=$2} END {print sum}')
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

# Stop loader first - no more txs should be sent
pkill -f "armageddon load" || true

# Give receivers a drain window to pull remaining blocks from assemblers
# before killing them. 30s is enough for the assembler backlog to clear.
echo "Waiting 30s for receivers to drain remaining blocks..."
DRAIN_DEADLINE=$(( $(date +%s) + 30 ))
ALL_DONE=false
while [ $(date +%s) -lt $DRAIN_DEADLINE ]; do
  ALL_DONE=true
  for i in $(seq 1 $NUM_PARTIES); do
    if ! grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      ALL_DONE=false
      break
    fi
  done
  if [ "$ALL_DONE" = "true" ]; then
    echo "All receivers finished draining"
    break
  fi
  sleep 2
done

if [ "$ALL_DONE" = "false" ]; then
  echo "Drain window elapsed, stopping receivers"
fi
pkill -f "armageddon receive" || true

echo "=========================================="
echo "✅ Monitoring completed"
echo "=========================================="
