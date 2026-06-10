#!/bin/bash
# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Monitor test completion - wait for loader and all receivers to finish

NUM_PARTIES=$1
TOTAL_TXS=$2

echo "=========================================="
echo "Monitoring Test Completion"
echo "=========================================="
echo "Waiting for loader to finish and all ${NUM_PARTIES} receivers to complete..."
echo "Expected TXs: ${TOTAL_TXS}"
echo "=========================================="

# Monitor loader
echo "Monitoring loader..."
while true; do
  if grep -q "Load command finished" loader.log 2>/dev/null; then
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Loader completed!"
    break
  fi
  echo "[$(date '+%Y-%m-%d %H:%M:%S')] Loader still running..."
  sleep 30
done

# Monitor all receivers
for i in $(seq 1 $NUM_PARTIES); do
  echo "Monitoring receiver ${i}..."
  while true; do
    if grep -q "Receive command finished" receiver${i}.log 2>/dev/null; then
      # Extract received transaction count
      RECEIVED=$(grep "were successfully received" receiver${i}.log 2>/dev/null | tail -1 | awk '{print $1}')
      if [ -n "$RECEIVED" ]; then
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Receiver ${i} completed! Received: ${RECEIVED} txs"
      else
        echo "[$(date '+%Y-%m-%d %H:%M:%S')] ✅ Receiver ${i} completed!"
      fi
      break
    fi
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] Receiver ${i} still running..."
    sleep 30
  done
done

echo "=========================================="
echo "✅ All receivers completed!"
echo "=========================================="
