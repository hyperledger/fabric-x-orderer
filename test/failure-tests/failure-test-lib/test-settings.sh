# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# Settings of the failure tests: environment variables with their defaults.

DURATION=${DURATION_MINUTES:-120}
TX_RATE=${TX_RATE:-1000}
TX_SIZE=${TX_SIZE:-300}
NUM_PARTIES=${NUM_PARTIES:-4}
NUM_SHARDS=${NUM_SHARDS:-2}
FAILURE_RUNNER_ENABLED=${FAILURE_RUNNER_ENABLED:-true}
STOP_DURATION=${FAILURE_RUNNER_STOP_DURATION:-60}
RESTART_WAIT=${FAILURE_RUNNER_RESTART_WAIT:-60}

# Let submit connect to every router and assembler 1 first; it exits if one is down at startup
FAILURE_RUNNER_START_DELAY=${FAILURE_RUNNER_START_DELAY:-10}

# Max wait (not a delay) for the last txs; must exceed STOP_DURATION + ~25s recovery (measured)
SUBMIT_DRAIN_SECONDS=${SUBMIT_DRAIN_SECONDS:-120}

TOTAL_TXS=$((DURATION * 60 * TX_RATE))
RESULTS_DIR=test/failure-tests/test-results
