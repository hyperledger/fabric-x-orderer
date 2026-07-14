# Chaos Test

## Overview

This directory contains the scripts used to run the ARMA chaos test.

The test starts a local ARMA network (4 parties, 2 shards by default), sends transactions through the loader, pulls blocks from each party's assembler via receivers, optionally runs a chaos runner that stops and restarts ARMA components one party at a time, monitors progress, and then collects logs, statistics, and a summary.

The same scripts are used by the GitHub Actions workflow and can also be executed locally from the command line on a Linux machine.

## Scripts

```text
test/chaos/
├── chaos-test.sh
├── start-arma-network.sh
├── chaos-runner.sh
├── monitor-completion.sh
├── collect-results.sh
└── README.md
```

### `chaos-test.sh`

Main orchestration script.

It reads configuration from environment variables, removes stale log files from previous runs, generates the network config YAML, runs `armageddon generate` to produce all crypto and config files, patches the generated FileStore `Location` and consenter `WALDir` paths to writable temp directories, starts the ARMA network, starts receivers and the loader, optionally starts the chaos runner, runs the monitor, and collects results.

### `start-arma-network.sh`

Starts all ARMA network components in the correct order: consenters first, then batchers, assemblers, and routers. Stores each process PID under the test directory.

### `chaos-runner.sh`

Stops and restarts ARMA components one party at a time in a continuous loop until the stop signal is received.

For each party it kills and restarts: assembler, consenter, router, then all batchers in shard order. After each full party cycle it writes a signal file so `monitor-completion.sh` knows to print a status snapshot.

### `monitor-completion.sh`

Monitors test execution.

- In chaos mode: prints a status snapshot after each party's full chaos cycle completes.
- Without chaos: prints a status snapshot every 5 minutes.
- Always: stops when the configured duration is reached or when the loader and all receivers finish early.

After duration expires, stops the loader immediately then gives receivers a 30-second drain window to pull remaining blocks from the assemblers before killing them.

### `collect-results.sh`

Collects test results.

Cleans the `test-results/` directory from any previous run, then extracts loader and receiver statistics, copies all component and loader/receiver logs into `test-results/logs/` and compresses them with gzip, collects receiver statistics CSV files, and creates a summary report.

## Prerequisites

Build the binaries before running the test:

```bash
make binary
```

The scripts expect the following binaries to exist:

```text
./bin/arma
./bin/armageddon
```

Run the test from the repository root.

## Running Locally

Execute the test from the repository root.

### Without chaos (basic smoke test)

```bash
chmod +x test/chaos/*.sh

DURATION_MINUTES=5 \
TX_RATE=100 \
TX_SIZE=300 \
NUM_PARTIES=4 \
NUM_SHARDS=2 \
CHAOS_ENABLED=false \
test/chaos/chaos-test.sh
```

### With chaos enabled

```bashsame idea
chmod +x test/chaos/*.sh

DURATION_MINUTES=5 \
TX_RATE=1000 \
TX_SIZE=300 \
NUM_PARTIES=4 \
NUM_SHARDS=2 \
CHAOS_ENABLED=true \
CHAOS_INITIAL_WAIT=120 \
CHAOS_STOP_DURATION=30 \
CHAOS_RESTART_WAIT=30 \
test/chaos/chaos-test.sh
```

The values can be adjusted as needed for the desired test configuration.

## Configuration

The test is configured using environment variables.

The values shown below are the defaults used by `chaos-test.sh`.

| Variable              | Description                                   | Default |
| --------------------- | --------------------------------------------- | ------- |
| `DURATION_MINUTES`    | Test duration in minutes                      | `120`   |
| `TX_RATE`             | Transactions per second                       | `1000`  |
| `TX_SIZE`             | Transaction size in bytes                     | `300`   |
| `NUM_PARTIES`         | Number of parties                             | `4`     |
| `NUM_SHARDS`          | Number of shards                              | `2`     |
| `CHAOS_ENABLED`       | Whether to run the chaos runner               | `true`  |
| `CHAOS_INITIAL_WAIT`  | Wait before first chaos action, in seconds    | `300`   |
| `CHAOS_STOP_DURATION` | How long to keep a component down, in seconds | `60`    |
| `CHAOS_RESTART_WAIT`  | Wait after restarting a component, in seconds | `60`    |

## Test Flow

When `chaos-test.sh` runs, it performs the following steps:

1. Reads configuration from environment variables.
2. Calculates the total number of transactions (`DURATION_MINUTES × 60 × TX_RATE`).
3. Creates a temporary test directory using `mktemp`.
4. Generates a network config YAML with ports allocated at `127.0.0.1:8011–8045`.
5. Runs `./bin/armageddon generate --sampleConfigPath=testutil/fabric/sampleconfig`.
6. Patches all generated `Location` (FileStore) and `WALDir` (consenter) paths to writable per-component subdirectories under the temp dir.
7. Removes stale log files from any previous run.
8. Starts the ARMA network using `start-arma-network.sh`.
9. Starts one receiver per party (background).
10. Starts the loader (background).
11. Starts `chaos-runner.sh` if `CHAOS_ENABLED=true` (background).
12. Runs `monitor-completion.sh` (blocks until duration expires or all components finish).
13. Waits briefly for the chaos runner to stop gracefully.
14. Runs `collect-results.sh`.
15. Kills any remaining `arma` and `armageddon` processes.

## Receiver Behaviour

Each party runs an independent assembler. Every transaction sent by the loader is committed to **every** party's assembler ledger. Each receiver pulls from its own party's assembler independently. This means each party's receiver is expected to receive the full `TOTAL_TXS` count — not `TOTAL_TXS / NUM_PARTIES`.

The receiver stops pulling when it has received at least `expectedTxs` transactions. Because it processes whole blocks, it may overshoot by the number of transactions in the final block — this is expected and not an error.

## Chaos Behaviour

The chaos runner cycles through all parties in order. For each party it kills and waits `CHAOS_STOP_DURATION` seconds, then restarts:

1. Assembler
2. Consenter
3. Router
4. Batcher shard 1
5. Batcher shard 2 (and any further shards)

After the restart, it waits `CHAOS_RESTART_WAIT` seconds before moving to the next component. After all components of a party are done, it signals the monitor to print a status snapshot, then moves to the next party.

A full round across all 4 parties with default timings (`STOP_DURATION=60`, `RESTART_WAIT=60`) takes approximately:
```
4 parties × (4 components + NUM_SHARDS) × (60 + 60)s ≈ 57 minutes
```

## Generated Artifacts

During execution, the test creates a temporary directory:

```text
/tmp/chaos-test-XXXXXX/
├── config/          # generated armageddon config per party
├── crypto/          # generated crypto material
├── bootstrap/       # genesis block and shared config
├── data/            # per-component writable data directories
├── output*/         # receiver statistics CSV files per party
└── pids/            # PID files for all started processes
```

Result artifacts are written to (cleaned at the start of each run):

```text
test-results/
├── logs/            # component and loader/receiver logs
├── statistics/      # per-party statistics CSV files
└── summary/         # summary.txt with pass/fail and tx counts
```

## GitHub Actions Workflow

The workflow is defined at `.github/workflows/chaos-test.yml`.

It runs on a schedule (Sunday–Thursday: 2-hour test, Friday: 5.5-hour test) and can also be triggered manually via `workflow_dispatch`.

Steps:
1. Checks out the repository.
2. Installs Go.
3. Builds binaries with `make binary`.
4. Determines test duration (schedule-based or from manual input).
5. Sets configuration via environment variables.
6. Runs `test/chaos/chaos-test.sh`.
7. Uploads `test-results/` artifacts (summary, statistics, logs — all as separate CI artifacts).

## Manual Workflow Trigger

The following parameters can be set when triggering manually:

| Parameter             | Description                          | Default |
| --------------------- | ------------------------------------ | ------- |
| `duration_minutes`    | Test duration in minutes             | `120`   |
| `tx_rate`             | Transactions per second              | `1000`  |
| `tx_size`             | Transaction size in bytes            | `300`   |
| `num_parties`         | Number of parties (4, 7, or 10)      | `4`     |
| `num_shards`          | Number of shards (1, 2, or 4)        | `2`     |
| `chaos_enabled`       | Enable chaos testing                 | `true`  |
| `chaos_initial_wait`  | Wait before starting chaos (seconds) | `300`   |
| `chaos_stop_duration` | How long to keep component down (s)  | `60`    |
| `chaos_restart_wait`  | Wait after component restart (s)     | `60`    |
