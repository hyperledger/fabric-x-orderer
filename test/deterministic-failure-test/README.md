# Deterministic Failure Test

## Overview

This directory contains the script used to run the ARMA deterministic failure test.

The test starts a local ARMA network (4 parties, 2 shards by default), sends transactions and verifies that every party's assembler confirmed each one using a single `armageddon submit`, optionally runs a failure runner that stops and restarts ARMA components one party at a time, monitors progress, and then collects logs and a summary.

The same script is used by the GitHub Actions workflow and can also be executed locally from the command line on a Linux machine.

## Scripts

```text
test/deterministic-failure-test/
├── deterministic-failure-test.sh
└── README.md
```

### `deterministic-failure-test.sh`

Single entry-point script that contains all logic previously split across five separate files.

It defines the following internal functions, then calls `main`:

- **`start_arma_network`** — Starts all ARMA network components in the correct order: consenters first, then batchers, assemblers, and routers. Stores each process PID under the test directory.
- **`run_failure_runner`** — Stops and restarts ARMA components one party at a time in a continuous loop until the stop signal is received. Its verbose output (PIDs, waits, force-kills) goes to `failure_runner.log`; only short one-line events (`🔻 assembler party 1 down (30s)`) are printed to the console, so status snapshots are never interleaved. For each party it kills and restarts: assembler, consenter, router, then all batchers in shard order. After each full party cycle it writes a signal file so `monitor_completion` knows to print a status snapshot.
- **`monitor_completion`** — Monitors test execution, reading all progress numbers from `submit.log`. In failure runner mode: prints a status snapshot after each party's full failure cycle completes. Without failure runner: prints a status snapshot every 5 minutes. Always stops when the configured duration is reached, or as soon as `submit` logs `Submit Finished`. It then signals the failure runner to stop and **waits** for `submit` to finish draining and print its verdict, recording its exit code for `main` to propagate.
- **`collect_results`** — Cleans the `test-results/` directory from any previous run, extracts the per-party results and the verdict from `submit.log`, copies all component logs plus `submit.log` into `test-results/logs/` and gzips them, writes a single-block `summary.txt` (and `failure_reason.txt` when the verdict is not a pass), then deletes the working-directory logs now that the compressed copies exist.
- **`main`** — Reads configuration from environment variables, removes stale log files from previous runs, generates the network config YAML, runs `armageddon generate` to produce all crypto and config files, patches the generated FileStore `Location` and consenter `WALDir` paths to writable temp directories, then calls the functions above in order.

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

### Without failure runner (basic smoke test)

```bash
chmod +x test/deterministic-failure-test/deterministic-failure-test.sh

DURATION_MINUTES=5 \
TX_RATE=100 \
TX_SIZE=300 \
NUM_PARTIES=4 \
NUM_SHARDS=2 \
FAILURE_RUNNER_ENABLED=false \
test/deterministic-failure-test/deterministic-failure-test.sh
```

### With failure runner enabled

Use the `make` target — it builds the binaries automatically and runs with sensible defaults:

```bash
make deterministic-failure-test
```

Default values used by the target:

| Variable                       | Value  |
| ------------------------------ | ------ |
| `DURATION_MINUTES`             | `5`    |
| `TX_RATE`                      | `1000` |
| `TX_SIZE`                      | `300`  |
| `NUM_PARTIES`                  | `4`    |
| `NUM_SHARDS`                   | `2`    |
| `FAILURE_RUNNER_ENABLED`       | `true` |
| `FAILURE_RUNNER_STOP_DURATION` | `30`   |
| `FAILURE_RUNNER_RESTART_WAIT`  | `30`   |

Any variable can be overridden on the command line:

```bash
make deterministic-failure-test DURATION_MINUTES=10 TX_RATE=500
```

The values can be adjusted as needed for the desired test configuration.

## Configuration

The test is configured using environment variables.

The values shown below are the defaults used by `deterministic-failure-test.sh`.

| Variable              | Description                                   | Default |
| --------------------- | --------------------------------------------- | ------- |
| `DURATION_MINUTES`    | Test duration in minutes                      | `120`   |
| `TX_RATE`             | Transactions per second                       | `1000`  |
| `TX_SIZE`             | Transaction size in bytes                     | `300`   |
| `NUM_PARTIES`         | Number of parties                             | `4`     |
| `NUM_SHARDS`          | Number of shards                              | `2`     |
| `FAILURE_RUNNER_ENABLED`       | Whether to run the failure runner             | `true`  |
| `FAILURE_RUNNER_STOP_DURATION` | How long to keep a component down, in seconds | `60`    |
| `FAILURE_RUNNER_RESTART_WAIT`  | Wait after restarting a component, in seconds | `60`    |

Network readiness is detected automatically via `/healthz` on each component after it starts — no startup wait needs to be configured.

## Test Flow

When `deterministic-failure-test.sh` runs, it performs the following steps:

1. Reads configuration from environment variables.
2. Calculates the total number of transactions (`DURATION_MINUTES × 60 × TX_RATE`).
3. Creates a temporary test directory using `mktemp`.
4. Generates a network config YAML with ports allocated at `127.0.0.1:8011–8045`.
5. Runs `./bin/armageddon generate --sampleConfigPath=testutil/fabric/sampleconfig`.
6. Patches all generated `Location` (FileStore) and `WALDir` (consenter) paths to writable per-component subdirectories under the temp dir.
7. Removes stale log files from any previous run.
8. Starts the ARMA network via `start_arma_network`, which polls `/healthz` on each component immediately after it starts. If any component fails to become healthy within 60 s the test aborts immediately, naming the failing component in the error output and in `test-results/startup_failure.txt`.
9. Starts `armageddon submit` (background) — it both sends transactions and verifies every party.
10. (nothing — `submit` replaced the separate loader and receivers.)
11. Starts `run_failure_runner` if `FAILURE_RUNNER_ENABLED=true` (background).
12. Calls `monitor_completion` (blocks until duration expires or all components finish).
13. Waits briefly for the failure runner to stop gracefully.
14. Calls `collect_results`.
15. Kills any remaining `arma` and `armageddon` processes.

## Transaction Verification

A single `armageddon submit` replaces the old loader plus one receiver per party. It sends
transactions to every router and, in the same process, pulls blocks from **every** party's
assembler and checks off each transaction it sent. At the end it reports, per party, how many
of the transactions it sent were confirmed and how many are missing.

Every transaction is committed to every party's assembler ledger, so each party is expected to
confirm the full `TOTAL_TXS` count — not `TOTAL_TXS / NUM_PARTIES`.

`submit` is started with:

- `--pullFromPartyId=0` — verify against all parties (party 1's user config already lists every
  assembler endpoint).
`submit` runs until every party has confirmed every transaction, so the deadline lives in this
script: when the drain window closes it sends `SIGTERM`, and `submit` reports whatever it has
confirmed by then.

### The drain window (`SUBMIT_DRAIN_SECONDS`, default 420)

`TOTAL_TXS = DURATION x 60 x TX_RATE`, so the last transaction is sent right at the end of the
test window while thousands are still legitimately in flight through
router -> batcher -> consensus -> assembler. `SUBMIT_DRAIN_SECONDS` is how long `submit` keeps
verifying after sending stops.

It is a **deadline, not a delay**: `submit` exits as soon as every party has confirmed every
transaction, so a generous value costs nothing on a healthy run. It must exceed
`FAILURE_RUNNER_STOP_DURATION + FAILURE_RUNNER_RESTART_WAIT`, because the failure runner finishes
the component it is working on after being told to stop — so the network can be incomplete for
that long after the test window closes.

The script does **not** kill `submit`; it waits for it, because `submit` prints the verdict when
it stops.

### Exit codes

The script exits with `submit`'s verdict, so a lost transaction turns the CI job red and fires
the Slack notification:

| Code | Meaning |
| ---- | ------- |
| `0`  | every transaction sent was confirmed by every assembler |
| `1`  | transactions were sent but never confirmed — the bug this test hunts |
| `3`  | infrastructure failure (e.g. an assembler was unreachable); verification inconclusive |

When `submit` was stopped by the drain deadline while transactions were still outstanding, the
summary says the run was inconclusive rather than claiming the transactions were lost.

## Failure Runner Behaviour

The failure runner cycles through all parties in order. For each party it kills and waits `FAILURE_RUNNER_STOP_DURATION` seconds, then restarts:

1. Assembler
2. Consenter
3. Router
4. Batcher shard 1
5. Batcher shard 2 (and any further shards)

After the restart, it waits `FAILURE_RUNNER_RESTART_WAIT` seconds before moving to the next component. After all components of a party are done, it signals the monitor to print a status snapshot, then moves to the next party.

A full round across all 4 parties with default timings (`STOP_DURATION=60`, `RESTART_WAIT=60`) takes approximately:
```
4 parties × (4 components + NUM_SHARDS) × (60 + 60)s ≈ 57 minutes
```

## Generated Artifacts

During execution, the test creates a temporary directory:

```text
/tmp/deterministic-failure-test-XXXXXX/
├── config/          # generated armageddon config per party
├── crypto/          # generated crypto material
├── bootstrap/       # genesis block and shared config
├── data/            # per-component writable data directories
└── pids/            # PID files for all started processes
```

Result artifacts are written to (cleaned at the start of each run):

```text
test-results/
├── logs/                  # component logs, submit.log, failure_runner.log (gzipped)
├── summary.txt            # per-party confirmed/missing counts and the verdict
└── failure_reason.txt     # (only on failure) one-line reason, used by the Slack step
```

## GitHub Actions Workflow

The workflow is defined at `.github/workflows/deterministic-failure-test.yml`.

Schedule:

| Day | Time (UTC) | Duration |
| --- | ---------- | -------- |
| Sun | 03:00      | 2 hours  |
| Tue | 03:00      | 2 hours  |
| Thu | 03:00      | 2 hours  |
| Sat | 03:00      | 5.5 hours |

It alternates with the fully randomized failure test (Mon/Wed/Fri) so both tests run on separate days. The workflow can also be triggered manually via `workflow_dispatch`.

Steps:
1. Checks out the repository.
2. Installs Go.
3. Builds binaries with `make binary`.
4. Determines test duration (schedule-based or from manual input).
5. Sets configuration via environment variables.
6. Runs `test/deterministic-failure-test/deterministic-failure-test.sh`.
7. Publishes the test summary directly to the workflow run's Summary tab (plain text, no download needed).
8. Uploads `test-results/logs/` as a CI artifact.

## Manual Workflow Trigger

The following parameters can be set when triggering manually:

| Parameter             | Description                          | Default |
| --------------------- | ------------------------------------ | ------- |
| `duration_minutes`    | Test duration in minutes             | `120`   |
| `tx_rate`             | Transactions per second              | `1000`  |
| `tx_size`             | Transaction size in bytes            | `300`   |
| `num_parties`         | Number of parties (4, 7, or 10)      | `4`     |
| `num_shards`          | Number of shards (1, 2, or 4)        | `2`     |
| `failure_runner_enabled`       | Enable failure runner                        | `true`  |
| `failure_runner_stop_duration` | How long to keep component down (s)          | `60`    |
| `failure_runner_restart_wait`  | Wait after component restart (s)             | `60`    |
| `submit_drain_seconds`         | How long submit keeps verifying after sending | `420`  |
