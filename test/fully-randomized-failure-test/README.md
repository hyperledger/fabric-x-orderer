# Fully Randomized Failure Test

## Overview

This directory contains the script used to run the ARMA fully randomized failure test.

The test starts a local ARMA network (4 parties, 2 shards by default), sends transactions and verifies that every party's assembler confirmed each one using a single `armageddon submit`, optionally runs a failure runner that stops and restarts ARMA components chosen **completely at random**, monitors progress, and then collects logs and a summary.

Unlike the deterministic failure test, the randomized failure runner does not cycle through parties or components in any fixed order. Any component — assembler, consenter, router, or batcher — from any party can be killed at any point, including immediately after being restarted. The test is designed to exercise unpredictable failure scenarios that a fixed ordering cannot cover.

The same script is used by the GitHub Actions workflow and can also be executed locally from the command line on a Linux machine.

## Scripts

```text
test/fully-randomized-failure-test/
├── fully-randomized-failure-test.sh
└── README.md
```

### `fully-randomized-failure-test.sh`

Single entry-point script that contains all logic previously split across five separate files.

It defines the following internal functions, then calls `main`:

- **`start_arma_network`** — Starts all ARMA network components in the correct order: consenters first, then batchers, assemblers, and routers. Stores each process PID under the test directory.
- **`run_failure_runner`** — Its verbose output goes to `failure_runner.log`; only short one-line events (`🎲 pick: batcher party 2 shard 1`, `🔻 … down (30s)`) are printed to the console, so status snapshots are never interleaved. Builds a flat pool of every component across all parties (`NUM_PARTIES × (3 + NUM_SHARDS)` entries). On each iteration, picks one entry at random using `$RANDOM`, kills it, waits `FAILURE_RUNNER_STOP_DURATION` seconds, restarts it, then waits `FAILURE_RUNNER_RESTART_WAIT` seconds. After every `N = 3 + NUM_SHARDS` kills a signal file is written so `monitor_completion` knows to print a status snapshot. A running kill counter is maintained in the test directory.
- **`monitor_completion`** — Monitors test execution, reading all progress numbers from `submit.log`. In failure runner mode: prints a status snapshot after every `N = 3 + NUM_SHARDS` kills. Without failure runner: prints a status snapshot every 5 minutes. Always stops when the configured duration is reached, or as soon as `submit` logs `Submit Finished`. It then signals the failure runner to stop and **waits** for `submit` to finish draining and print its verdict, recording its exit code for `main` to propagate.
- **`collect_results`** — Cleans the `test-results/` directory from any previous run, counts per-component kills from `failure_runner.log`, extracts the per-party results and the verdict from `submit.log`, copies all component logs plus `submit.log` and `failure_runner.log` into `test-results/logs/` and gzips them, writes a single-block `summary.txt` (and `failure_reason.txt` when the verdict is not a pass) plus `summary-kills.txt`, then deletes the working-directory logs now that the compressed copies exist.
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
chmod +x test/fully-randomized-failure-test/fully-randomized-failure-test.sh

DURATION_MINUTES=5 \
TX_RATE=100 \
TX_SIZE=300 \
NUM_PARTIES=4 \
NUM_SHARDS=2 \
FAILURE_RUNNER_ENABLED=false \
test/fully-randomized-failure-test/fully-randomized-failure-test.sh
```

### With failure runner enabled

Use the `make` target — it builds the binaries automatically and runs with sensible defaults:

```bash
make fully-randomized-failure-test
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
make fully-randomized-failure-test DURATION_MINUTES=10 TX_RATE=500
```

The values can be adjusted as needed for the desired test configuration.

## Configuration

The test is configured using environment variables.

The values shown below are the defaults used by `fully-randomized-failure-test.sh`.

| Variable                       | Description                                   | Default |
| ------------------------------ | --------------------------------------------- | ------- |
| `DURATION_MINUTES`             | Test duration in minutes                      | `120`   |
| `TX_RATE`                      | Transactions per second                       | `1000`  |
| `TX_SIZE`                      | Transaction size in bytes                     | `300`   |
| `NUM_PARTIES`                  | Number of parties                             | `4`     |
| `NUM_SHARDS`                   | Number of shards                              | `2`     |
| `FAILURE_RUNNER_ENABLED`       | Whether to run the failure runner             | `true`  |
| `FAILURE_RUNNER_STOP_DURATION` | How long to keep a component down, in seconds | `60`    |
| `FAILURE_RUNNER_RESTART_WAIT`  | Wait after restarting a component, in seconds | `60`    |

Network readiness is detected automatically via `/healthz` on each component after it starts — no startup wait needs to be configured.

## Test Flow

When `fully-randomized-failure-test.sh` runs, it performs the following steps:

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

The failure runner builds a flat pool of all `NUM_PARTIES × (3 + NUM_SHARDS)` components:

```
[assembler party 1, consenter party 1, router party 1,
 batcher party 1 shard 1, batcher party 1 shard 2,
 assembler party 2, consenter party 2, router party 2,
 batcher party 2 shard 1, batcher party 2 shard 2,
 ... etc.]
```

On each iteration it selects one entry completely at random (`$RANDOM % pool_size`), kills it, waits `FAILURE_RUNNER_STOP_DURATION` seconds, restarts it, then waits `FAILURE_RUNNER_RESTART_WAIT` seconds. There is no ordering guarantee — the same component can be selected multiple times in a row.

A status snapshot is printed every `N = 3 + NUM_SHARDS` kills. This matches one "equivalent party's worth" of kill events:

| Configuration | N (kills per snapshot) |
| ------------- | ---------------------- |
| 4P 1S         | 4                      |
| 4P 2S         | 5                      |
| 7P 1S         | 4                      |
| 7P 4S         | 7                      |

With default timings (`STOP_DURATION=60`, `RESTART_WAIT=60`) the time between snapshots is approximately:
```
(3 + NUM_SHARDS) × (60 + 60)s
```
e.g. for 4P2S: `5 × 120s ≈ 10 minutes per snapshot`

## Generated Artifacts

During execution, the test creates a temporary directory:

```text
/tmp/fully-randomized-failure-test-XXXXXX/
├── config/          # generated armageddon config per party
├── crypto/          # generated crypto material
├── bootstrap/       # genesis block and shared config
├── data/            # per-component writable data directories
├── pids/            # PID files for all started processes
└── kill_counter     # running total of kills written by the failure runner
```

Result artifacts are written to (cleaned at the start of each run):

```text
test-results/
├── logs/                  # component logs, submit.log, failure_runner.log (gzipped)
├── summary.txt            # per-party confirmed/missing counts, kill counts, verdict
├── summary-kills.txt      # full per-component kill report (artifact only)
└── failure_reason.txt     # (only on failure) one-line reason, used by the Slack step
```

### `summary-kills.txt`

After each run a dedicated kill report is written. Example (4 parties, 2 shards, 2-hour run):

```
========================================
Fully Randomized Failure Test — Kill Report
========================================
Date: Mon Jan  6 03:00:00 UTC 2025
Duration: 120 minutes
Total components in pool: 20 (4 parties × 5 components each)

========================================
Per-Component Kill Counts
========================================
Party 1:
  assembler  party 1:          4 kills
  consenter  party 1:          3 kills
  router     party 1:          5 kills
  batcher    party 1 shard 1:  2 kills
  batcher    party 1 shard 2:  3 kills
Party 2:
  ...

========================================
Total kills: 28
========================================
```

This report is useful for spotting whether certain components were never selected over a run — which may indicate a need to adjust the randomization or introduce a minimum-kill guarantee in a future version.

## GitHub Actions Workflow

The workflow is defined at `.github/workflows/fully-randomized-failure-test.yml`.

Schedule:

| Day | Time (UTC) | Duration |
| --- | ---------- | -------- |
| Mon | 03:00      | 2 hours  |
| Wed | 03:00      | 2 hours  |
| Fri | 03:00      | 2 hours  |
| Sat | 09:00      | 5.5 hours (starts after deterministic test ends at 08:30 + 30 min gap) |

The workflow can also be triggered manually via `workflow_dispatch`.

Steps:
1. Checks out the repository.
2. Installs Go.
3. Builds binaries with `make binary`.
4. Determines test duration (schedule-based or from manual input).
5. Sets configuration via environment variables.
6. Runs `test/fully-randomized-failure-test/fully-randomized-failure-test.sh`.
7. Publishes the test summary directly to the workflow run's Summary tab (plain text, no download needed).
8. Uploads `test-results/logs/` and the kill report as CI artifacts.

## Manual Workflow Trigger

The following parameters can be set when triggering manually:

| Parameter                      | Description                                 | Default |
| ------------------------------ | ------------------------------------------- | ------- |
| `duration_minutes`             | Test duration in minutes                    | `120`   |
| `tx_rate`                      | Transactions per second                     | `1000`  |
| `tx_size`                      | Transaction size in bytes                   | `300`   |
| `num_parties`                  | Number of parties (4, 7, or 10)             | `4`     |
| `num_shards`                   | Number of shards (1, 2, or 4)               | `2`     |
| `failure_runner_enabled`       | Enable fully randomized failure runner      | `true`  |
| `failure_runner_stop_duration` | How long to keep component down (s)         | `60`    |
| `failure_runner_restart_wait`  | Wait after component restart (s)            | `60`    |
| `submit_drain_seconds`         | How long submit keeps verifying after sending | `420` |
