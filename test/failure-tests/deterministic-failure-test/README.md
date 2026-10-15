# Deterministic Failure Test

Starts a local ARMA network (4 parties, 2 shards by default), sends transactions with `armageddon submit`,
and kills and restarts every node **one party at a time, in a fixed order** while it runs.
The test passes only if assembler 1 confirms every transaction that was sent.

## Running

From the repository root (the `make` target builds the binaries first):

```bash
make deterministic-failure-test
make deterministic-failure-test DURATION_MINUTES=10 TX_RATE=500   # override any setting
```

The exit code is `0` if every transaction was confirmed, `1` otherwise.

## Settings

| Variable                       | Workflow input                 | Description                                             | Script | `make` / CI |
| ------------------------------ | ------------------------------ | ------------------------------------------------------- | ------ | ----------- |
| `DURATION_MINUTES`             | `duration_minutes`             | Test duration in minutes                                | `120`  | `5` / `120` |
| `TX_RATE`                      | `tx_rate`                      | Transactions per second                                 | `1000` | `1000`      |
| `TX_SIZE`                      | `tx_size`                      | Transaction size in bytes                               | `300`  | `300`       |
| `NUM_PARTIES`                  | `num_parties` (4, 7, 10)       | Number of parties                                       | `4`    | `4`         |
| `NUM_SHARDS`                   | `num_shards` (1, 2, 4)         | Number of shards                                        | `2`    | `2`         |
| `FAILURE_RUNNER_ENABLED`       | `failure_runner_enabled`       | Kill and restart nodes during the run                   | `true` | `true`      |
| `FAILURE_RUNNER_STOP_DURATION` | `failure_runner_stop_duration` | Seconds a node stays down                               | `60`   | `30` / `60` |
| `FAILURE_RUNNER_RESTART_WAIT`  | `failure_runner_restart_wait`  | Seconds to wait after restarting a node                 | `60`   | `30` / `60` |
| `FAILURE_RUNNER_START_DELAY`   | –                              | Seconds before the first kill, so submit can connect    | `10`   | `10`        |
| `SUBMIT_DRAIN_SECONDS`         | `submit_drain_seconds`         | Max seconds to wait for the last txs after sending ends | `120`  | `120`       |

## How It Works

1. Generates the network config and crypto material (`armageddon generate`) in a temp directory.
2. Starts consenters, batchers, assemblers and routers, waiting for `/healthz` after each one.
   A node that is not healthy within 60s aborts the test and is named in `test/failure-tests/test-results/startup_failure.txt`.
3. Starts `armageddon submit`: it sends `DURATION_MINUTES × 60 × TX_RATE` txs to every router and checks
   off each one as it appears in **assembler 1**'s blocks.
4. Starts the failure runner: for each party in turn it kills the assembler, consenter, router and each
   batcher, one by one, keeping each down for `FAILURE_RUNNER_STOP_DURATION`. One round over all parties
   takes `NUM_PARTIES × (3 + NUM_SHARDS) × (STOP + RESTART)`, about 40 minutes with the script defaults.
5. Prints a status snapshot after each party (every 5 minutes without the runner).
6. When the duration ends, the runner starts no new kills, and the test waits up to `SUBMIT_DRAIN_SECONDS`
   for submit to confirm the last txs. This is a deadline, not a delay: on a healthy run it takes seconds.
   It must exceed `FAILURE_RUNNER_STOP_DURATION` plus ~25s of recovery (measured), because a node that is
   down at the end stays down until the runner restarts it.
7. Writes the results and exits.

**Pass / fail:** the test passes only if `submit.log` contains
`all N txs were sent to the routers and received by assembler 1`. Submit has no "failed" line: a lost tx
makes it wait until the drain deadline stops it, so the missing line is what fails the test.

## Results

```text
test/failure-tests/test-results/
├── logs/                  # all node logs, submit.log, failure_runner.log (gzipped)
├── summary.txt            # load, network, kills, reconnects, blocks, verdict
├── summary-kills.txt      # kill count per node and total
└── failure_reason.txt     # only on failure: one-line reason, used by the Slack notification
```

## Files

| File                                       | What it does                                          |
| ------------------------------------------ | ----------------------------------------------------- |
| `deterministic-failure-test.sh`            | this test: its fixed-order failure runner and `main`  |
| `../failure-test-lib/test-settings.sh`     | the settings above and their defaults                 |
| `../failure-test-lib/arma-network.sh`      | config generation, starting nodes, health checks      |
| `../failure-test-lib/kill-and-restart.sh`  | killing and restarting one node                       |
| `../failure-test-lib/progress-monitor.sh`  | status snapshots and the wait for the last txs        |
| `../failure-test-lib/test-report.sh`       | the verdict, the results folder and the exit code        |

## GitHub Actions

`.github/workflows/deterministic-failure-test.yml` runs this script, shows `summary.txt` in the run's
Summary tab, and uploads the results' `logs/` folder. It runs Sun/Tue/Thu at 03:00 UTC for 2 hours and Sat at
03:00 UTC for 5.5 hours, alternating with the fully randomized test. It can also be started manually with
the workflow inputs above.
