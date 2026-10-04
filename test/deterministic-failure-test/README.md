# Deterministic Failure Test

Starts a local ARMA network (4 parties, 2 shards by default), sends transactions with
`armageddon submit`, and kills and restarts components **one party at a time, in a fixed order**
while it runs. The test passes only if assembler 1 confirms every transaction that was sent.

The same script runs in GitHub Actions and locally on Linux. Run it from the repository root.

## Running Locally

With the failure runner (builds the binaries first):

```bash
make deterministic-failure-test
make deterministic-failure-test DURATION_MINUTES=10 TX_RATE=500   # override any variable
```

Without the failure runner (basic smoke test, needs `make binary` first):

```bash
DURATION_MINUTES=5 TX_RATE=100 FAILURE_RUNNER_ENABLED=false \
test/deterministic-failure-test/deterministic-failure-test.sh
```

The exit code is `0` if every transaction was confirmed, `1` otherwise.

## Configuration

Environment variables, with the defaults of the script and of the `make` target:

| Variable                       | Description                                             | Script | `make` |
| ------------------------------ | ------------------------------------------------------- | ------ | ------ |
| `DURATION_MINUTES`             | Test duration in minutes                                | `120`  | `5`    |
| `TX_RATE`                      | Transactions per second                                 | `1000` | `1000` |
| `TX_SIZE`                      | Transaction size in bytes                               | `300`  | `300`  |
| `NUM_PARTIES`                  | Number of parties                                       | `4`    | `4`    |
| `NUM_SHARDS`                   | Number of shards                                        | `2`    | `2`    |
| `FAILURE_RUNNER_ENABLED`       | Run the failure runner                                  | `true` | `true` |
| `FAILURE_RUNNER_STOP_DURATION` | Seconds a component stays down                          | `60`   | `30`   |
| `FAILURE_RUNNER_RESTART_WAIT`  | Seconds to wait after restarting a component            | `60`   | `30`   |
| `FAILURE_RUNNER_START_DELAY`   | Seconds before the first kill, so submit can connect    | `10`   | `10`   |
| `SUBMIT_DRAIN_SECONDS`         | Max seconds to wait for the last txs after sending ends | `120`  | `120`  |

## Test Flow

1. Generates the network config and crypto material (`armageddon generate`) in a temp directory.
2. Starts consenters, batchers, assemblers and routers, and waits for `/healthz` on each one. A component that is not healthy within 60s aborts the test and is named in `test-results/startup_failure.txt`.
3. Starts `armageddon submit`: it sends `DURATION_MINUTES × 60 × TX_RATE` txs to every router and checks off each one as it appears in **assembler 1**'s blocks.
4. Starts the failure runner (if enabled).
5. Prints a status snapshot after each party's failure cycle (every 5 minutes without the runner) until the duration ends or submit finishes.
6. Stops the failure runner and waits up to `SUBMIT_DRAIN_SECONDS` for submit to confirm the last txs.
7. Writes the results to `test-results/` and exits with the test outcome.

## Pass / Fail

The test passes only if `submit.log` contains `all N txs were sent to the routers and received by assembler 1`.
A lost transaction makes submit wait forever, so it never logs that line; the drain window then stops it
and the test fails. A crash of submit fails the same way.

`SUBMIT_DRAIN_SECONDS` is a deadline, not a delay: on a healthy run the drain takes seconds. It must be
larger than `FAILURE_RUNNER_STOP_DURATION` plus ~25s of recovery (measured), because the component that is down when
the run ends stays down until the runner restarts it (the runner starts no new kills after that).

## Failure Runner

Cycles through the parties in order. For each party it stops, waits `FAILURE_RUNNER_STOP_DURATION`,
restarts, and waits `FAILURE_RUNNER_RESTART_WAIT` for: assembler, consenter, router, then each batcher.
One round over all parties takes `NUM_PARTIES × (3 + NUM_SHARDS) × (STOP + RESTART)`, about 40 minutes
with the script defaults. Its details go to `failure_runner.log`; the console only shows short event lines.

## Results

```text
test-results/
├── logs/                  # all component logs, submit.log, failure_runner.log (gzipped)
├── summary.txt            # load, network, reconnects, blocks, verdict
└── failure_reason.txt     # only on failure: one-line reason, used by the Slack notification
```

## GitHub Actions Workflow

Defined in `.github/workflows/deterministic-failure-test.yml`. It runs `make binary` and this script,
publishes `summary.txt` to the run's Summary tab, and uploads `test-results/logs/` as an artifact.

| Day         | Time (UTC) | Duration  |
| ----------- | ---------- | --------- |
| Sun/Tue/Thu | 03:00      | 2 hours   |
| Sat         | 03:00      | 5.5 hours |

It alternates with the fully randomized failure test (Mon/Wed/Fri). It can also be triggered manually
(`workflow_dispatch`) with these parameters:

| Parameter                      | Default |
| ------------------------------ | ------- |
| `duration_minutes`             | `120`   |
| `tx_rate`                      | `1000`  |
| `tx_size`                      | `300`   |
| `num_parties` (4, 7, or 10)    | `4`     |
| `num_shards` (1, 2, or 4)      | `2`     |
| `failure_runner_enabled`       | `true`  |
| `failure_runner_stop_duration` | `60`    |
| `failure_runner_restart_wait`  | `60`    |
| `submit_drain_seconds`         | `120`   |
