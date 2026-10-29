# Custom Order Failure Test

Starts a local ARMA network (4 parties, 2 shards by default), sends transactions with `armageddon submit`,
and kills and restarts nodes **in the order given by `KILL_ORDER`** while it runs, to repeat a failure
sequence on purpose. The test passes only if assembler 1 confirms every transaction that was sent.

## Running

From the repository root, on Linux or macOS (the `make` target builds the binaries first):

```bash
make custom-order-failure-test                                       # the default order below
make custom-order-failure-test KILL_ORDER="C -> A -> B1 -> R -> B2"  # your own order
make custom-order-failure-test KILL_ORDER="p3:C p3:C p4:A" DURATION_MINUTES=10
```

The exit code is `0` if every transaction was confirmed, `1` otherwise.

## The kill order

Items are separated by `->`, `,` or spaces:

| Item  | Node                       |
| ----- | -------------------------- |
| `C`   | consenter                  |
| `A`   | assembler                  |
| `R`   | router                     |
| `B<n>`| batcher of shard `n`       |
| `pN:` | before an item: in party N |

There are two forms, and one order uses only one of them:

- **Per party**, no `pN:`: the order runs for party 1, then for party 2, and so on up to `NUM_PARTIES`.
  `C -> A -> B1 -> R -> B2` with 4 parties kills `p1:C p1:A p1:B1 p1:R p1:B2 p2:C …`, 20 kills per round.
- **Exact**, every item with `pN:`: exactly these kills, for example `p4:A p2:B1 p3:C p3:C`.

In both forms the order starts again from the beginning until the duration ends. An unknown item, a
party above `NUM_PARTIES`, a shard above `NUM_SHARDS`, or an order that mixes the two forms stops the test
at startup, with the reason in `test/failure-tests/test-results/startup_failure.txt`.

### The default order

`C -> A -> R -> C -> C -> B1 -> B2`. With the `make` and CI defaults it reproduces the assembler crash
of #1353 about 20 minutes in, on party 3 (`panic: block number should have been N but was M`):

1. Party 2's `B1` kill moves the primary of shard 1 to party 3's batcher.
2. Party 3's first `C` kill: after the restart, that batcher goes back to the old primary and stops
   (it keeps logging `Created complaint with term 0`), so assembler 3 falls behind.
3. Party 3's last `C` kill, about 4 minutes later: assembler 3 reconnects to its consenter, receives
   blocks it already has, and panics.

The order comes from the fully randomized test, which found the crash with seeds `19163133` and
`20090328`: in both, an assembler panicked after one party's consenter was restarted a few times within
a few minutes. `C -> C -> C -> A -> R -> B1 -> B2` did not crash: its last `C` came before assembler 3
was behind.

## Settings

| Variable                       | Workflow input                 | Description                                             | Script | `make` / CI       |
| ------------------------------ | ------------------------------ | ------------------------------------------------------- | ------ | ----------------- |
| `KILL_ORDER`                   | `kill_order`                   | The kill order, see above                               | the default order | same  |
| `DURATION_MINUTES`             | `duration_minutes`             | Test duration in minutes                                | `120`  | `5` / `30` (scheduled: `60`) |
| `TX_RATE`                      | `tx_rate`                      | Transactions per second                                 | `1000` | `1000`            |
| `TX_SIZE`                      | `tx_size`                      | Transaction size in bytes                               | `300`  | `300`             |
| `NUM_PARTIES`                  | `num_parties` (4, 7, 10)       | Number of parties                                       | `4`    | `4`               |
| `NUM_SHARDS`                   | `num_shards` (1, 2, 4)         | Number of shards                                        | `2`    | `2`               |
| `FAILURE_RUNNER_STOP_DURATION` | `failure_runner_stop_duration` | Seconds a node stays down                               | `60`   | `30`              |
| `FAILURE_RUNNER_RESTART_WAIT`  | `failure_runner_restart_wait`  | Seconds to wait after restarting a node                 | `60`   | `30`              |
| `FAILURE_RUNNER_START_DELAY`   | –                              | Seconds before the first kill, so submit can connect    | `10`   | `10`              |
| `SUBMIT_DRAIN_SECONDS`         | `submit_drain_seconds`         | Max seconds to wait for the last txs after sending ends | `120`  | `120`             |

The failure runner is always enabled in this test.

## How It Works

The steps are the same as in the [deterministic failure test](../deterministic-failure-test/README.md#how-it-works),
except step 4: the failure runner goes through the kills of `KILL_ORDER` one by one, keeping each node down
for `FAILURE_RUNNER_STOP_DURATION`, and prints a status snapshot after each round of the order. The summary
shows the order that ran.

## Results

The same as the [deterministic failure test](../deterministic-failure-test/README.md#results), in
`test/failure-tests/test-results/`.

## Files

| File                             | What it does                                                |
| -------------------------------- | ----------------------------------------------------------- |
| `custom-order-failure-test.sh`   | this test: reads `KILL_ORDER`, its failure runner and `main` |
| `../failure-test-lib/`           | the code shared by all failure tests                        |

## GitHub Actions

`.github/workflows/custom-order-failure-test.yml` runs this script, shows `summary.txt` and
`summary-kills.txt` in the run's Summary tab, and uploads the results' `logs/` folder and the kill report.
It runs Sun/Tue/Thu at 05:30 UTC for 1 hour with the default order, after the deterministic test ends, and
can be started manually with the workflow inputs above. A failed run, scheduled or manual, is reported to
Slack with the run type and the kill order.
