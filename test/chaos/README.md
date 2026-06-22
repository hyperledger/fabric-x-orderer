# Chaos Test

## Overview

This directory contains the scripts used to run the ARMA chaos test.

The test starts a local ARMA network, starts receivers and a loader, optionally runs a chaos runner that stops and restarts ARMA components, monitors the test until completion or timeout, and then collects logs, statistics, and a summary.

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

It reads the test parameters from environment variables, creates a temporary test directory, generates the Armageddon configuration, generates the ARMA configuration files, updates the generated FileStore locations, starts the ARMA network, starts receivers, starts the loader, optionally starts the chaos runner, monitors the test, and collects the results.

### `start-arma-network.sh`

Starts the ARMA network components.

It starts consenters, batchers, assemblers, and routers, and stores their process IDs under the test directory.

### `chaos-runner.sh`

Stops and restarts ARMA components in sequence.

It uses the PID files created by `start-arma-network.sh`, waits according to the chaos timing configuration, and exits when the stop signal file is created.

### `monitor-completion.sh`

Monitors the test execution.

It waits until the configured duration is reached or until the loader and all receivers complete. When monitoring finishes, it creates the chaos stop signal and stops loader and receiver processes if they are still running.

### `collect-results.sh`

Collects test results.

It creates the `test-results` directory, extracts loader and receiver statistics, copies or compresses logs depending on the test duration, collects receiver statistics CSV files, and creates a summary report.

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

The following command is an example configuration:

```bash
chmod +x test/chaos/*.sh

DURATION_MINUTES=10 \
TX_RATE=1000 \
TX_SIZE=300 \
NUM_PARTIES=4 \
NUM_SHARDS=2 \
CHAOS_ENABLED=true \
CHAOS_INITIAL_WAIT=30 \
CHAOS_STOP_DURATION=10 \
CHAOS_RESTART_WAIT=10 \
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
| `CHAOS_INITIAL_WAIT`  | Wait before starting chaos, in seconds        | `300`   |
| `CHAOS_STOP_DURATION` | How long to keep a component down, in seconds | `60`    |
| `CHAOS_RESTART_WAIT`  | Wait after restarting a component, in seconds | `60`    |

## Test Flow

When `chaos-test.sh` runs, it performs the following steps:

1. Reads configuration from environment variables.
2. Calculates the total number of transactions.
3. Creates a temporary test directory using `mktemp`.
4. Generates a test configuration YAML file.
5. Runs `./bin/armageddon generate`.
6. Creates a data directory under the temporary test directory.
7. Updates the generated FileStore locations so each component uses its own data directory.
8. Starts the ARMA network using `start-arma-network.sh`.
9. Creates output directories for receivers.
10. Starts one receiver per party.
11. Starts the loader.
12. Starts `chaos-runner.sh` if `CHAOS_ENABLED=true`.
13. Runs `monitor-completion.sh`.
14. Waits briefly for the chaos runner to stop.
15. Runs `collect-results.sh`.
16. Cleans up remaining `arma` and `armageddon` processes.

## Generated Files

During execution, the test creates a temporary directory similar to:

```text
/tmp/chaos-test-XXXXXX
```

That directory contains generated configuration, data directories, receiver output directories, PID files, and the chaos stop signal file.

The result artifacts are written under:

```text
test-results/
├── logs/
├── statistics/
└── summary/
```

## GitHub Actions Workflow

The GitHub Actions workflow is defined under:

```text
.github/workflows/chaos-test.yml
```

The workflow is responsible for invoking the chaos test in GitHub Actions.

It performs the following steps:

1. Checks out the repository.
2. Installs Go.
3. Builds the binaries using `make binary`.
4. Determines the test duration.
5. Sets the test configuration through environment variables.
6. Executes:

```bash
test/chaos/chaos-test.sh
```

7. Uploads the generated artifacts from `test-results`.

The workflow contains only the GitHub Actions orchestration. The chaos test implementation resides under `test/chaos`.

## Manual Runs

The workflow can also be triggered manually using `workflow_dispatch`.

The following parameters can be configured:

* Test duration
* Transaction rate
* Transaction size
* Number of parties
* Number of shards
* Whether chaos is enabled
* Chaos timing values

These values are passed to `test/chaos/chaos-test.sh` through environment variables.