## Running Arma with Prometheus and Grafana

This README explains how to run the Arma sample network of [node/examples](../README.md) together with Prometheus and Grafana, and watch its metrics on a dashboard while a test runs.

On default this docker compose runs the arma network of four parties with two shards, with Prometheus to scrape every node and Grafana to display the dashboard, and submits transactions for five minutes.

This test is intended for local use: Grafana is reachable without a login and nothing is encrypted
towards the host.

### Prerequisites

- Docker or Podman with the Compose v2 plugin, usable as your own user
- Go, for `make binary`

### Quick Start

Both commands are run from the root directory.

```
1.
(cd node/examples; bash ./scripts/build_docker.sh)
```
This builds the `arma` image from [node/examples/Dockerfile](../Dockerfile), the same image the
other examples use.

```
2.
./node/examples/grafana/scripts/run_dashboard.sh
```

This script performs the following tasks:
- Generates a configuration file for each node using `armageddon generate`.
- Sets a known metrics port in each node configuration, so Prometheus can scrape it.
- Writes the Prometheus scrape targets and the Grafana data source and dashboard.
- Starts the network of [node/examples/compose.yaml](../compose.yaml) together with Prometheus and Grafana.
- Waits until every node is being scraped.
- Submits transactions using `armageddon submit`, which processes 300000 transactions at a
  rate of 1000 per second.
- Prints the dashboard link and opens it in a browser if there is a display.

Grafana and Prometheus are published on ports chosen by Docker, so they never collide with
anything already running. The script prints both:

```
EXAMPLE:
  Dashboard  : http://localhost:32769/d/arma-dashboard
  Prometheus : http://localhost:32770
```

The script returns once the network is up, leaving the test running. Watch the dashboard
for progress: `submit` itself stays quiet until it has seen every transaction in a block,
then reports the transaction rate, block rate and block size.

```
docker logs -f arma-grafana-submitter-1
```

### Clean Up Sample

To clean up the environment after running the example, run:
```
./node/examples/grafana/scripts/clean_dashboard.sh
```

This script stops and removes the Docker containers and volumes, and deletes `/tmp/arma-sample`.

### Configuration

The following variables can be used to run the default arma network differently:

| Variable           | Default   | Meaning                                    |
| ------------------ | --------- | ------------------------------------------ |
| `RATE`             | `1000`    | transactions per second                    |
| `TX_SIZE`          | `300`     | transaction size in bytes                  |
| `DURATION_SECONDS` | `300`     | how long to submit; times `RATE` gives the transaction count |
| `OPEN_BROWSER`     | `auto`    | `false` to never open a browser            |

```
EXAMPLE:

RATE=500 DURATION_SECONDS=60 ./node/examples/grafana/scripts/run_dashboard.sh
```

The number of parties and shards comes from the network itself, so changing it means
editing [../config/example-deployment.yaml](../config/example-deployment.yaml) and
[../compose.yaml](../compose.yaml).

The work directory is `/tmp/arma-sample`, fixed by [../compose.yaml](../compose.yaml), which
mounts it into every node.

### The Dashboard

[grafana/arma-dashboard.json](grafana/arma-dashboard.json) is provisioned on startup, in four rows: Batchers,
Consenters, Routers, Assemblers.
 The panels split per party and per shard by themselves, using the labels the nodes attach to their metrics, which are documented in
[docs/monitoring/metrics.md](../../../docs/monitoring/metrics.md).

Panels can be edited during a run. 
Edits live in Grafana's volume, which `clean_dashboard.sh` removes, so export the JSON over [grafana/arma-dashboard.json](grafana/arma-dashboard.json) to keep one.
