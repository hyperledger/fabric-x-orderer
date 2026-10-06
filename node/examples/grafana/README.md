## Running Arma with Prometheus and Grafana

This README explains how to run the Arma sample network of [node/examples](../README.md)
together with Prometheus and Grafana, to watch its metrics on a dashboard while transactions
are submitted.

Grafana opens without a login, so this example is for local use only.

### Building the Docker Container
To build the Docker container, run the following command from the root directory:
```
(cd node/examples; bash ./scripts/build_docker.sh)
```

### Run Arma with Grafana
To run the example, run the following command from the root directory:
```
./node/examples/grafana/scripts/run_dashboard.sh
```
The `run_dashboard.sh` script performs the following tasks:
- Generates a configuration file for each node using `armageddon generate`.
- Starts the Arma network together with Prometheus and Grafana.
- Submits transactions using `armageddon submit`, which processes 300000 transactions at a rate of 1000 per second.
- Prints the dashboard link and opens it in a browser.

To change the defaults, set `RATE`, `TX_SIZE`, `DURATION_SECONDS` or `OPEN_BROWSER=false`, for example:
```
RATE=500 DURATION_SECONDS=60 ./node/examples/grafana/scripts/run_dashboard.sh
```

### Clean Up Sample
To clean up the environment, run the following command from the root directory:
```
./node/examples/grafana/scripts/clean_dashboard.sh
```
This script stops and removes the Docker containers and volumes, and deletes temporary files.
Dashboard edits made in Grafana are lost on clean-up, so export the dashboard JSON over
[grafana/arma-dashboard.json](grafana/arma-dashboard.json) to keep them.
