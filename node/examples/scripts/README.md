## Running Arma 

This README explains how to use the Armageddon tool to run Arma, which simulates four parties and two shards: a router, two batchers, a consensus and an assembler nodes.


### Building the Docker Container
To build the Docker container required for running the Arma sample, run the following command from the root directory:

```
(cd node/examples; bash ./scripts/build_docker.sh)
```

### Run Arma Sample
To run the Arma example, run the following command from the root directory:
```
(cd node/examples; bash ./scripts/run_sample.sh)
```
The `run_sample.sh` script performs the following tasks:
- Generates a configuration file for each node using `armageddon generate`.
- Creates and manages a volume for configuration files, with each node running in its own container.
- Submits transactions using `armageddon submit`, which processes 1000 transactions at a rate of 500 per second. 