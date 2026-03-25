# All-in-One Arma Example (4 Parties, 1 Shard)

## Overview

This example builds a single Docker image that runs a full Arma network consisting of 4 parties and 1 shard within a single container.

Each party includes:
- Router
- Batcher
- Consenter
- Assembler

The container is responsible **only for running the Arma services**.

All client operations (`armageddon load` / `armageddon receive`) are executed **from the host machine**, not inside the container.

---

## Architecture

- Docker container = ARMA network (servers)
- Host machine = client (`armageddon`)

This separation allows realistic integration testing similar to real deployments.

---

## Prerequisites

- Docker installed
- Linux environment (tested on RHEL)
- `armageddon` binary available on the host

Before running the example, build `armageddon` and install it on the host:

```bash
make binary
cp ./bin/armageddon /usr/local/bin/
```

You can verify it is available with:

```bash
armageddon version
```

---

## Hostname Resolution (Required)

The generated configuration uses internal hostnames such as:

- router.p1
- assembler.p1

You must map them on the host:

```bash
sudo bash -c 'cat >> /etc/hosts <<EOF
127.0.0.1 assembler.p1 router.p1 consensus.p1 batcher1.p1
127.0.0.1 assembler.p2 router.p2 consensus.p2 batcher1.p2
127.0.0.1 assembler.p3 router.p3 consensus.p3 batcher1.p3
127.0.0.1 assembler.p4 router.p4 consensus.p4 batcher1.p4
EOF'
```

This is required only once per machine.

---

### Build and Run

From the repository root:

```bash
cd node/examples/all-in-one/scripts
bash clean.sh
bash run.sh
```

What run.sh does:

- builds the Docker image
- starts the all-in-one container
- waits for the generated configuration and exposed ports to become ready
- runs a host-side test using:
  - one armageddon receive
  - one armageddon load

---

## What Happens

- armageddon generates Arma configuration under /tmp/arma-all-in-one
- the generated configs are patched with:
  - party-specific ports
  - external storage paths
  - listen address set to 0.0.0.0
- 16 ARMA processes are started inside one container:
  - 4 Routers
  - 4 Assemblers
  - 4 Batchers
  - 4 Consenters
- only router and assembler ports are exposed to the host
- the test is executed from the host, not inside the container

--- 

## Exposed Ports

Per party:

- Party 1
  - Router: 6022
  - Assembler: 6023
- Party 2
  - Router: 6122
  - Assembler: 6123
- Party 3
  - Router: 6222
  - Assembler: 6223
- Party 4
  - Router: 6322
  - Assembler: 6323

---

## Storage

Host path:

```
/tmp/arma-all-in-one/storage/partyX/<role>
```

Container path:

```
/storage/partyX/<role>
```

Example roles:

- router
- assembler
- batcher
- consenter

This creates 16 storage folders in total.

---

## Testing

The test is executed from the host.

run.sh already performs the basic validation automatically after starting the container.

The current test uses:

- one receiver from party 1
- one loader through party 1
- 1000 transactions
- rate 200
- tx size 300

Equivalent host-side commands are:

### Receiver
```bash
armageddon receive \
  --config /tmp/arma-all-in-one/config/party1/user_config.yaml \
  --pullFromPartyId=1 \
  --expectedTxs=1000 \
  --output=/tmp/arma-all-in-one/logs/output1
```

### Loader
```bash
armageddon load \
  --config /tmp/arma-all-in-one/config/party1/user_config.yaml \
  --transactions=1000 \
  --rate=200 \
  --txSize=300
```

---

### Expected Result

A successful run should end with the receiver reporting that all expected transactions were received.

Expected receiver message:

```
1000 txs were expected and overall 1000 were successfully received
```

The script also prints test results at the end.

You can verify manually:

```bash
wc -l /tmp/arma-all-in-one/logs/output1
```

Expected output:

```
1000 /tmp/arma-all-in-one/logs/output1
```

---

## Logs

Host-side test logs are written under:

```
/tmp/arma-all-in-one/logs/
```

Typical files include:

- loader.log
- receiver.log
- output1

---

## Notes

- The Docker container must remain running while the host-side test is executed.
- armageddon must be executed from the host, not inside the container.
- Loader may print connection closing or reconnection messages during shutdown. This is expected and does not necessarily indicate failure.
- Generated configuration files are stored under:
```
/tmp/arma-all-in-one/config/
```

---

## Debugging

Check that the container is running:

```bash
docker ps | grep arma-4p1s
```

Check container logs:

```bash
docker logs <container_id>
```

Check host-side logs:

```bash
cat /tmp/arma-all-in-one/logs/loader.log
cat /tmp/arma-all-in-one/logs/receiver.log
```

Check generated output:

```bash
wc -l /tmp/arma-all-in-one/logs/output1
```

---

## Clean

```bash
bash clean.sh
```

This will:

- stop the running container
- remove the container
- delete all data under /tmp/arma-all-in-one
