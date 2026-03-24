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
- Host machine = client (armageddon)

This separation allows realistic integration testing similar to real deployments.

---

## Prerequisites

- Docker installed
- Linux environment (tested on RHEL)
- `armageddon` binary available on host (same version as built in the image)

---

## ⚠️ Hostname Resolution (REQUIRED)

The generated configuration uses internal hostnames like:

- `router.p1`
- `assembler.p1`

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

## Build & Run

```bash
cd node/examples/all-in-one/scripts
bash run.sh
```

## What Happens
- Generates Arma configuration using armageddon
- Patches:
  - ports per party
  - storage paths (16 folders)
  - listen address (0.0.0.0)
- Starts 16 ARMA processes inside ONE container:
  - 4 Routers
  - 4 Assemblers
  - 4 Batchers
  - 4 Consenters
- Exposes Router and Assembler ports to the host
- Waits for external clients (armageddon) to connect

## Exposed Ports

Per party:
- Party 1:
  - Router: 6022
  - Assembler: 6023
- Party 2:
  - Router: 6122
  - Assembler: 6123
- Party 3:
  - Router: 6222
  - Assembler: 6223
- Party 4:
  - Router: 6322
  - Assembler: 6323

## Storage

Host:

```
/tmp/arma-all-in-one/storage/partyX/<role>
```

Container:

```
/storage/partyX/<role>
```

--
## 🧪 Testing (Run From Host)

After starting the container, wait ~15 seconds for all services to initialize.

✅ Single Loader + Single Receiver (Recommended)

1. Start Receiver (FIRST)

``` bash
./armageddon receive \
  --config /tmp/arma-all-in-one/config/party1/user_config.yaml \
  --pullFromPartyId=1 \
  --expectedTxs=1000 \
  --output=/tmp/output1
  ```

  2. Start Loader

``` bash
./armageddon load \
  --config /tmp/arma-all-in-one/config/party1/user_config.yaml \
  --transactions=1000 \
  --rate=200 \
  --txSize=300
```

✅ Expected Result

Receiver output:

``` 1000 txs were expected and overall 1000 were successfully received ```

Verify manually:

``` bash
wc -l /tmp/output1
```

Expected:

1000

--

ℹ️ Notes

- The Docker container must remain running during tests
- Loader may print connection closing messages — this is expected
- armageddon must be executed from the host (not inside the container)
- Configuration files are generated under:
```
/tmp/arma-all-in-one/config/
```

## Clean

```bash
bash clean.sh
```

This will:
- Stop the container
- Remove it
- Delete all storage under /tmp/arma-all-in-one
