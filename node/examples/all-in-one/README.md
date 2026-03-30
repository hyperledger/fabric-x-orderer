# All-in-One Arma Example (4 Parties, 1 Shard)

## Overview

This example builds a single Docker image that runs a full Arma network consisting of 4 parties and 1 shard within a single container.

Each party includes:
- Router
- Batcher
- Consenter
- Assembler

The container is responsible for:
- running the Arma services
- executing the test using `armageddon load` and `armageddon receive`

---

## Architecture

- Docker container = ARMA network + client (armageddon)
- Host machine = orchestration only (scripts)

All ARMA components run inside the same container, therefore communication via `127.0.0.1` replaces hostname-based networking.
All communication is done via **localhost + ports**, without DNS or `/etc/hosts`.

---

## Prerequisites

- Docker installed
- Linux environment (tested on RHEL)

---

## Build and Run

From the repository root:

```bash
cd node/examples/all-in-one/scripts
bash clean_sample.sh
bash build_docker.sh
bash run_sample.sh
```

What the scripts do:

- **build_docker.sh**
  - builds the Docker image
  - compiles `arma` and `armageddon`

- **clean_sample.sh**
  - stops and removes the container
  - removes `/tmp/arma-all-in-one`

- **run_sample.sh**
  - starts the container
  - waits for configuration and services
  - runs a test using:
    - one `armageddon receive`
    - one `armageddon load`

---

## What Happens

- `armageddon generate` creates configuration under `/tmp/arma-all-in-one`
- configuration is patched with:
  - party-specific ports
  - storage paths
  - listen address set to `0.0.0.0`
- 16 ARMA processes run inside the container:
  - 4 Routers
  - 4 Assemblers
  - 4 Batchers
  - 4 Consenters
- all communication is done using **127.0.0.1 + ports**
- no hostname resolution is required

---

## Networking Model

Each party uses fixed ports:

| Component   | Party 1 | Party 2 | Party 3 | Party 4 |
|------------|--------|--------|--------|--------|
| Router     | 6022   | 6122   | 6222   | 6322   |
| Assembler  | 6023   | 6123   | 6223   | 6323   |
| Batcher    | 6024   | 6124   | 6224   | 6324   |
| Consenter  | 6025   | 6125   | 6225   | 6325   |

Ports are exposed from the container to the host to allow the test client (armageddon) to connect.

All endpoints use:

```
127.0.0.1:<port>
```

No `/etc/hosts` configuration is needed.

---

## TLS Configuration

mTLS is enabled:

```yaml
UseTLSRouter: "mTLS"
UseTLSAssembler: "mTLS"
```

The system runs fully with TLS enabled using localhost endpoints.

---

## Storage

Each component persists its ledger and state under its respective storage directory.

Host path:

```
/tmp/arma-all-in-one/storage/partyX/<role>
```

Container path:

```
/storage/partyX/<role>
```

Roles:
- router
- assembler
- batcher
- consenter

---

## Testing

The test is executed automatically inside the container.

Current test configuration:

- receiver from party 1
- loader connects via party 1 and distributes transactions across all parties
- 1000 transactions
- rate: 200 TPS
- tx size: 300 bytes

Equivalent commands executed inside container:

### Receiver
```bash
armageddon receive \
  --config=/tmp/arma-all-in-one/config/party1/user_config.yaml \
  --pullFromPartyId=1 \
  --expectedTxs=1000 \
  --output=/tmp/arma-all-in-one/logs/output1
```

### Loader
```bash
armageddon load \
  --config=/tmp/arma-all-in-one/config/party1/user_config.yaml \
  --transactions=1000 \
  --rate=200 \
  --txSize=300
```

---

## Expected Result

A successful run should end with:

```
1000 txs were expected and overall 1000 were successfully received
```

Manual verification:

```bash
wc -l /tmp/arma-all-in-one/logs/output1
```

Expected:

```
1000 /tmp/arma-all-in-one/logs/output1
```

---

## Logs

Logs are stored under:

```
/tmp/arma-all-in-one/logs/
```

Important files:

- loader.log
- receiver.log
- output1: contains received transactions (one line per transaction)

---

## Notes

- This setup is intended for local testing and debugging, not production deployment
- No `/etc/hosts` configuration is required
- All communication is done via localhost ports
- Loader may print connection closing or retry messages — this is expected
- Configuration is generated under:
```
/tmp/arma-all-in-one/config/
```

---

## Debugging

Check container:

```bash
docker ps | grep arma-4p1s
```

Check logs:

```bash
docker logs <container_id>
```

Check test logs:

```bash
cat /tmp/arma-all-in-one/logs/loader.log
cat /tmp/arma-all-in-one/logs/receiver.log
```

---

## Clean

```bash
bash clean_sample.sh
```

This will:

- stop the container
- remove it
- delete `/tmp/arma-all-in-one`