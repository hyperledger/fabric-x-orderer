# All-in-One Arma Example (4 Parties, 1 Shard)

## Overview

In this example, we build a single Docker image that runs a full Arma network consisting of 4 parties and 1 shard within a single container. Each party includes a router, batcher, consenter, and assembler process.

We start the container, dynamically generate the required configuration using `armageddon`, and patch it to expose only the router and assembler endpoints externally while mapping persistent storage for all components.

After the network is up, we run a basic test by submitting transactions to the routers using `armageddon load` and concurrently receiving them from each party using `armageddon receive`. This verifies end-to-end flow across the entire system.

## Build & Run

```bash
cd node/examples/all-in-one/scripts
bash run.sh
```

## What happens
- Generates Arma config
- Patches:
    - ports per party
    - storage paths (16 folders)
    - listen address
- Starts 16 processes inside ONE container
- Runs test:
    - submits 100 TXs
    - receives them from all 4 parties

## Exposed Ports
Ports mapping per party:

- Party 1:
  - Router:    6022
  - Assembler: 6023

- Party 2:
  - Router:    6122
  - Assembler: 6123

- Party 3:
  - Router:    6222
  - Assembler: 6223

- Party 4:
  - Router:    6322
  - Assembler: 6323

## Storage

Host:
```
/tmp/arma-all-in-one/storage/partyX/<role>
```

Mapped to:

```
/storage/partyX/<role>
```

## Clean
```bash
bash clean.sh
```

---

# 🚀 How to run (simple explanation)

From repo root:

```bash
cd node/examples/all-in-one/scripts
bash run.sh
```