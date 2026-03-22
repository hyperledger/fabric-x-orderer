# 4 Parties 1 Shard - Single Container Arma

## Build & Run

```bash
cd node/examples/4parties1shard/scripts
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
- Router: 6022, 6122, 6222, 6322
- Assembler: 6023, 6123, 6223, 6323

## Storage

Host:
```
/tmp/arma-4parties1shard/storage/partyX/<role>
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
cd node/examples/4parties1shard/scripts
bash run.sh
```