# Arma Reconfiguration Guide

This guide explains how to create, sign, and submit a configuration update transaction to an Arma network.

---

## Table of Contents

1. [Overview](#overview)
2. [Step-by-Step: Manual Reconfiguration via configtxlator](#step-by-step-manual-reconfiguration-via-configtxlator)
3. [Signing Policy: Who Must Sign?](#signing-policy-who-must-sign)
4. [What Happens Inside Arma When a Config Transaction is submitted](#what-happens-inside-arma-after-a-config-transaction)
5. [Example: Remove a Party](#example-remove-a-party)
6. [Example: Add a Party](#example-add-a-party)
7. [Example: Update Endpoints](#example-update-endpoints)
8. [Example: Update Consensus Parameters (SmartBFT)](#example-update-consensus-parameters)
9. [Example: Update Batcher Parameters](#example-update-batcher-parameters)
10. [Example: Update TLS / Sign Certificates](#example-update-tls--sign-certificates)

---

## Overview

Arma uses the same reconfiguration model as Hyperledger Fabric. A configuration update is stored in a block and saved in the ledger. 
To change any aspect of the network (membership, endpoints, consensus parameters, certificates) an admin must:
1. Fetch the latest configuration block.
2. Decode it from protobuf to JSON.
3. Apply the desired changes in JSON.
4. Encode the modified JSON back to protobuf.
5. Compute a config update (diff between original and modified).
6. Wrap the config update in an envelope and collect the required admin signatures.
7. Broadcast the signed envelope to the system.

When a configuration transaction is submitted to Arma, the Arma nodes apply the new configuration and a new configuration block containing the updated configuration is written to the ledger.

---

## Step-by-Step: Manual Reconfiguration using `configtxlator`

### Prerequisites

Configuration updates are created using the `configtxlator` tool, which is part of the Hyperledger Fabric tools included in the `fabric-x-common` repository.
For more details about `configtxlator` see the [documentation](https://hyperledger-fabric.readthedocs.io/en/release-2.2/commands/configtxlator.html).
The steps below use `configtxlator` as a command-line tool.

Run from `fabric-x-common` and compile `configtxlator`:
```bash
mkdir bin
go build -o bin/configtxlator ./cmd/configtxlator
```

---

### Step 1 – Fetch the latest config block

Use the genesis block if it is the first reconfiguration.
If the network has already committed one or more config blocks, pull the latest one instead of using the genesis block.

// TODO: add a command to pull the last block?

### Step 2 – Decode the config block to JSON

```bash
configtxlator proto_decode \
  --input  bootstrap/bootstrap.block \
  --type   common.Block \
  --output config_block.json
```

---

### Step 3 – Extract the current `config` object

The config block JSON is deeply nested. The actual config lives at:

```
data.data[0].payload.data.config
```

Extract it with `jq`:

```bash
jq '.data.data[0].payload.data.config' config_block.json > config.json
```

Keep a copy of the original as a reference for computing the diff later:

```bash
cp config.json original_config.json
```

---

### Step 4 – Create `modified_config.json`

Copy `config.json` to `modified_config.json` and make your changes. 
Common change locations inside the JSON:

| Change                                      | JSON path                                                                                                                                                                                                |
|---------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Batch size - `MaxMessageCount`              | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.BatchingConfig.BatchSize.MaxMessageCount`                                                                                             |
| Batch timeout - `AutoRemoveTimeout`         | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.BatchingConfig.BatchTimeouts.AutoRemoveTimeout`                                                                                       |
| SmartBFT parameter - `RequestBatchMaxBytes` | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.ConsensusConfig.SmartBFTConfig.RequestBatchMaxBytes`                                                                                  |
| Party endpoint (router host/port)           | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.PartiesConfig[<idx>].RouterConfig.{Host,Port}`                                                                                        |
| Party TLS cert (router)                     | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.PartiesConfig[<idx>].RouterConfig.TlsCert`                                                                                            |
| Party TLS CA certs                          | `.channel_group.groups.Orderer.values.ConsensusType.value.metadata.PartiesConfig[<idx>].TLSCACerts`                                                                                                      |

// TODO: mention cases in which we must change some json paths.

---

### Step 5 – Encode both configs back to protobuf

```bash
configtxlator proto_encode \
  --input  original_config.json \
  --type   common.Config \
  --output original_config.pb

configtxlator proto_encode \
  --input  modified_config.json \
  --type   common.Config \
  --output modified_config.pb
```

---

### Step 6 – Compute the config update (delta)

```bash
configtxlator compute_update \
  --channel_id arma \
  --original   original_config.pb \
  --updated    modified_config.pb \
  --output     config_update.pb
```

---

### Step 7 – Wrap the config update in an envelope

A configuration update is submitted to the orderer inside an `Envelope`. 
The `Payload.Data` field contains a marshaled `ConfigUpdateEnvelope`, and the envelope is signed by the client.

```
Envelope
├── Payload
│   └── Data (marshaled ConfigUpdateEnvelope)
└── Signature (client signature)

The ConfigUpdateEnvelope has the following structure:

ConfigUpdateEnvelope
├── ConfigUpdate (config_update.pb)
└── Signatures []*ConfigSignature (admin signatures)
```

// TODO: add script ? use existing commands like: 
```bash
configtxlator proto_decode \
  --input  config_update.pb \
  --type   common.ConfigUpdate \
  --output config_update.json

configtxlator proto_encode \
  --input  config_update_in_envelope.json \
  --type   common.Envelope \
  --output config_update_in_envelope.pb
```

---

### Step 8 – Collect admin signatures

Each admin signs the `config_update.pb` bytes using their admin private key and appends a `ConfigSignature`.

// TODO: add commands ?
```bash

```

---

### Step 9 – Broadcast the signed envelope

Submit the `Envelope` to the ordering service, using the `Broadcast` API.

---

## Signing Policy: Who Must Sign?

Configuration transaction must be signed by the administrators authorized to request a configuration change.
Fabric determines who is authorized by evaluating the policy associated with the modified configuration.
For example, changing an ordering service parameter is typically governed by the `/Channel/Orderer/Admins` policy, 
which usually requires signatures from a majority of the ordering organizations' administrators.

---

## What Happens Inside Arma After a Config Transaction

1. **Reception** – Router nodes receive the signed config envelope via gRPC Broadcast.
2. **Signature verification** – The Router verifies each TX. If the verification failed the transaction is rejected immediately with an error.
   For example, if majority threshold is not met, you might see the following error:
   ```
   1 sub-policies were satisfied, but this policy requires 3 of the 'Admins' sub-policies to be satisfied
   ```
3. **Ordering** – The config envelope is forwarded to the Consenter and ordered like a regular transaction.
4. **Dynamic restart** – All Arma nodes pull from Consenter decisions and when a new config block is detected one of two things can happen:
   - Nodes **restart dynamically** in-process.
   - Nodes enter the **pending-admin** state and wait for an administrator to restart the process (for example, after updating the listen address, TLS certificates, or signing certificate files).
   // TODO: add the log
5. A new block containing the configuration is appended to the ledger.
**New config block as bootstrap** – Any party that needs to join the network after the change must use this config block as its bootstrap block.

---

## Reconfiguration Examples 

## Example: Remove a Party

This example removes **party 2** from a 5-party network.

Three sections must be updated **atomically in the same config update**, and should be changed in the config JSON:

1. **PartiesConfig** – remove the entry where `PartyID == 2`.
2. **ConsenterMapping** (`consenter_mapping`) – remove the entry where `id == 2`.
3. **Orderer groups** – delete the `org2` group from `.channel_group.groups.Orderer.groups`.
// TODO: make sure that other places are not missing

### flow
1. Stop the party to be removed.
2. Submit the config update to all Routers in the network.
3. Eventually, all nodes receive the config TX.
4. All nodes, except those of the removed party apply the new config and restart dynamically in-process.
5. Party 2 nodes enter a pending-admin state, and the operator admin must stop those processes manually.
   Remaining parties continue processing transactions after the restart, and reject connections from the removed party nodes as the new config no longer includes them.

---

// TODO: complete examples
## Example: Add a Party

## Example: Update Endpoints

## Example: Update Consensus Parameters

## Example: Update Consensus parameters (SmartBFT)

## Example: Update Batcher Parameters

## Example: Update TLS / Sign Certificates
