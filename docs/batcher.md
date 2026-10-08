<!--
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
-->
# Batcher

*Audience: ordering-service operators, contributors to the batcher implementation, and operators of
the other Arma roles, which send transactions to the batcher, order its attestations, and pull its
batches.*

This document describes the **batcher**, the Fabric-X Orderer node that turns transactions into
durable, replicated batches. It covers the interfaces the batcher serves and uses, its internal design,
and its configuration, recovery, and failure behavior. It focuses on the batcher; for the end-to-end
system flow and how the batcher relates to the other three roles, start with the
[architecture overview](architecture.md).

1. [Overview](#1-overview)
    1. [Units and Terms](#11-units-and-terms)
2. [Interfaces](#2-interfaces)
3. [Design and Internal Architecture](#3-design-and-internal-architecture)
    1. [Units and Wiring](#31-units-and-wiring)
    2. [Primary and Secondaries](#32-primary-and-secondaries)
    3. [Selecting the Primary](#33-selecting-the-primary)
    4. [The Memory Pool and Censorship Resistance](#34-the-memory-pool-and-censorship-resistance)
    5. [The Batch Ledger Array](#35-the-batch-ledger-array)
    6. [Code Map](#36-code-map)
4. [Configuration](#4-configuration)
5. [Metrics and Monitoring](#5-metrics-and-monitoring)
6. [Failure and Recovery](#6-failure-and-recovery)
    1. [Restarting from Local State](#61-restarting-from-local-state)
    2. [When Other Nodes Fail](#62-when-other-nodes-fail)
7. [Reconfiguration](#7-reconfiguration)
8. [Deployment Guidance](#8-deployment-guidance)
9. [Further Reading](#9-further-reading)

## 1. Overview

The **batcher** ([`node/batcher`](../node/batcher)) is the second stage of the Arma pipeline
(**Router → Batcher → Consenter → Assembler**), and the node that carries the transaction payloads.
Batchers are grouped into **shards**, and every party runs one batcher in every shard. A batcher
receives transactions from the router of its own party, bundles them into **batches**, persists the
batches, and replicates them among the batchers of its shard. For each batch it stores, it sends the
consenters a **batch attestation fragment (BAF)**: a small signed message with the batch's digest
and metadata, stating that the batch is stored. The payload itself never enters consensus; it stays in
the batchers until an assembler pulls it to build a block.

Within a shard, one batcher is the **primary** and creates the batches; the others are
**secondaries**, which pull each batch from the primary, verify it, store it, and attest it. Which
batcher is primary is decided by the consenters, and the secondaries are what keep the primary honest:
a secondary that sees its requests ignored, or receives an invalid batch, complains to the consenters,
and enough complaints replace the primary.

<!-- Figure 1 placeholder: from slide 6 of the batcher tech-transfer deck. -->
*Figure 1: The batcher's inputs and outputs — transactions from the router of its own party, batches
pulled by the other batchers of its shard and by the assemblers, BAFs and complaints sent to every
consenter, and decisions pulled from the consenter of its own party. (Diagram to be added.)*

### 1.1 Units and Terms

The system-wide terms the batcher is defined in — party, shard, primary, batch, BAF and decision —
are the ones listed in the [data model](architecture.md#5-data-model) of the architecture overview.

**Units**

- **Memory pool (mempool)** — the batcher's store of requests that are not yet in a batch. On the
  primary it is where batches are cut from; on a secondary it tracks requests until they show up in a
  batch from the primary.
- **Batch ledger array** — the batcher's durable store of batches, with one ledger per party of the
  shard.
- **Batcher role** — the loop that acts as primary or as secondary, and switches between the two
  when the primary changes.

**Terms**

- **Term** — a period in which one batcher is the shard's primary. The consenters keep the current
  term of every shard, and incrementing a shard's term rotates its primary.
- **Batch sequence** — the position of a batch among the batches of one primary. A batch is
  identified by its ⟨shard, primary, sequence⟩.
- **Ack** — a secondary's notice to the primary that it has stored a given batch sequence.
- **Complaint** — a signed message from a batcher to the consenters, suspecting its shard's primary of
  censorship or failure.
- **First and second strike** — the two timeouts after which a secondary that holds a request not yet
  batched first forwards it to the primary, then complains.

## 2. Interfaces

The batcher is an internal node: it does not face clients. It serves three gRPC services, used by its
router, by the other batchers of its shard, and by the assemblers, and it calls the services of the
consenters. All of this traffic is over mutual TLS. The Arma services are defined in
[`communication.proto`](../node/protos/comm/communication.proto); the [APIs](api.md) document lists
them in full.

**Served by the batcher**

- `RequestTransmit` (`Submit`, `SubmitStream`) — the router's path. Only the router of the batcher's
  own party is served, checked by its TLS certificate. The router has already verified the request,
  so the batcher only checks that it was routed under the batcher's current config sequence, then
  re-encodes it as a Fabric `Envelope` and submits it to the mempool.
- `BatcherControlService` — the path between the batchers of one shard, served by the primary:
  `NotifyAck` receives the secondaries' acks, and `FwdRequestStream` receives requests a secondary
  forwards after the first strike. A forwarded request is verified before it enters the primary's
  mempool.
- `Deliver` — Fabric's Atomic Broadcast `Deliver` service, reading the batch ledger array. The
  secondaries of the shard pull the primary's batches through it, and the assemblers pull batches
  from it to build blocks. `Broadcast` is not served.

**Used by the batcher**

- `Consensus.NotifyEvent`, on every consenter — to send BAFs and complaints. Each is sent to all
  consenters and retried, with back-off, until at least `N−f` have accepted it.
- `Deliver`, on the consenter of its own party — to pull the decision stream. A decision carries the
  current term of every shard, which is how the batcher learns who the primary is, and it may carry a
  config block.
- `Consensus.AckConfig` — to tell the consenter that it has taken a new configuration (see
  [Reconfiguration](#7-reconfiguration)).

## 3. Design and Internal Architecture

The whole batcher is the loop below: find out who the primary is, then act as primary or as secondary
until the term changes.

```
1  loop
2     term    ← term of this shard in the latest decision;
3     primary ← batchers[(shard + term) mod N];
4     seq     ← L.Height(primary);              // the next batch sequence of that primary
5     if primary = self then
6        runPrimary(seq)                         // until the term changes
7     else
8        runSecondary(primary, seq)              // until the term changes
9     end
10 end
```

### 3.1 Units and Wiring

Around the **batcher role** and the **mempool** run a few units. A **decision puller** follows the
decision stream of the consenter of its own party; it writes each decision header to a write-ahead log
(WAL), hands the latest state to the batcher role, and stores any config block in the **config store**.
A **control-event broadcaster** sends BAFs and complaints to all consenters. Two **primary connectors**
keep a stream open to the current primary, one for acks and one for forwarded requests, and reconnect
when the primary changes. A **batch puller** streams batches from the primary, and a **request
verifier** checks requests coming from the router, from other batchers, and inside pulled batches. The
**batch ledger array** is written by the batcher role and read by the `Deliver` service. Around these
run the operations subsystem, the metrics tracker, and a health checker.

<!-- Figure 2 placeholder: from slide 31 of the batcher tech-transfer deck. -->
*Figure 2: The units of a primary and a secondary batcher, and how batches, BAFs, acks, forwarded
requests, complaints and term changes flow between them and the consenters. (Diagram to be added.)*

### 3.2 Primary and Secondaries

The **primary** creates the shard's batches:

```
runPrimary(seq):
1  loop
2     wait until seq − confirmedSeq < BatchSequenceGap;   // secondaries keep up
3     batch  ← M.NextRequests();         // when the batch is full, or after BatchCreationTimeout
4     baf    ← sign(shard, self, seq, digest(batch));
5     L.Append(self, seq, batch, baf.signature);
6     send baf to all consenters;
7     M.Remove(batch);  seq ← seq + 1;
8  end
```

A **secondary** follows the primary:

```
runSecondary(primary, seq):
1  foreach batch pulled from primary, starting at seq do
2     if not verify(batch) then complain; restart pulling;
3     L.Append(primary, seq, batch, batch.primarySignature);
4     baf ← sign(shard, primary, seq, digest(batch));
5     send baf to all consenters;
6     M.Remove(batch);  ack(seq) to primary;  seq ← seq + 1;
7  end
```

The primary's BAF signature is also its signature over the batch, and it is stored and served with the
batch. A secondary verifies a pulled batch before storing it: the batch must come from the expected
primary, shard and sequence, be non-empty, match its digest, carry a valid primary signature, and
contain only valid requests. Requests already in the secondary's mempool were verified when they
entered it and are not verified again.

The batchers send their BAFs only after the batch is stored, which is what a BAF attests. Once the
consenters receive `f+1` BAFs for a batch, they order a **batch attestation**, and at least one correct
batcher is then known to hold the batch.

The acks bound how far the primary can run ahead of the secondaries. The primary counts its own batch
as acked, and a sequence is confirmed once `f+1` batchers have acked it. The primary does not cut a
new batch while it is `BatchSequenceGap` or more sequences ahead of the last confirmed one.

<!-- Figure 3 placeholder: from slides 7-15 of the batcher tech-transfer deck. -->
*Figure 3: One batch in a shard of four parties — the primary cuts it from its mempool, stores it, and
sends a BAF; each secondary pulls, verifies, stores it, sends its own BAF, and acks. (Diagram to be
added.)*

### 3.3 Selecting the Primary

The batchers of a shard are ordered by party ID, and the primary of the shard in a given term is

```
primaryIndex = (shard + term) mod N
primary      = batchers[primaryIndex]
```

where `N` is the number of parties. Every batcher reads the term from the same ordered decisions, so
all of them agree on the primary at the same point. When the term changes, the batcher role stops what
it is doing — cutting batches, or pulling them — and starts again with the new primary.

A term change can leave a batch with fewer than `f+1` BAFs: attested by some batchers but never
ordered. Its requests would be lost if every batcher that holds them had already removed them from its
mempool. So on a term change, each batcher looks for its own BAFs that the consenters still hold as
pending for the previous primary, reads those batches from its ledger, and returns their requests to
its mempool.

### 3.4 The Memory Pool and Censorship Resistance

The mempool works in one of two modes, chosen by the batcher's role:

- On the **primary**, it is a **batch store**. Incoming requests are grouped into batches of up to
  `MaxMessageCount` requests and `AbsoluteMaxBytes` bytes, and the primary takes the next batch from
  it.
- On a **secondary**, it is a **pending store**. Requests wait until a batch from the primary contains
  them, and a timer runs for each.

A router forwards each transaction to the batcher of its own party in the shard, and a client that
wants censorship resistance submits to the routers of several parties. A secondary therefore usually
holds the same requests as the primary, and the pending store is how it notices when the primary
leaves one out:

1. **First strike** — a request still pending after `FirstStrikeThreshold` is forwarded to the
   primary, in case the primary never received it.
2. **Second strike** — a request still pending `SecondStrikeThreshold` after that makes the secondary
   send a **complaint** to the consenters.
3. **Term change** — when `f+1` batchers of the shard complain in the same term, the consenters
   increment the term, and the next batcher becomes primary.

A secondary also complains when a batch it pulled fails verification. The consenter side of this is
described in [The Consenter as System Controller](consensus.md#34-the-consenter-as-system-controller).

<!-- Figure 4 placeholder: from slides 23-27 of the batcher tech-transfer deck. -->
*Figure 4: A primary censoring a request — the secondaries forward it after the first strike, complain
after the second, and the consenters change the term. (Diagram to be added.)*

The mempool holds at most `MemPoolMaxSize` requests. When it is full, a new request waits up to
`SubmitTimeout` for space and is then rejected, which pushes back on the router.

### 3.5 The Batch Ledger Array

Every batcher can be primary in some term, so a batcher stores batches from every party of its shard.
The batch ledger array holds one ledger per party, and a batch is appended to the ledger of the primary
that created it, at its batch sequence. Each ledger is served as its own `Deliver` channel. A
secondary pulls from the primary's ledger starting at its own height for that party, and an assembler
pulls from the ledger the batch attestation names.

<!-- Figure 5 placeholder: from slides 16-17 of the batcher tech-transfer deck. -->
*Figure 5: The batch ledger array of one batcher in a shard of four parties — one ledger per party,
each holding the batches that party created while it was primary. (Diagram to be added.)*

### 3.6 Code Map

| Concept | Source |
|---------|--------|
| Node lifecycle, RPC handlers, decision pulling, applying a new config | [`batcher.go`](../node/batcher/batcher.go), [`batcher_builder.go`](../node/batcher/batcher_builder.go) |
| Primary and secondary loops, primary selection, resubmitting requests | [`batcher_role.go`](../node/batcher/batcher_role.go) |
| Acks and the sequence gap | [`acker.go`](../node/batcher/acker.go) |
| Pulling and serving batches | [`puller.go`](../node/batcher/puller.go), [`batcher_deliver_service.go`](../node/batcher/batcher_deliver_service.go) |
| Request and batch verification | [`requests_inspector_verifier.go`](../node/batcher/requests_inspector_verifier.go) |
| Sending BAFs and complaints to the consenters | [`control_event_broadcaster.go`](../node/batcher/control_event_broadcaster.go), [`consenter_control_event_sender.go`](../node/batcher/consenter_control_event_sender.go) |
| Acks and forwarded requests to the primary | [`primary_ack_connector.go`](../node/batcher/primary_ack_connector.go), [`primary_req_connector.go`](../node/batcher/primary_req_connector.go) |
| Memory pool: batch store, pending store, strikes | [`request/`](../request) |
| Batch ledger array | [`node/ledger/batch_ledger_array.go`](../node/ledger/batch_ledger_array.go) |
| Metrics | [`metrics.go`](../node/batcher/metrics.go) |

## 4. Configuration

A batcher is configured like every other node: a local YAML file for what this node is and where it
keeps its state, and the shared configuration for what the network is, which it obtains from a config
block. The sections common to all roles — `General`, `FileStore`, `Operations` and `Metrics` — are
documented field by field in the commented [`local_config.yaml`](../config/sample/local_config.yaml),
and the [configuration model](architecture.md#6-configuration-and-membership) of the architecture
overview explains how local and shared configuration fit together. `FileStore.Location` is the
directory the batch ledger array, the WAL, and the config store are written under.

The role-specific section is `Batcher`:

```yaml
Batcher:
  ShardID: 1
  BatchSequenceGap: 10
  MemPoolMaxSize: 1000000
  SubmitTimeout: 500ms
```

| Field | Default | What it controls |
|-------|---------|------------------|
| `ShardID` | — | The shard this batcher belongs to. |
| `BatchSequenceGap` | `10` | How many sequences the primary may run ahead of the last batch that `f+1` batchers have acked. |
| `MemPoolMaxSize` | `1000000` | The most requests the mempool holds. |
| `SubmitTimeout` | `500ms` | How long a request waits for space in a full mempool before it is rejected. |

The batching parameters must be the same on every batcher, so they are part of the shared
configuration, in the `Batching` block:

```yaml
Batching:
  BatchTimeouts:
    BatchCreationTimeout: 500ms
    FirstStrikeThreshold: 10s
    SecondStrikeThreshold: 10s
    AutoRemoveTimeout: 10s
  BatchSize:
    MaxMessageCount: 10000
    AbsoluteMaxBytes: 10485760
```

| Field | What it controls |
|-------|------------------|
| `BatchCreationTimeout` | The longest the primary waits for a batch to fill before it cuts a smaller one. |
| `FirstStrikeThreshold` | How long a secondary holds a request not yet batched before forwarding it to the primary. |
| `SecondStrikeThreshold` | How long after the first strike a secondary waits before complaining about the primary. |
| `AutoRemoveTimeout` | How long a request may stay in the mempool after the second strike before it is removed. |
| `MaxMessageCount`, `AbsoluteMaxBytes` | The most requests, and bytes, in one batch. |

Templates can be found under [`config/sample`](../config/sample):
[`local_config_batcher.yaml`](../config/sample/local_config_batcher.yaml) for this role, and
[`shared_config.yaml`](../config/sample/shared_config.yaml) for the network-wide part.

## 5. Metrics and Monitoring

A batcher exposes its metrics in Prometheus format on the operations endpoint configured by
`Operations.ListenAddress` and `Operations.ListenPort`, alongside a health check, a logging-spec
endpoint, and version information. Its metrics are labelled with the party ID and the shard ID; the
most useful are:

| Metric | What it tells you |
|--------|-------------------|
| `batcher_current_role` | `1` while the batcher is primary, `2` while it is secondary. |
| `batcher_role_changes_total` | How many times the primary has changed. A rising count means the shard keeps rotating its primary. |
| `batcher_mempool_size` | How many requests are waiting in the mempool. A mempool that keeps growing on a secondary means the primary is not batching them. |
| `batcher_router_txs_total`, `batcher_batched_txs_total` | The transactions received from the router, and those stored in batches. Their rates should match. |
| `batcher_batches_pulled_total` | How many batches a secondary has pulled and verified. |
| `batcher_first_resends_total`, `batcher_complaints_total` | How many requests were forwarded after the first strike, and how many complaints were sent. These are the early signals of a primary in trouble. |

The full list, including the latency histograms of batch cutting, hashing and verification, is in the
[monitoring and metrics guide](monitoring/metrics.md).

## 6. Failure and Recovery

### 6.1 Restarting from Local State

A batcher's durable state is its batch ledger array, its WAL of decision headers, and its config
store. On restart it reads the last decision number from the WAL and resumes pulling decisions from
there, and it takes its configuration from the last config block in the config store. The batch
sequence it resumes from is the height of the current primary's ledger, so a primary continues after
its last stored batch, and a secondary pulls from where it stopped. The mempool is not persisted, so
the requests it held that were not yet in a stored batch are lost on this batcher; a client that
submitted to the routers of several parties still has them in the other batchers' mempools.

### 6.2 When Other Nodes Fail

The shard tolerates up to `f` faulty batchers out of `N ≥ 3f+1`.

- **A primary that crashes or censors** leaves the secondaries' requests unbatched. They forward them,
  then complain, and the term changes to a new primary ([section 3.4](#34-the-memory-pool-and-censorship-resistance)).
- **A primary that creates an invalid batch** is caught by the secondaries' verification. They refuse
  to store or attest it, and complain.
- **A slow or failed secondary** does not stop the shard, as long as `f` secondaries keep acking. If
  fewer do, the primary stops cutting batches once it is `BatchSequenceGap` ahead.
- **A failed consenter** does not stop the batcher either, since a BAF only has to reach `N−f`
  consenters. If fewer are reachable, the batcher keeps retrying and does not move on to the next
  batch.

## 7. Reconfiguration

A new configuration reaches the batcher as a decision whose last block is a config block. The batcher
adds the block to its config store, **soft-stops**, which pauses the batcher role and the decision
puller while keeping the mempool, and acknowledges the new configuration sequence to the consenter.

Some changes cannot be applied by the running node. If the party is evicted, if the batcher's own
identity changes (its endpoint or certificates), or, in the current version, if the batching
parameters change, the batcher enters the **pending-admin** state and waits for an administrator.
Anything else it applies in the running process: it rebuilds its configuration and services against
the new topology, keeps its mempool but removes the requests that no longer pass verification under
the new configuration, and starts again.

Batches attested under the previous configuration may not have been ordered. When a decision reports
such a BAF of this batcher, the batcher returns the batch's requests to its mempool, verifying each
under the new configuration, so they are batched again.

For the configuration model this rests on, see
[Configuration and Membership](architecture.md#6-configuration-and-membership).

## 8. Deployment Guidance

<!-- Section 8 placeholder -->
*To be written: what to provision a batcher with — the storage the batch ledger array needs and how
fast it grows; the memory the mempool needs; and network considerations for replication within a
shard and for the assemblers that pull batches.*

## 9. Further Reading

- [Architecture overview](architecture.md) — the four roles, the end-to-end flow, and the
  configuration model.
- [APIs](api.md) — the gRPC services of every role, and which role implements which.
- [Consenter](consensus.md) — how BAFs become batch attestations, and how complaints rotate a primary.
- [Assembler](assembler.md) — how batches are pulled from the batchers and turned into blocks.
- [Monitoring and metrics](monitoring/metrics.md) — the full list of metrics, and how to collect and
  visualize them.
- [`arma` CLI](cli/arma.md) — the node binary and its subcommands.
- [`node/batcher`](../node/batcher) — the source.
