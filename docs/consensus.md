<!--
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
-->
# Consenter

*Audience: ordering-service operators, contributors to the consensus implementation, and operators
of the other Arma roles, which submit events to the consenter and consume the ordered stream it
produces.*

This document describes the **consenter**, the Fabric-X Orderer node that runs Byzantine fault
tolerant consensus. It covers the events the consenter ingests and the ordered stream it serves, the
internal design and algorithms, and the configuration,
recovery, and failure behavior. It focuses on the consenter; for the
end-to-end system flow and how the consenter relates to the other three roles, start with the
[architecture overview](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/architecture.md).

1. [Overview](#1-overview)
    1. [Units and Terms](#11-units-and-terms)
2. [Interfaces: Ingesting Events and Serving Decisions](#2-interfaces-ingesting-events-and-serving-decisions)
3. [Design and Internal Architecture](#3-design-and-internal-architecture)
    1. [Units and Wiring](#31-units-and-wiring)
    2. [The Deterministic State Machine](#32-the-deterministic-state-machine)
    3. [From BAFs to a Batch Attestation](#33-from-bafs-to-a-batch-attestation)
    4. [The Consenter as System Controller](#34-the-consenter-as-system-controller)
    5. [Building a Decision](#35-building-a-decision)
    6. [SmartBFT as the Engine](#36-smartbft-as-the-engine)
    7. [Code Map](#37-code-map)
4. [Configuration](#4-configuration)
5. [Metrics and Monitoring](#5-metrics-and-monitoring)
6. [Failure and Recovery](#6-failure-and-recovery)
    1. [Restarting from Local State](#61-restarting-from-local-state)
    2. [Catching Up from Other Consenters](#62-catching-up-from-other-consenters)
    3. [When Other Nodes Fail](#63-when-other-nodes-fail)
7. [Reconfiguration](#7-reconfiguration)
8. [Deployment Guidance](#8-deployment-guidance)
9. [Further Reading](#9-further-reading)

## 1. Overview

The **consenter** ([`node/consensus`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus))
is the party-local node that establishes total order, and the third stage of the Arma pipeline
(**Router → Batcher → Consenter → Assembler**). It is where the parties agree, under Byzantine fault
tolerance, on the sequence of everything that reaches the ledger. What it orders is deliberately
small: **control events** — a signed **batch attestation fragment (BAF)** from a batcher, a
**complaint** against a shard's primary, or a **configuration request** from the router — never
data transaction payloads, which travel the parallel batcher–assembler path. The consenter feeds these
events into [SmartBFT](https://github.com/hyperledger/SmartBFT), the BFT engine the consenters of all
parties run together, and interprets the ordered result: once enough batchers have attested the same
batch, it emits a **batch attestation (BA)**, and its output is a totally ordered stream of BAs and
configuration decisions that every correct consenter produces identically.

Beyond ordering, the consenter is the system's **controller**. Because it sees every attestation and
every complaint, it is the node that decides when a shard's primary batcher must be replaced — on
enough complaints, or on proof that a primary equivocated — and it enacts that ruling as part of
the same ordered stream, so all parties rotate the primary at the same point in the order.

<!-- Figure 1 placeholder -->
*Figure 1: The consenter's inputs and output — batch attestation fragments and complaints from the
batchers of every party, configuration requests from the router of its own party, SmartBFT messages
exchanged with the consenters of the other parties, and the ordered decision stream served to the
assembler, batchers, and routers. (Diagram to be added.)*

### 1.1 Units and Terms

A small number of units do the consenter's work, on the state and data named below; the units around
them are named in [Units and Wiring](#31-units-and-wiring). The system-wide terms the consenter is
defined in — party, shard, primary, batch, batchID, BAF, BA and decision — are the ones listed in the
[data model](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/architecture.md#5-data-model)
of the architecture overview.

**Units**

- **SmartBFT engine** — the Byzantine fault tolerant protocol the consenters of all parties run
  together. It agrees on the order of opaque request bytes and on when a batch of them is committed;
  the consenter treats it as its ordering engine and provides the Arma-specific meaning of those
  bytes (see [SmartBFT as the Engine](#36-smartbft-as-the-engine)).
- **State machine** — the deterministic, replicated logic layered on top of SmartBFT. It reads each
  ordered request as a control event and folds it into a `State` that every correct consenter
  computes identically, so the derived facts — which batches are attested, which primaries must
  rotate — need no separate agreement.
- **Batch-attestation DB (BADB)** — the durable set of batch digests whose attestation has already
  been ordered. It is what lets the consenter drop a fragment for an already-ordered batch instead of
  ordering it twice.
- **Consensus ledger** — the consenter's durable output: the decision blocks it has committed, on
  local storage. It is also the state a restarting consenter recovers from.

**Terms**

- **Control event** — the unit SmartBFT orders. Exactly one of a BAF, a complaint, or a
  configuration request; on the wire each is a serialized `ControlEvent`.
- **BAF** and **batch attestation (BA)** — a BAF is one signer's signed attestation that it persisted
  a batch with a given digest and metadata; a BA is the aggregate formed once enough distinct signers
  have attested the same digest.
- **Threshold** and **quorum** — derived from the party count `N` as `f = (N-1)/3`,
  `threshold = f+1`, and a `quorum` majority. A batch becomes a BA once `threshold` distinct signers
  attest the same digest — `f+1` guarantees at least one correct attester.
- **Shard** and **term** — each shard has a current *term*, and the ⟨shard, term⟩ pair selects the
  shard's primary batcher. Incrementing a shard's term rotates its primary.
- **Complaint** — a signed vote from a batcher that suspects its shard's primary of censorship or
  failure, carried as a control event.
- **Equivocation** — a primary signing two different digests for the same ⟨shard, primary, sequence⟩.
  Because the primary signs every batch, this is self-proving and, like complaints, rotates the
  primary.
- **Decision** — one SmartBFT agreement output: a proposal plus a quorum of signatures. A decision
  carries a header, one pre-built block per BA it commits (plus a configuration block if a
  configuration request was decided), and a snapshot of the post-decision `State`.
- **Ordering information** — what the consenter records with each block so the assembler can place it
  in the global order: the decision number, the block's index within that decision, and the
  decision's block count.

## 2. Interfaces: Ingesting Events and Serving Decisions

The consenter is an internal ordering-service node: it does not face clients. Its interfaces connect
it to the batchers of every party, which broadcast their attestations and complaints to all
consenters; to the router of its own party, for configuration changes; to the consenters of the other
parties, for the consensus protocol itself; and to the nodes that read its ordered stream.

**Ingesting events** happens through the `Consensus` gRPC service, defined in
[`communication.proto`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/protos/comm/communication.proto):

```protobuf
service Consensus {
  // NotifyEvent receives a stream of events (BAFs and complaints) from clients.
  rpc NotifyEvent(stream Event) returns (stream EventResponse);
  // SubmitConfig receives a configuration request.
  rpc SubmitConfig(Request) returns (SubmitResponse);
  // AckConfig receives an acknowledgment that a node has taken a new configuration.
  rpc AckConfig(ConfigAck) returns (ConfigAckResponse);
}
```

- `NotifyEvent` is the batchers' path, and the batchers of every party stream to it. Each `Event`
  carries one serialized `ControlEvent` — a BAF or a complaint. The consenter verifies the event's
  signature, updates its BAF and complaint metrics, and submits the event to SmartBFT for ordering.
- `SubmitConfig` is the router's path for a configuration change. The consenter checks that the caller
  is the party's router (by its TLS certificate), validates and re-derives the configuration update,
  and, if it is well-formed, submits it as a configuration-request control event.
- `AckConfig` collects acknowledgments that a router, batcher, or assembler has taken a given
  configuration sequence. These gate reconfiguration (see [Reconfiguration](#7-reconfiguration)).

**Consensus among the parties** flows over Fabric's cluster service (`ClusterNodeService`): its
streaming `Step` RPC carries the SmartBFT protocol messages exchanged between the consenters of
different parties, and forwarded requests, all over mutual TLS.

**Serving decisions** uses Fabric's Atomic Broadcast `Deliver` service, the same service the assembler
uses to serve blocks, here carrying the consenter's *decision channel* rather than a client ledger.
`Broadcast` is not served (it is a no-op); a consumer opens a `Deliver` stream and drives it with a
`SeekInfo`, exactly as against any Fabric orderer:

```go
seekInfo := &orderer.SeekInfo{
    Start:         &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: startBlock}}},
    Stop:          &orderer.SeekPosition{Type: &orderer.SeekPosition_Specified{Specified: &orderer.SeekSpecified{Number: math.MaxUint64}}},
    Behavior:      orderer.SeekInfo_BLOCK_UNTIL_READY,
    ErrorResponse: orderer.SeekInfo_BEST_EFFORT,
}
```

Three kinds of consumer follow this stream in steady state, all node-to-node and authenticated by
mutual TLS: assemblers pull the ordered **decisions** containing **batch attestations** to materialize
blocks; batchers pull the ordered decision to learn the current term; and routers track decisions
to stay current with configuration. Beyond these, the consenter itself consumes the stream: a node
that is new or has fallen behind reads the same `Deliver` stream from its peer consenters to rebuild
its own ledger before it can take part (see [Catching Up from Other Consenters](#62-catching-up-from-other-consenters)).
Because a decision carries a quorum of consenter signatures per block (see
[Building a Decision](#35-building-a-decision)), a consumer — including a synchronizing consenter —
can verify the order it reads without trusting the single consenter it happens to be connected to.

## 3. Design and Internal Architecture

Two layers make up the consenter. SmartBFT agrees on the order of opaque request bytes; the state
machine gives those bytes their Arma meaning. Every correct consenter feeds the *same* ordered
requests into the *same* deterministic state machine, so every consenter derives the same batch
attestations, the same primary rotations, and the same blocks — no second round of agreement is
needed on any of that. The whole consenter is the loop below: order the next batch of control events,
fold them into the state, and emit whatever became decided.

```
1  loop
2     ces   ← SmartBFT.OrderNextRequests();          // total order over control events
3     ces   ← drop ces whose batch digest ∈ BADB;    // already-ordered batches
4     state, bafs, cfgReqs ← state.Process(ces);      // deterministic fold
5     bas   ← aggregateFragments(bafs);               // group fragments into batch attestations
6     blocks ← buildBlocks(bas, cfgReqs, state);      // one block per BA, plus any config block
7     Deliver(blocks);                                // index digests, then append to ledger and serve
8  end
```

Line 4 is where the consenter's logic lives — the fold that turns an ordered list of events into
newly-decided attestations and configuration requests — and the subsections below work outward from
there.

### 3.1 Units and Wiring

The consenter object registers itself with the **SmartBFT engine** as all of the roles SmartBFT needs
— the application that assembles and delivers proposals, the signer, the verifier, and the request
inspector — and hands SmartBFT its communication layer and its synchronizer. The **state machine**
(the `Consenter` and the `state` package) is invoked from proposal assembly to compute the next state;
the **BADB** is consulted there to drop already-ordered batches and updated on delivery; the
**consensus ledger** is where delivered decisions are appended. A **synchronizer** catches a lagging
node up from its peers. A **communication layer**
(the cluster service and an egress client) carries SmartBFT traffic between parties, a
**configuration-ack receiver** collects reconfiguration acknowledgments, and the **`Deliver`
service** reads the consensus ledger for consumers. Around these run the operations subsystem, the
metrics tracker, and a health checker.

The units are coordinated by a handful of channels: `softStopCh` gates request handling while the node
is between configurations, `ReconfigAbort` releases anything blocked on a reconfiguration when the
node stops, and `MainExitChan` signals process exit. Inbound RPC handlers reject requests while the
node is soft-stopped.

<!-- Figure 2 placeholder -->
*Figure 2: The consenter's units — the SmartBFT engine, the state machine, the BADB, the consensus
ledger, the synchronizer, and the communication and Deliver services — and how they are wired.
(Diagram to be added.)*

### 3.2 The Deterministic State Machine

The state machine's core is a single pure function that takes the current state, the configuration
sequence in force, and the newly-ordered control events, and returns the next state together with the
fragments and configuration requests that became decided. It never mutates the current state; it
clones it and returns a successor, so a proposal can be simulated and verified before it is committed.
It runs the following steps, in this order:

```
Process(state, configSeq, ces):
1  next ← state.Clone();
2  ces  ← keep BAFs/complaints whose configSeq matches, and config requests whose configSeq is next;
3  next.dropPendingEventsWithStaleConfigSeq(configSeq);
4  next.collectAndDeduplicate(ces);        // append new BAFs to Pending, complaints to Complaints
5  next.detectEquivocation();              // conflicting digests for one batch → rotate that primary
6  next.rotatePrimariesOnComplaints();     // ≥ threshold complaints for a shard → rotate its primary
7  next.cleanupStaleComplaints();          // discard complaints below a shard's current term
8  bafs    ← next.extractAttestations();   // digests that reached threshold → decided; clear Pending
9  cfgReqs ← extractConfigRequests(ces);
10 return next, bafs, cfgReqs;
```

Everything about this is deterministic: events are deduplicated on stable keys, shards and batches are
iterated in sorted order, and the state serializes with deterministic marshaling. Two consenters given
the same ordered events therefore produce byte-identical successor states — which is why the state
snapshot can travel inside a decision and be checked by anyone verifying it. Configuration requests are
treated apart from the rest: they must *advance* the configuration sequence by exactly one (a BAF or
complaint must instead *match* the current sequence), and they are not folded into the state but
returned for the block-building layer to turn into a configuration block.

BAFs are handled by config sequence in three ways: those matching the current sequence are processed
as above; those *ahead* of it are dropped and will be re-sent; and those *behind* it are not dropped
but set aside as **stale config BAFs**, carried in the state so a batch attested under an outgoing
configuration can be revived after a reconfiguration (see [Reconfiguration](#7-reconfiguration)).

### 3.3 From BAFs to a Batch Attestation

A batch is identified by its ⟨shard, primary, sequence⟩. Within that key the consenter groups pending
BAFs by the digest they attest, and a digest becomes a **batch attestation** as soon as `threshold`
(`f+1`) distinct signers have attested it — `f+1` is the smallest set that must contain a correct
signer, so a BA cannot rest on faulty attesters alone. Every digest that reaches threshold is
extracted; digests below it — a minority a faulty batcher put forward — are dropped, and once any
digest is decided the batch is settled and all of its pending fragments are cleared. With an honest
primary exactly one digest reaches threshold, so the batch yields a single BA. An **equivocating**
primary, though, can drive two conflicting digests to threshold for the same ⟨shard, primary,
sequence⟩; both are then extracted and each becomes its own block. Detecting the equivocation rotates
the primary for later sequences (see [The Consenter as System Controller](#34-the-consenter-as-system-controller)),
but it does not retract the attestations already decided here. A fragment whose digest is already
recorded in the **BADB** is discarded before the fold even runs, so a batch that has already been
ordered is never ordered again.

A single decision often commits several batches at once. Each consenter's `SignProposal` callback
produces one composite signature covering the proposal together with each block header in the
decision; SmartBFT collects a quorum of these, and the consenter stores and serves them unchanged, one
per party. The **consumer** unpacks them: an assembler or a synchronizing consenter, on reading the
decision, splits each party's composite signature into a per-block signature set, so every block it
materializes carries a quorum of consenter signatures over *its own* header. That per-block check is
what lets a consumer verify the order without trusting the single consenter it pulled from.

### 3.4 The Consenter as System Controller

The consenter replaces a shard's primary by incrementing that shard's **term**, and it does so inside
the same deterministic fold, so every correct consenter rotates the same primary at the same point in
the order. Two things trigger a rotation.

The first is **complaints**. A batcher that suspects its shard's primary — of censoring it, or of
having failed — sends a complaint to the consenters. When `threshold` (`f+1`) distinct complaints
accumulate against a shard's *current* term, the consenter increments the term, which rotates the
primary; `f+1` again ensures at least one complainant is correct. Complaints below the threshold are
retained, and complaints about a term the shard has already left are discarded as stale.

The second is **equivocation**. Since the primary signs every batch it creates, two BAFs that attest
different digests for the same ⟨shard, primary, sequence⟩ are proof that the primary signed
conflicting batches, and that alone rotates the primary. Fragments a batcher signed for its own batch
are excluded from this check, so a non-primary cannot manufacture equivocation evidence against a
primary. At most one rotation per shard happens in a single step, and shards are processed in a fixed
order, keeping the outcome identical across consenters.

### 3.5 Building a Decision

The consenter does not merely order references to batches; it builds the blocks. When SmartBFT asks it
to assemble a proposal, the consenter runs the state machine over the ordered events and packs the
result into the proposal: a header carrying the decision number and a hash link to the previous
decision, one pre-built **common block** per newly-decided BA, a configuration block if a
configuration request was decided, and a full snapshot of the post-decision state. Each BA's block has
empty block data — the payload stays in the batchers — and a header whose data hash *is* the batch
digest, which is what later binds the ordered metadata to the payload an assembler fetches. Into each
block's metadata the consenter writes its **ordering information**: the decision number, the block's
index within the decision, and the number of blocks in the decision.

On delivery the consenter records the decided batch digests in the BADB, writes the composite
signatures into the decision block, and appends that block to the consensus ledger. One decision thus
becomes as many BA blocks as it committed, carried in the decision's own order; the ordering information
is what lets an assembler reassemble these blocks into the single global order across decisions.

<!-- Figure 3 placeholder -->
*Figure 3: One decision becoming blocks — the header and state snapshot, one common block per batch
attestation with the batch digest as its data hash, and the ordering information written into each
block. (Diagram to be added.)*

### 3.6 SmartBFT as the Engine

Ordering itself — leaders, views, view changes, the exchange of protocol messages, and the write-ahead
log that makes agreement durable — is handled by
[SmartBFT](https://github.com/hyperledger/SmartBFT), which the consenter embeds as its engine and
which this document treats as a black box. What the consenter supplies is the boundary between that
engine and Arma's meaning of the ordered bytes. It registers itself as the engine's application,
signer, verifier, and request inspector, and provides the adapter methods the engine calls: to inspect
and identify a request, to assemble a proposal (which is where the state machine runs and the blocks
are built), to verify a proposal or a peer's signature, to sign, and — the commit callback — to deliver
a decided proposal, which indexes the batch digests, appends the block, and tells the engine about any
membership change the decision carried. The engine's own configuration (its timeouts, batch sizing, and
pool limits) comes from shared configuration, described next.

### 3.7 Code Map

| Concept | Source |
|---------|--------|
| Node lifecycle: build, start, stop, soft stop, applying a new config | [`consensus.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/consensus.go), [`consensus_builder.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/consensus_builder.go) |
| The SmartBFT adapter: proposal assembly, verification, signing, delivery | [`consensus.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/consensus.go) |
| The deterministic state machine and its fold | [`state/state.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/state.go), [`consenter.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/consenter.go) |
| Control events, BAFs, complaints, config requests | [`state/control_event.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/control_event.go), [`state/complaint.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/complaint.go), [`state/config_request.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/config_request.go) |
| Batch attestations, composite signatures, ordering information | [`state/available_batch.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/available_batch.go), [`state/compound_sig.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/compound_sig.go), [`state/ordering_information.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/ordering_information.go) |
| Decision ⇄ block encoding | [`state/decision.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/decision.go), [`state/header.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/state/header.go) |
| Batch-attestation DB (dedup) | [`badb/badb.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/badb/badb.go) |
| Configuration apply and config-request validation | [`consensus_config_applier.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/consensus_config_applier.go), [`configrequest/config_request_validator.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/configrequest/config_request_validator.go) |
| Synchronizer: catch-up, block and signature verification | [`synchronizer/`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/synchronizer) |
| `Deliver` service | [`node/delivery`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/delivery) |
| Consensus ledger | [`node/ledger`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/ledger) |
| Metrics | [`metrics.go`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus/metrics.go) |

## 4. Configuration

A consenter is configured like every other node: a local YAML file for what this node is and where it
keeps its state, and the shared configuration for what the network is, which it obtains from a config
block. The sections common to all roles — `General`, `FileStore`, `Operations` and `Metrics` — are
explained in the
[configuration model](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/architecture.md#6-configuration-and-membership)
of the architecture overview, and documented field by field in the commented
[`local_config.yaml`](https://github.com/hyperledger/fabric-x-orderer/blob/main/config/sample/local_config.yaml).
`General.Bootstrap` names the block the node bootstraps from — the genesis block, or a later config
block for a node joining a running network — and `FileStore.Location` is the directory its state is
written under.

The role-specific section is `Consensus`, and it holds a single field:

```yaml
# Consensus specific parameters
Consensus:
  # WALDir specifies the location at which Write Ahead Logs for SmartBFT are
  # stored. If not set, it defaults to <FileStore.Location>/wal.
  WALDir:
```

| Field | Default | What it controls |
|-------|---------|------------------|
| `WALDir` | `<FileStore.Location>/wal` | Where SmartBFT's write-ahead log is stored. The WAL is what makes agreement durable across a restart, so it should be on fast, reliable storage. |

Two more directories are derived from `FileStore.Location`: the **consensus ledger**, the decision
blocks the node has committed, and the **batch-attestation DB** (`batchDB`), the set of already-ordered
batch digests. Neither is configured separately.

Everything else a consenter needs comes from the **shared configuration** and is therefore uniform
across the whole cluster. This includes the topology — the parties (from which `N`, and hence `f`,
`threshold`, and `quorum`, are derived), the shards, and the consenter endpoints it exchanges SmartBFT
messages with and synchronizes from — and the SmartBFT engine's own tuning, carried in the
`Consensus.SmartBFT` block:

```yaml
# Consensus carries config parameters that need to be uniform across the consenters' cluster.
Consensus:
  SmartBFT:
    RequestBatchMaxInterval: 200ms
    RequestForwardTimeout: 5s
    RequestComplainTimeout: 20s
    RequestAutoRemoveTimeout: 3m0s
    ViewChangeResendInterval: 5s
    ViewChangeTimeout: 20s
    LeaderHeartbeatTimeout: 1m0s
    CollectTimeout: 1s
    IncomingMessageBufferSize: 200
    RequestPoolSize: 100000
    LeaderHeartbeatCount: 10
```

| Field | What it controls |
|-------|------------------|
| `RequestBatchMaxInterval` | The longest a leader waits before proposing, even if a full request batch has not accumulated — the latency/throughput trade-off of the ordering path. |
| `RequestForwardTimeout` | How long a node waits for a submitted request to be ordered before forwarding it to the leader. |
| `RequestComplainTimeout` | How long before a node that forwarded a request and saw no progress raises a complaint against the leader. |
| `RequestAutoRemoveTimeout` | How long a request may sit in the pool before it is dropped. |
| `ViewChangeResendInterval`, `ViewChangeTimeout` | How often view-change messages are resent, and how long a view change may take before it is retried. |
| `LeaderHeartbeatTimeout`, `LeaderHeartbeatCount` | How the followers detect a silent leader and start a view change. |
| `CollectTimeout` | How long the state-collection phase of a view change waits. |
| `IncomingMessageBufferSize` | How many incoming consensus messages a node buffers before processing them; the buffer is shared by all senders. |
| `RequestPoolSize` | The capacity of the request (control-event) pool. |

Note that these govern SmartBFT's ordering of *control events*, not transaction batching — batch
sizing and timeouts belong to the batchers and live in the shared `Batching` block. The consenter also
takes its maximum request size from that shared batching configuration.

Templates can be found under
[`config/sample`](https://github.com/hyperledger/fabric-x-orderer/blob/main/config/sample):
[`local_config_consenter.yaml`](https://github.com/hyperledger/fabric-x-orderer/blob/main/config/sample/local_config_consenter.yaml)
for this role, and
[`shared_config.yaml`](https://github.com/hyperledger/fabric-x-orderer/blob/main/config/sample/shared_config.yaml)
for the network-wide part.

## 5. Metrics and Monitoring

A consenter exposes its metrics in Prometheus format on the operations endpoint configured by
`Operations.ListenAddress` and `Operations.ListenPort`, alongside a health check, a logging-spec
endpoint, and version information. When `Metrics.MetricsLogInterval` is non-zero the node also writes a
periodic `CONSENSUS_METRICS` line to its log, with running totals and the decisions and blocks made
during the last interval — enough to watch progress without a Prometheus deployment.

Five metrics are the consenter's own, each a counter labelled with the party ID:

| Metric | What it tells you |
|--------|-------------------|
| `consensus_decisions_count` | How many decisions the consenter has committed. Its rate is the ordering throughput of the cluster as this node sees it; a rate that drops toward zero points at a stalled view or a leader in trouble. |
| `consensus_blocks_count` | How many blocks the consenter has ordered. It rises faster than `decisions_count` because one decision can commit several batch attestations. |
| `consensus_bafs_count` | How many batch attestation fragments the consenter has received. Compare across parties: a shard whose fragments dry up is a shard whose batchers are struggling. |
| `consensus_complaints_count` | How many complaints the consenter has received. A rising count is the early signal of a suspect shard primary, ahead of the rotation it may cause. |
| `consensus_txs_count` | How many transactions the consenter has ordered, across all the blocks it committed. |

The full list of metrics for every role, and how to collect and visualize them, is in the
[monitoring and metrics guide](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/monitoring/metrics.md).

Beside its metrics, a node keeps a status of its own, which moves between five states: `initializing`
while it starts up or takes a new configuration, `running` once it is ordering, `soft-stopped` while it
is between configurations, `pending-admin` when a configuration change needs an administrator, and
`stopped`. The status is not published on the operations endpoint, so the node's log is where these
transitions are visible.

## 6. Failure and Recovery

### 6.1 Restarting from Local State

The consenter's durable state is its consensus ledger and SmartBFT's write-ahead log, and between them
they hold everything it needs to resume. On restart the consenter reads the last block of its ledger
and reconstructs from it the post-decision state, the SmartBFT view metadata, the last proposal and its
signatures, and the hash link to continue the chain; SmartBFT then replays its WAL to recover any
agreement that was in flight when the process stopped. The batch-attestation DB is reopened from disk,
so the deduplication of already-ordered batches survives the restart too. A consenter that went down
mid-agreement therefore comes back exactly where it left off, without re-ordering anything.

### 6.2 Catching Up from Other Consenters

Catch-up serves two situations through one path. A consenter that is new, or whose ledger is behind the
config block it was given, must be filled from the consenters of the other parties before it can take
part. A consenter that is already running can also fall behind — and when SmartBFT determines it has, it
drives the node's synchronizer to catch up in place, without a restart. Either way the synchronizer
first works out a target height by asking every consenter endpoint for its height and taking the
`f+1`-th highest — a height at least one correct node is guaranteed to have reached, so a set of faulty
nodes cannot lure it to a bad target. If its ledger is empty it first obtains the genesis block,
accepting the block that `f+1` endpoints agree on, which is what makes a fresh node's first block safe
to accept from parties it does not trust. From
there, blocks are pulled and committed up to the target height, and each one is verified before it is
written — that it chains to the previous block, that its data hash matches its data, and that it
carries a quorum of consenter signatures under the channel's block-validation policy — so a party that
serves a wrong block cannot advance a synchronizing node's ledger. Once the target is reached the node
resumes normal ordering, and 6.1 applies from that point on.

### 6.3 When Other Nodes Fail

The consensus cluster tolerates up to `F` faulty parties out of `N ≥ 3F+1`. A faulty consenter that is
the current SmartBFT leader is handled by the engine itself: the followers detect the silence or
misbehavior and change the view to a new leader, under the heartbeat and view-change timeouts in the
shared configuration. A faulty *shard primary* — a different failure, one stage upstream — is handled
by the controller path of [section 3.4](#34-the-consenter-as-system-controller): the batchers complain
or the primary equivocates, and the consenter rotates the primary as part of the ordered stream. In
both cases the batch-attestation DB guarantees that recovery never causes a batch to be ordered twice.

## 7. Reconfiguration

A new configuration reaches the consenter the way everything else does — as a decision. A configuration
request that is ordered becomes a configuration block in the stream, and when the consenter delivers a
decision whose block is that configuration block, and the configuration it carries is newer than the
one the node is running, the node applies it.

Applying begins with a **soft stop**, which pauses the parts of the node that depend on the current
configuration while the reconfiguration is worked out. The node first waits, up to a timeout, for the
router, batchers, and assembler of its party to acknowledge the new configuration sequence — this is
what the `AckConfig` path collects — so that consensus can tell which nodes have taken the new
configuration; it proceeds even if not all acknowledgments arrive.

Not every change can be applied by the node itself. If the new configuration evicts the consenter's own
party, or changes this node's own identity — its endpoint, its TLS certificate, or its signing
certificate — the node enters the **pending-admin** state and waits for an administrator rather than
reconfiguring itself. Anything else it applies dynamically, in the running process: it rebuilds its
configuration and recreates its services against the new topology, swapping in the new state while the
SmartBFT engine keeps running, and resumes ordering — no process restart. A change to the number of
parties additionally bumps the term of every shard, since the party count is what `f`, `threshold`,
and `quorum` are derived from. Throughout, the config-request validator has already ensured that the
pending update re-derives to exactly the configuration that was proposed, so every consenter applies
the same change.

For the configuration model this rests on, and how a configuration change is proposed and ordered in
the first place, see
[Configuration and Membership](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/architecture.md#6-configuration-and-membership).

## 8. Deployment Guidance

<!-- Section 8 placeholder -->
*To be written: what to provision a consenter with — the storage the ledger and WAL need and how fast
they grow; CPU and network for the BFT path; and endpoint and certificate considerations for the
inter-consenter mesh.*

## 9. Further Reading

- [Architecture overview](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/architecture.md)
  — the four roles, the end-to-end flow, and the configuration model.
- [APIs](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/api.md) — the gRPC services of
  every role, and which role implements which.
- [Monitoring and metrics](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/monitoring/metrics.md)
  — the full list of metrics, and how to collect and visualize them.
- [SmartBFT](https://github.com/hyperledger/SmartBFT) — the BFT engine the consenter embeds.
- [`arma` CLI](https://github.com/hyperledger/fabric-x-orderer/blob/main/docs/cli/arma.md) — the node
  binary and its subcommands.
- [`node/consensus`](https://github.com/hyperledger/fabric-x-orderer/blob/main/node/consensus) — the
  source.
