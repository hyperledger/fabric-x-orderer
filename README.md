# Arma: a scalable Byzantine Fault Tolerant ordering service


## Abstract

Arma is a Byzantine Fault Tolerant (BFT) consensus system designed to achieve horizontal scalability across all hardware
resources: network bandwidth, CPU, and disk I/O. As opposed to preceding BFT protocols, Arma separates the dissemination
and validation of client transactions from the consensus process, restricting the latter to totally ordering only
metadata of batches of transactions. This separation enables each party to distribute compute and storage resources for
transaction validation, dissemination and disk I/O among multiple machines, resulting in horizontal scalability.
Additionally, Arma ensures censorship resistance by imposing a maximum time limit on the inclusion of client transactions.

## Introduction

Arma is composed of 4 types of servers: routers, batchers, consenters and assemblers.

- Routers accept transactions from submitting clients, perform some validation on the transactions, and dispatch them to batchers.

- Batchers are grouped in to shards. A transaction is dispatched to a single shard. The batchers in a shard then bundle
  transactions into batches, and save them to disk. Batchers then send digests of the batches, called batch attestation
  fragments (BAF) to the consenters.

- Consenters run a BFT consensus protocol which receives as input the BAF's from the batcher shards and provide a total
  order of batch attestations (BA). This induces total order among the batches and hence among TXs.

- Assemblers consume the stream of totally ordered batch attestations from the consensus cluster, and pull batches from
  the batchers. They then fuse the two sources to create a totally ordered ledger of blocks - one block for each batch.
  The block ledger is largely compatible with the Fabric ledger. (See [https://github.com/hyperledger/fabric-protos/blob/main/common/common.proto]).

Clients submit transactions to the routers, whereas blocks are consumed from the assemblers.

More details on the internal architecture and inner workings of Arma can be found in the white paper: [https://ia.cr/2024/808]


## Client API

Arma provides a gRPC service for submitting transactions and consuming blocks. This service is identical to Fabric's "Atomic Broadcast API".
The gRPC service is defined here: [https://github.com/hyperledger/fabric-protos/blob/main/orderer/ab.proto]

It defines two services:
-	The `Broadcast` service allows a client to submit transactions for ordering by the ordering servers.
-	The `Deliver` service allows clients to consume ordered blocks.

```protobuf
service AtomicBroadcast {
// broadcast receives a reply of Acknowledgement for each common.Envelope in order, indicating success or type of failure
rpc Broadcast(stream common.Envelope) returns (stream BroadcastResponse);

// deliver first requires an Envelope of type DELIVER_SEEK_INFO with Payload data as a mashaled SeekInfo message, then a stream of block replies is received.
rpc Deliver(stream common.Envelope) returns (stream DeliverResponse);
}
```

The Arma routers implement the `Broadcast` service whereas the Arma assemblers implement the `Deliver` service.

In order to submit a TX the submiting client must connect to the router endpoints and try to submit to all the routers, from all parties.
Even though a submitting party may submit to a single party it trusts, that may incur a performance penalty and may lead to censorship, and thus is strongly discouraged.

In order to pull blocks it is enough for a scalable committer (peer) to connect to the assembler that belongs to its own party.

The standard clients used in Fabric for submitting and pulling blocks should generally be compatible with Arma, with some minor adjustments.

### Authentication and Authorization

Arma routers and assemblers support two modes of operation.

- Mutual TLS. The client needs to have the CA certificate(s) (a pool of certificates) of the certificate authorities that
  had issued the TLS certificates of the routers and assemblers. In addition, the routers and assemblers need to have the
  CA certificate(s) (a pool of certificates) of the certificate authorities that had issued the TLS certificates of the clients.

- No TLS. Any client can submit TXs and pull blocks. This mode is only to be used in non-production settings such as testing and demonstrations.

The sections below explain how to configure the security aspects of an Arma deployment.

### Transaction verification

Currently, transactions are not verified by Arma, and do not need to be signed by the submitting client. Transactions
pass as is to the scalable committer which is in charge of verifying their validity in terms of structure, semantics, and
signatures.

## Install

TODO



## Run

Arma is composed of 4 types of servers: `router`, `batcher`, `consensus` and `assembler`; also known as "server roles".
To start a server use the arma CLI tool:

* To run a router node:
   ```bash
   ./arma router --config=arma-config/Party1/router_node_config.yaml
   ```
* To run a batcher node:
   ```bash
   ./arma batcher --config=arma-config/Party1/batcher_node_1_config.yaml
   ```
* To run a consenter node:
   ```bash
   ./arma consensus --config=arma-config/Party1/consenter_node_config.yaml
   ```
* To run an assembler node:
   ```bash
   ./arma assembler --config=arma-config/Party1/assembler_node_config.yaml
   ```

Each server role expects a config file, specified in the command line (mandatory).
For more details please refer to [arma-deployment](deployment/README.md).

### Starting with an external genesis block

Each server may be given an optional external genesis block, which must be on the same path as the config file and be called `genesis.block`.
For example, if an assembler is started like so:
   ```bash
   ./arma assembler --config=arma-config/Party1/assembler_node_config.yaml
   ```

It will look for a genesis block here: `arma-config/Party1/genesis.block`.

All parties and all servers must be given the same genesis block.
If a genesis block is not found, Arma will start with an empty config block.

*NOTE*: This is a temporary feature, as Arma is planned to migrate to a "Fabric-style" configuration.
Furthermore, at this point the genesis block is not read by Arma at all, it is only served to client of the delivery service as block 0.

## Configuration and deployment

For more information about deployment of Arma, please refer to [arma-deployment](deployment/README.md).



## Tools

TODO





