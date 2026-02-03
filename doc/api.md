## APIs
Arma decomposes the ordering service into 4 types of servers: routers, batchers, consenters and assemblers, and distributes logic across components.   
Each server implements one or more gRPC services secured with mutual TLS (mTLS), ensuring authenticated and trusted communication between nodes.   
In addition, each node exposes a monitoring endpoint that provides runtime metrics such as transaction throughput and latency. 
For more details on monitoring see [Monitoring and Metrics](https://github.com/hyperledger/fabric-x-orderer/blob/main/doc/monitoring/metrics.md).  

Here is a description of Arma gRPC services:

### Atomic Broadcast API
Arma provides a gRPC service for submitting transactions and consuming blocks. This service is identical to Fabric's "Atomic Broadcast API".
The gRPC service is defined here: [Atomic Broadcast API](https://github.com/hyperledger/fabric-protos/blob/main/orderer/ab.proto).

It defines two services:

- The `Broadcast` service allows a client to submit transactions for ordering by the ordering servers.
- The `Deliver` service allows clients to consume ordered blocks. This service is also used for consuming batches from batchers and decisions from consenters.

```protobuf
service AtomicBroadcast {
// broadcast receives a reply of Acknowledgement for each common.Envelope in order, indicating success or type of failure
rpc Broadcast(stream common.Envelope) returns (stream BroadcastResponse);

// deliver first requires an Envelope of type DELIVER_SEEK_INFO with Payload data as a marshaled SeekInfo message, then a stream of block replies is received.
rpc Deliver(stream common.Envelope) returns (stream DeliverResponse);
}
```

### Request Transmit API 
Arma provides a gRPC service that allows clients to submit requests to the batchers, either as a single request or a stream.
```protobuf
service RequestTransmit {

  rpc Submit(Request) returns (SubmitResponse);
  
  rpc SubmitStream(stream Request) returns (stream SubmitResponse);
}

```

### BatcherControlService API
The primary Batcher exposes the `BatcherControlService` API, used to manage acknowledgments and requests from secondary batchers.

Recall that batchers are grouped into shards.
Each party runs a single batcher for every shard in the system, and that batcher can be either a primary or a secondary. 
Each shard has a single designated primary batcher node, while the rest are secondaries.
The secondaries act as clients of the primary batcher, pulling batches, sending acknowledgments on requests and forwarding requests to the primary if a timeout is reached.  

```protobuf
service BatcherControlService {
  // NotifyAck receives a stream of acknowledgments from secondary batchers.
  rpc NotifyAck(stream Ack) returns (stream AckResponse);
  // FwdRequestStream receives a stream of requests from secondary batchers if a timeout is reached.
  rpc FwdRequestStream(stream FwdRequest) returns (stream FwdRequestResponse);
}
```

### Consensus API
The Consenter nodes expose the Consensus API, used to send events and configuration requests from clients.
Event can be a batch attestation fragment (BAF), which are signatures over batch digests, or a complaint vote sent by secondary batchers
that suspects the primary node of censorship. 

```protobuf
service Consensus {
  // NotifyEvent receives a stream of events from clients.
  rpc NotifyEvent(stream Event) returns (stream EventResponse);
  // SubmitConfig receive a configuration request.
  rpc SubmitConfig(Request) returns (SubmitResponse);
}
```

## gRPC services distribution across roles

The following describes the gRPC services implemented by each node role.

Router:
* `Broadcast` - the router accepts transactions from submitting clients, performs some validation on the transactions, and dispatch them to batchers.
In order to submit a TX the submitting client must connect to the router endpoints and try to submit to all the routers, from all parties.

Batcher:
* `RequestTransmit` - the batcher receives client requests, batches and processes them, and streams responses back to the client. This applies to both primary and secondary batcher.
* `BatcherControlService` - implemented by the primary batcher to manage acknowledgments and handle requests from the secondary batchers.
* `Deliver` - exposed by each batcher (primary or secondary) to allow clients to pull batches.
The deliver service allows assemblers to pull batches from primary or secondary batchers for block assembly, and allows secondary batchers to pull batches from the primary batcher.

Consenter:
* `Consensus` - the consenter accepts events (BAF or complaint), which then are totally ordered through the BFT consensus protocol.
It also accepts configuration requests, performs validation checks and processes them.
* `Deliver` - exposed by each consenter to allow clients to pull decisions. 
Routers, Batchers and Assebmlers consume decisions from consensus nodes acting as clients of the consensus nodes.

Assembler:
* `Deliver` - the assembler accepts a signed envelope from a client and responds with the requested blocks.
In order to pull blocks it is enough for a scalable committer (peer) to connect to the assembler that belongs to its own party.


For more details on Arma see [Hyperledger Fabric-X orderer](https://github.com/hyperledger/fabric-x-orderer/blob/main/README.md).