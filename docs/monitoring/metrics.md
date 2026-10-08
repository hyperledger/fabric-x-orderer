# Monitoring and Metrics

This document lists the metrics each Arma component exposes, and how to collect them with Prometheus and view them in Grafana.

Each node serves its metrics over HTTP in Prometheus format. Prometheus scrapes (pulls) them at a fixed interval and stores them as time series, and Grafana reads them from Prometheus to draw dashboards.

---

## List of Metrics per Component

The name to query in Prometheus is the namespace and the name joined by `_`, for example `router_requests_completed`.

### Common to All Components

- **Name**: "arma_version"  
  **Help**: "The active version of Arma."  
  **Labels**: "version" is the version of the running binary.

---

### Router

All router metrics carry the "party_id" label.

- **Namespace**: "router"  
  **Name**: "requests_completed"  
  **Help**: "The number of incoming requests that have been completed."

- **Namespace**: "router"  
  **Name**: "requests_rejected"  
  **Help**: "The number of incoming requests that have been rejected."  
  **Labels**: "code" is either "400" or "500".

- **Namespace**: "router"  
  **Name**: "requests_throttled"  
  **Help**: "The number of incoming requests rejected by the rate limiter."

- **Namespace**: "router"  
  **Name**: "active_streams"  
  **Help**: "The number of currently active client gRPC streams."  
  **Labels**: "stream_type" is either "broadcast" or "submit_stream".

- **Namespace**: "router"  
  **Name**: "submit_invocations"  
  **Help**: "The number of times the Submit RPC was invoked."

---

### Assembler

What each of these metrics tells you about the assembler is described in [Assembler](../assembler.md#5-metrics-and-monitoring).

All assembler metrics carry the "party_id" label, and "prefetch_index_size_bytes" also carries "shard_id".

- **Namespace:** "assembler"  
  **Name:** "batch_unary_fetch_latency_seconds"
  **Help:** "The latency to unary fetch a requested batch from the batchers in the shard."

- **Namespace:** "assembler"  
  **Name:** "attestation_to_batch_collation_latency_seconds"
  **Help:** "The latency from receiving a batch attestation until the matching batch is available."

- **Namespace:** "assembler"  
  **Name:** "batch_ledger_append_latency_seconds"
  **Help:** "The latency to append a batch to the ledger."

- **Namespace:** "assembler"  
  **Name:** "prefetch_index_size_bytes"
  **Help:** "The current size of the assembler prefetch index for a shard in bytes."

- **Namespace:** "assembler"  
  **Name:** "prefetch_index_cache_evictions_total"  
  **Help:** "The total number of evictions from the assembler prefetch index cache."

- **Namespace:** "assembler_ledger"  
  **Name:** "transaction_count_total"  
  **Help:** "The total number of transactions committed to the ledger."

- **Namespace:** "assembler_ledger"  
  **Name:** "blocks_size_bytes_total"  
  **Help:** "The estimated total size in bytes of blocks committed to the ledger."

- **Namespace:** "assembler_ledger"  
  **Name:** "blocks_count_total"  
  **Help:** "The total number of blocks committed to the ledger."

The assembler also serves the Deliver API, and exposes these metrics for it:

- **Namespace:** "deliver"  
  **Name:** "streams_opened"  
  **Help:** "The number of GRPC streams that have been opened for the deliver service."

- **Namespace:** "deliver"  
  **Name:** "streams_closed"  
  **Help:** "The number of GRPC streams that have been closed for the deliver service."

- **Namespace:** "deliver"  
  **Name:** "requests_received"  
  **Help:** "The number of deliver requests that have been received."  
  **Labels:** "channel", "filtered", "data_type".

- **Namespace:** "deliver"  
  **Name:** "requests_completed"  
  **Help:** "The number of deliver requests that have been completed."  
  **Labels:** "channel", "filtered", "data_type", "success".

- **Namespace:** "deliver"  
  **Name:** "blocks_sent"  
  **Help:** "The number of blocks sent by the deliver service."  
  **Labels:** "channel", "filtered", "data_type".

---

### Batcher

All batcher metrics carry the "party_id" and "shard_id" labels.

- **Namespace:** "batcher"  
  **Name:** "current_role"  
  **Help:** "The current role of the batcher: 1=primary, 2=secondary."

- **Namespace:** "batcher"  
  **Name:** "mempool_size"  
  **Help:** "The current size of the mempool."

- **Namespace:** "batcher"  
  **Name:** "role_changes_total"  
  **Help:** "The total number of role changes."

- **Namespace:** "batcher"  
  **Name:** "batches_created_total"  
  **Help:** "The total number of batches created."

- **Namespace:** "batcher"  
  **Name:** "batches_pulled_total"  
  **Help:** "The total number of batches pulled."

- **Namespace:** "batcher"  
  **Name:** "batched_txs_total"  
  **Help:** "The total number of transactions batched."

- **Namespace:** "batcher"  
  **Name:** "router_txs_total"  
  **Help:** "The total number of transactions received from the router."

- **Namespace:** "batcher"  
  **Name:** "complaints_total"  
  **Help:** "The total number of complaints sent."

- **Namespace:** "batcher"  
  **Name:** "first_resends_total"  
  **Help:** "The total number of first resends performed."

- **Namespace:** "batcher"  
  **Name:** "batch_mempool_next_requests_latency_seconds"  
  **Help:** "The latency for the primary to retrieve the next batch from the mempool."

- **Namespace:** "batcher"  
  **Name:** "batch_verify_latency_seconds"  
  **Help:** "The latency from receiving a batch on the secondary until it is verified."

- **Namespace:** "batcher"  
  **Name:** "batch_hashing_latency_seconds"  
  **Help:** "The latency to compute the batch requests digest."

- **Namespace:** "batch_ledger"  
  **Name:** "header_hashing_latency_seconds"  
  **Help:** "The latency to compute the block header hash."

- **Namespace:** "batch_ledger"  
  **Name:** "append_latency_seconds"  
  **Help:** "The latency to append a batch to the ledger."

---

### Consenter

What each of these metrics tells you about the consenter is described in [Consenter](../consensus.md#5-metrics-and-monitoring).

All consenter metrics carry the "party_id" label.

- **Namespace:** "consensus"  
  **Name:** "decisions_count"  
  **Help:** "Total number of decisions made by the consenter."

- **Namespace:** "consensus"  
  **Name:** "blocks_count"  
  **Help:** "Total number of blocks ordered by the consenter."

- **Namespace:** "consensus"  
  **Name:** "bafs_count"  
  **Help:** "Total number of batch attestation fragments received by the consenter."

- **Namespace:** "consensus"  
  **Name:** "complaints_count"  
  **Help:** "Total number of complaints received by the consenter."

- **Namespace:** "consensus"  
  **Name:** "txs_count"  
  **Help:** "Total number of transactions ordered by the consenter."

---

## Enabling Metrics

Each node serves its metrics on its operations endpoint, configured in the `Operations` and `Metrics` sections of its `local_config_*.yaml` file (where `*` is `assembler`, `consenter`, `batcher` or `router`):

```yaml
Operations:
    ListenAddress: 0.0.0.0
    ListenPort: 8080
    TLS:
        Enabled: false
Metrics:
    Provider: prometheus
    MetricsLogInterval: 10s
```

- `ListenAddress`: `armageddon generate` writes `127.0.0.1`, which is reachable only from the same machine. Use `0.0.0.0` so that Prometheus can reach the node.
- `ListenPort`: without it a random port is picked, so set a fixed one. All nodes can use the same port.
- `TLS.Enabled`: when true, the metrics are served over HTTPS and Prometheus must be configured for it.
- `Provider`: `prometheus` (the default) or `disabled`.
- `MetricsLogInterval`: how often the node also writes its metrics to its log; `0` turns this off.

The metrics are then served at `http://<host>:<ListenPort>/metrics`. The same port also serves `/healthz`, `/version` and `/logspec`.

---

## Pulling the Metrics

Prometheus must be configured with the metrics endpoint of every node. For example, for the nodes of party 1 in [example-deployment.yaml](../../node/examples/config/example-deployment.yaml), which has two shards:

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "arma"
    static_configs:
      - targets:
          - "consensus.p1:8080"
          - "batcher1.p1:8080"
          - "batcher2.p1:8080"
          - "assembler.p1:8080"
          - "router.p1:8080"
          # ... and the same for every other party
```

Then start Prometheus with this file, and configure Grafana to use Prometheus as its data source:

```
./prometheus --config.file=prometheus.yml
```

To run all of this locally, [node/examples/grafana](../../node/examples/grafana/README.md) starts the sample network together with Prometheus and Grafana, and writes the Prometheus configuration for you.
