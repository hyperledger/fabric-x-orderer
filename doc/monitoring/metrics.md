# Monitoring and Metrics

This document describes the metrics exposed by each ARMA component, how to enable metric endpoints, and how Prometheus and Grafana are used to collect and visualize these metrics.

---

## Background: Prometheus and Grafana

Prometheus is a monitoring system that periodically **scrapes metrics** from HTTP endpoints exposed by services.  
Each component exposes its metrics in Prometheus format over HTTP, and Prometheus pulls (scrapes) those metrics at a fixed interval.

Prometheus stores the scraped metrics as time-series data.

Grafana is used as a visualization layer on top of Prometheus.  
It queries Prometheus for metric data and displays it as dashboards, graphs, and alerts. Grafana does not collect metrics itself — it relies entirely on Prometheus as its data source.

---

## List of Metrics per Component

### Router

- **Namespace**: "router"  
  **Name**: "requests_completed"  
  **Help**: "The number of incoming requests that have been completed."

- **Namespace**: "router"  
  **Name**: "requests_rejected"  
  **Help**: "The number of incoming requests that have been rejected."

---

### Assembler

- **Namespace:** "assembler_ledger"  
  **Name:** "transaction_count_total"  
  **Help:** "The total number of transactions committed to the ledger."

- **Namespace:** "assembler_ledger"  
  **Name:** "blocks_size_bytes_total"  
  **Help:** "The estimated total size in bytes of blocks committed to the ledger."

- **Namespace:** "assembler_ledger"  
  **Name:** "blocks_count_total"  
  **Help:** "The total number of blocks committed to the ledger."

---

### Batcher

- **Namespace:** "batcher"  
  **Name:** "current_role"  
  **Help:** "The current role of the batcher: 1 = primary, 2 = secondary."

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

---

### Consenter

- **Namespace:** "consensus"  
  **Name:** "decisions_count"  
  **Help:** "The total number of decisions made by the consenter."

- **Namespace:** "consensus"  
  **Name:** "blocks_count"  
  **Help:** "The total number of blocks ordered by the consenter."

- **Namespace:** "consensus"  
  **Name:** "bafs_count"  
  **Help:** "The total number of batch attestation fragments received by the consenter."

- **Namespace:** "consensus"  
  **Name:** "complaints_count"  
  **Help:** "The total number of complaints received by the consenter."

---

## Enabling Metrics

Each component exposes its metrics via an HTTP endpoint that Prometheus scrapes.

Locate the `local_config_*.yaml` files (where `*` is the component name:   `assembler`, `consenter`, `batcher`, or `router`) and add the following fields **below** the `MetricsLogInterval` field.

### Example: `party1/local_config_assembler.yaml`

```yaml
...
MetricsLogInterval: 10s
MonitoringListenAddress: "0.0.0.0"
MonitoringListenPort: 9001
```
---

## Pulling the Metrics

### Prometheus Configuration

Prometheus must be configured with the list of all metric endpoints it should scrape.

Edit the ``prometheus.yml`` file as shown below.

The following example describes a deployment with **4 parties** and **2 batcher shards per party**.

```yaml
global:
  scrape_interval: 5s

scrape_configs:
  - job_name: "arma"
    static_configs:
      - targets:
          # Party 1
          - "9.47.166.166:9001" # assembler
          - "9.47.166.167:9002" # consenter
          - "9.47.166.168:9003" # router
          - "9.47.166.169:9004" # batcher shard 1
          - "9.47.166.170:9005" # batcher shard 2

          # Party 2
          - "9.47.166.171:9001" # assembler
          - "9.47.166.173:9002" # consenter
          - "9.47.166.174:9003" # router
          - "9.47.166.175:9004" # batcher shard 1
          - "9.47.166.176:9005" # batcher shard 2

          # Party 3
          - "9.47.166.177:9001" # assembler
          - "9.47.166.178:9002" # consenter
          - "9.47.166.179:9003" # router
          - "9.47.166.180:9004" # batcher shard 1
          - "9.47.166.181:9005" # batcher shard 2

          # Party 4
          - "9.47.166.182:9001" # assembler
          - "9.47.166.183:9002" # consenter
          - "9.47.166.184:9003" # router
          - "9.47.166.185:9004" # batcher shard 1
          - "9.47.166.186:9005" # batcher shard 2
```

**Notes:**

- Monitoring ports must be constant and known in advance.

- All metric endpoints must be reachable from the Prometheus server.

- Grafana should be configured to use Prometheus as its data source.


After all configuration files are updated, start the Prometheus server using:

```
./prometheus --config.file=prometheus.yml
```

This command must be executed from the directory that contains the Prometheus binaries and configuration files.

---