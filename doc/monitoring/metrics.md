The Orderers Metrics per each component are:

Router:
*	Namespace:  "router",	
	Name:       "requests_completed",	
	Help:       "The number of incomming requests that have been completed."	
	
*	Namespace:  "router",	
	Name:       "requests_rejected",	
	Help:       "The number of incomming requests that have been rejected."	
	
Assembler:
*	Namespace:  "assembler_ledger",	
	Name:       "transaction_count_total",	
	Help:       "The total number of transactions committed to the ledger."	
	
*	Namespace:  "assembler_ledger",	
	Name:       "blocks_size_bytes_total",	
	Help:       "The estimated total size in bytes of blocks committed to the ledger."	
	
*	Namespace:  "assembler_ledger",	
	Name:       "blocks_count_total",	
	Help:       "The total number of blocks committed to the ledger."	
	
Batcher:
*	Namespace:  "batcher",	
	Name:       "current_role",	
	Help:       "The current role of the batcher: 1=primary, 2=secondary."	
	
*	Namespace:  "batcher",	
	Name:       "mempool_size",	
	Help:       "The current size of the mempool."	
	
*	Namespace:  "batcher",	
	Name:       "role_changes_total",	
	Help:       "The total number of role changes."	
	
*	Namespace:  "batcher",	
	Name:       "batches_created_total",	
	Help:       "The total number of batches created."	
	
*	Namespace:  "batcher",	
	Name:       "batches_pulled_total",	
	Help:       "The total number of batches pulled."	
	
*	Namespace:  "batcher",	
	Name:       "batched_txs_total",	
	Help:       "The total number of transactions batched."			
	
*	Namespace:  "batcher",	
	Name:       "router_txs_total",	
	Help:       "The total number of transactions received from the router."			
	
*	Namespace:  "batcher",	
	Name:       "complaints_total",	
	Help:       "The total number of complaints sent."			
	
*	Namespace:  "batcher",	
	Name:       "first_resends_total",	
	Help:       "The total number of first resends performed."	
	
Consenter:
*	Namespace:  "consensus",	
	Name:       "decisions_count",	
	Help:       "Total number of decisions made by the consenter."	
	
*	Namespace:  "consensus",	
	Name:       "blocks_count",	
	Help:       "Total number of blocks ordered by the consenter."	
	
*	Namespace:  "consensus",	
	Name:       "bafs_count",	
	Help:       "Total number of batch attestation fragments received by the consenter."		
	
*	Namespace:  "consensus",	
	Name:       "complaints_count",	
	Help:       "Total number of complaints received by the consenter."
	
	
In order to set up prometheus (and grafana as well) correctly we need to edit the file "prometheus.yml" as this example (the example is for 4 parties and 2 shards):

"
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
        - "9.47.166.169:9004" # batcher shard1
        - "9.47.166.170:9005" # batcher shard2

        # Party 2
        - "9.47.166.171:9001"
        - "9.47.166.173:9002"
        - "9.47.166.174:9003"
        - "9.47.166.175:9004"
        - "9.47.166.176:9005"

        # Party 3
        - "9.47.166.177:9001"
        - "9.47.166.178:9002"
        - "9.47.166.179:9003"
        - "9.47.166.180:9004"
        - "9.47.166.181:9005"

        # Party 4
        - "9.47.166.182:9001"
        - "9.47.166.183:9002"
        - "9.47.166.184:9003"
        - "9.47.166.185:9004"
        - "9.47.166.186:9005"
"

The ports must be constant and known in advance.

We also need to set up inside deployment/vms/sample-config/arma-config, by adding the following fields in each "local_config_*.yaml" file:

example for party1/local_config_assembler.yaml:
"
    ...
    MonitoringListenAddress: "0.0.0.0:9001"
    MonitoringListenPort: 9001
"
PAY ATTENSION that each port must correspond to the port inside the prometheus.yml as well

Once all of those yamls are set up we can just upload the prometheus server by this command:
"./prometheus --config.file=prometheus.yml" 
the command should be running from the folder that contains all the prometheus installed files as well.
