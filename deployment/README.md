## ARMA Deployment

This guide provides instructions on how to deploy ARMA in two deployment options:
1. Running each node of each party in separate virtual machine (VMs) or bare metal machines.
2. Running each party on a single VM, where all party's components running on the same VM.


### VMs
In the following paragraph, we outline and describe the scenario where each node run on a separate virtual machine with its own unique IP address.
Since the nodes have different IPs, similar services can all use the same port number (e.g., 8012 for all consenters), with no need for distinct ports across nodes.

Steps:
1. Provision separate VMs or bare metal machine for each node.   
The number of machines should be: (3 * number of parties + number of parties * shards), as each party includes one router, one consenter, one assembler and the number of batchers is the number of shards.   
    For recommendations on the servers profiles, see [VM profiles](#vm-profiles) below.
2. Edit the `config.yaml` file under `ARMA/deployment/vms/sample-config`.
   Update the file by replacing the IP addresses and ports with the actual real-time server IPs and ports for each node.
    
    NOTE: 
   - The config presented in the config.yaml file corresponds to 4 parties and 2 shards.
   - Batcher endpoints are ordered by shard ID. 
    
3. Compile Armageddon and Arma: 

   From the root directory (ARMA) run:
   ```bash
   make binary
   ```
    In the `ARMA/bin` directory, you will find two binary files: `arma` and `armageddon`. 
    
    The certificates and keys in the YAMLs are stored in Base64-encoded format, which is a text-friendly representation of the raw binary data.
4. Run Armageddon to create configuration files for each node:
   
    Note: Arma can be executed either with TLS or without it. To enable TLS, include the `useTLS` flag. To run without TLS, simply omit the flag.

   ```bash
   cd bin
   ./armageddon generate --config=config.yaml --output=[output]/arma-config --useTLS
   ```

   Under the output directory, you will find an `arma-config` folder, which contains yaml configuration files for each party's nodes. The command also generates crypto material and embeds it in the yaml files.  
   Additionally, each party has a `user_config.yaml` file, which serves as a client for connecting to the router to send transactions and to the assembler to retrieve blocks. This file contains all the cryptographic materials and server details required for the connection.

5. Copy the executable of Arma and the corresponding node configuration file to each VM. Then, run the Arma executable.
For example:
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


At this point, a network of Arma is running, so you can start send txs. 

### Containers

<a name="vm-profiles"></a>
### VM profiles 
**Storage:**  
* Routers do not use storage, don't require SSD or heavy storage capacity. 
* Batchers and Assemblers heavily utilize storage for their operations, we strongly recommend using SSD for optimal performance. We recommend using high capacity storage since all transaction data is stored on these servers.
* Consenters moderately use storage capacity, yet SSD is recommended for optimal performance.

**Connectivity:**  
Routers and Assemblers need connectivity to clients.