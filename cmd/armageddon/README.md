# Config ARMA via CLI

This command-line tool provides a simple way to config an ARMA network, which is the ordering service in FabricX.

## Building the tool
1. Run from `ARMA` root folder
2. Run `make binary` to create an executable file named `armageddon` under `bin` directory.


## Commands

Here we list and describe the available commands.
We give a short explanation of their usage and describe the flags for each command.
We provide real-world examples demonstrating how to use the CLI tool for various tasks.



### Version Command
This command prints the version of the CLI tool.
1. Run from `ARMA` root folder.
2. Run `./bin/armageddon version`.  

This command has no flags.


##
### Generate Command
This command enables to create configuration files for ARMA nodes. 


1. Run from `ARMA` root folder.
2.  Run `./bin/armageddon generate [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags                 | Description                                                                                                                                                           |
|-----------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| ` config`             | The absolute or relative path to the configuration template to use                                                                                                    |
| ` output`             | The absolute or relative path to which the configuration files will be saved. If missing configuration files will be saved under `arma-config`                        |
| ` version`            | The version of the configuration. The default is version=2. Configuration created by version=1 is not supported anymore                                               |
| ` sampleConfigPath`   | The absolute or relative path to the sample config directory that includes the msp and the `configtx.yaml` file. For example, see `ARMA/testutil/fabric/sampleconfig` |

###
##### Example:

Running
`./bin/armageddon generate --config=config.yaml --output=arma-config --sampleConfigPath=ARMA/testutil/fabric/sampleconfig` involves:
1) Reading the configuration template that includes the nodes addresses and whether we are running a non/TLS/mTLS connection between client and routers and assemblers. 
2) Generating crypto material.
3) Generating the local and the shared configuration for all nodes.  

###
This command creates an `arma-config` directory, which contains the following subdirectories:
   * config - the local configuration of each node, for more details see [local configuration](#local-configuration).
   * bootstrap - the shared configuration of each node, for more details see [shared configuration](#shared-configuration).
   * crypto - the crypto material for all ARMA nodes, for more details see [crypto](#crypto).

##
<a id="crypto"></a>
Crypto:  
The folder structure of the crypto material is:  

````
arma-config
└── crypto
	└── ordererOrganizations
	    └── org{partyID}
	        ├── ca
	        ├── tlsca
	        ├── msp
	        │   └── admincerts (ignored)
	        ├── orderers
	        │   └── party{partyID}
	        │       ├── router
	        │       ├── batcher1
	        │       ├── batcher2
	        │       ├── ...
	        │       ├── batcher{shards}
	        │       ├── consenter
	        │       └── assembler
	        └── users
````
##
For each party i, all cryptographic materials for party{i} are located under `arma-config/crypto/ordererOrganizations/org{i}`
For example, all cryptographic materials for party1 are located under `arma-config/crypto/ordererOrganizations/org1`:
   - The Certificate Authority (CA) certificates are stored in `arma-config/crypto/ordererOrganizations/org1/ca`.
   - The TLS Certificate Authority (TLS CA) certificates are stored in `arma-config/crypto/ordererOrganizations/org1/tlsca`.
   - Each node within party1 has its own TLS certificates and keys located in `arma-config/crypto/ordererOrganizations/org1/orderers/party1/{node}`. For batchers and cosenters nodes a signing certificate with a corresponding key are generated. 
   - TLS certificate and key for user of party1 is stored in `arma-config/crypto/ordererOrganizations/org1/users`.
NOTE: A fake CA is created for each party.

###
<a id="local-configuration"></a>
Local configuration:  
The local configuration is created for each party for each node and holds node-specific details that are necessary for the node's operation and accessible only to that particular node.  
The local configuration is organized in a folder structure divided into parties, each party includes YAML files for every node in that party:
````
arma-config
└── config
   ├── party1
   │   ├── local_config_assembler.yaml
   │   ├── local_config_batcher1.yaml
   │   ├── local_config_batcher2.yaml
   │   ├── local_config_consenter.yaml
   │   ├── local_config_router.yaml
   │   ├── user_config.yaml
   ├── party2
   ├── party3
   ├── party4
````
For example, see `ARMA/config/sample/test-sample/config`.  

The local configuration generation involves creating for each party a `user_config.yaml` that includes all necessary details that an ARMA user needs.  

The local configuration specifies the location and format (YAML or block) of the shared configuration, with "block" being the default format, i.e. the local configuration points to the location of the config block where the shared configuration is encoded.  
Here’s how it appears in the local config YAML file:
````
General:
   Bootstrap:
      Method: block
      File: /var/folders/dec-trust/arma-config/bootstrap/bootstrap.block
````

###
<a id="shared-configuration"></a>
Shared configuration:  
The shared configuration contains essential information that is uniformly applied across multiple ARMA nodes, ensuring they all operate with the same fundamental settings.  
For example, see `ARMA/config/sample/test-sample/bootstrap`. This directory includes:
   - shared_config.yaml, which contains paths to certificates and keys.
   - shared_config.bin, which is the encoding of the shared configuration that holds the certificates and keys themselves.
   - bootstrap.block, which is the config block to bootstrap from. Within the block, the `consensusMetadata` field contains the content of the `shared_config.bin` file.
   - metaNamespaceVerificationKeyPath.pem, which is needed for the creation of the block.

   All nodes, by default, bootstrap their shared configuration from the generated config block when they are launched.


##
### Submit Command
1. Run from `ARMA` root folder.
2. Run `./bin/armageddon submit [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags              | Description                                                                                                           |
|--------------------|-----------------------------------------------------------------------------------------------------------------------|
| `config`           | The absolute or relative path of the user configuration used for defining a GRPC client to the routers and assemblers |
| `transactions`     | The number of transactions to be sent                                                                                 |
| `rate`             | The number of transactions per second to be sent                                                                      |
| `txSize`           | The required transaction size in bytes. If missing the default txSize=512 is taken.                                   |


###
##### Example:

Running
`./bin/armageddon submit --config=arma-config/config/party1/user_config.yaml --transactions=1000 --rate=500 --txSize=32"` involves: 
1) Creating a GRPC client for the routers and assemblers. 
2) Preparing synthetic transactions of the specified size. 
3) Sending these transactions to all router nodes using the Broadcast API at the specified rate.
4) Pulling blocks from some assembler, using the Delivery API, to validate the transactions appear in some block.
5) Printing statistics.

This command is recommended in small setups for testing with a small amount of transactions, to confirm the system functionality under minimal load.

##
### Load Command
1. Run from `ARMA` root folder.
2. Run `./bin/armageddon load [args]`.

   Replace `[args]` with flags.

###
##### Flags
| Flags             | Description                                                                                                           |
|-------------------|-----------------------------------------------------------------------------------------------------------------------|
| `config`          | The absolute or relative path of the user configuration used for defining a GRPC client to the routers and assemblers |
| `transactions`    | The number of transactions to be sent                                                                                 |
| `rate`            | The number of transactions per second to be sent                                                                      |
| `txSize`          | The required transaction size in bytes                                                                                |

###
##### Example:

Running
`./bin/armageddon load --config=arma-config/config/party1/user_config.yaml --transactions=1000 --rate=500 --txSize=32` involves:
1) Creating a GRPC client for the router.
2) Preparing synthetic transactions of the specified size.
3) Sending these transactions to all router nodes using the Broadcast API at the specified rate.

NOTE: This command allows for variable transaction rates. For example, specifying transactions=100 and rates="500 1000" will send 100 transactions at a rate of 500, followed by another 100 transactions at a rate of 1000.

##
### Receive Command
1. Run from `ARMA` root folder.
2. Run: `./bin/armageddon receive [args]`.

   Replace `[args]` with corresponding flags.

###
##### Flags
| Flags               | Description                                                                                                                                                             |
|---------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `config`            | The absolute or relative path of the user configuration used for defining a GRPC client to the routers and assemblers                                                   |
| `expectedTxs`       | The expected number of transactions the assembler should receive. If missing, the default value expectedTxs=-1 is taken, leading to an endless execution of the command |
| `output`            | The absolute or relative path to which `statistics.csv` file is written. If missing, statistics will be saved under current directory                                   |
| `pullFromPartyId`   | The party id of the assembler to pull blocks from                                                                                                                       |


###
##### Example:

Running
`./bin/armageddon receive --config=arma-config/config/party1/user_config.yaml --pullFromPartyId=1 --expectedTxs=1000 --output=stats` involves:
1) Creating a GRPC client for the assembler.
2) Pulling blocks from the assembler endlessly or until an amount of `expectedTxs` is received.
3) Collecting statistics on blocks and output them to a statistics.csv file under `output`.

NOTE:
It is recommended to run the `receive` command first to start waiting for blocks and then run `load` to send transactions.

##
### createSharedConfigProto Command
1. Run from `ARMA` root folder.
2. Run: `./bin/armageddon createSharedConfigProto [args]`.

   Replace `[args]` with corresponding flags.

###
##### Flags
| Flags                              | Description                                                                                                                                                           |
|------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `sharedConfigYaml`                 | The absolute or relative path of the shared configuration YAML file                                                                                                   |
| `output`                           | The absolute or relative path to which the config block will be saved.                                                                                                |


###
##### Example:

Running
`./bin/armageddon createSharedConfigProto --sharedConfigYaml=arma-config/bootstrap/shared_config.yaml --output=arma-shared-config` involves:
1) Reading the shared configuration.
2) Creating `shared_config.binpb` and write it under the output directory.
   
For more details on the structure of the config block, see [config block](#config-block).
##
<a id="config-block"></a>
### Config block
// TODO: add details
