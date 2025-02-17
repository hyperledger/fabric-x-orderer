#!/bin/bash


ARMA_EXEC="/home/yacovm/gopath/src/cmd/arma/main/arma"
CLIENT_EXEC="/home/yacovm/gopath/src/github.ibm.com/decentralized-trust-research/orderingservice-experiments/clients/cmd/client"

	hosts=("arma1.sl.cloud9.ibm.com"
    "consensus1.sl.cloud9.ibm.com"
    "router1.sl.cloud9.ibm.com"
    "batcher11.sl.cloud9.ibm.com"
    "batcher12.sl.cloud9.ibm.com"
    "batcher13.sl.cloud9.ibm.com"
    "batcher14.sl.cloud9.ibm.com"
    "consensus2.sl.cloud9.ibm.com"
    "router2.sl.cloud9.ibm.com"
    "batcher21.sl.cloud9.ibm.com"
    "batcher22.sl.cloud9.ibm.com"
    "batcher23.sl.cloud9.ibm.com"
    "batcher24.sl.cloud9.ibm.com"
    "consensus3.sl.cloud9.ibm.com"
    "router3.sl.cloud9.ibm.com"
    "batcher31.sl.cloud9.ibm.com"
    "batcher32.sl.cloud9.ibm.com"
    "batcher33.sl.cloud9.ibm.com"
    "batcher34.sl.cloud9.ibm.com"
    "consensus4.sl.cloud9.ibm.com"
    "router4.sl.cloud9.ibm.com"
    "batcher41.sl.cloud9.ibm.com"
    "batcher42.sl.cloud9.ibm.com"
    "batcher43.sl.cloud9.ibm.com"
    "batcher44.sl.cloud9.ibm.com")

        routers=(
    "router1.sl.cloud9.ibm.com"
    "router2.sl.cloud9.ibm.com"
    "router3.sl.cloud9.ibm.com"
    "router4.sl.cloud9.ibm.com")


        consenters=(
    "consensus1.sl.cloud9.ibm.com"
    "consensus2.sl.cloud9.ibm.com"
    "consensus3.sl.cloud9.ibm.com"
    "consensus4.sl.cloud9.ibm.com")

     batchers=(
    "batcher11.sl.cloud9.ibm.com"
    "batcher12.sl.cloud9.ibm.com"
    "batcher13.sl.cloud9.ibm.com"
    "batcher14.sl.cloud9.ibm.com"
    "batcher21.sl.cloud9.ibm.com"
    "batcher22.sl.cloud9.ibm.com"
    "batcher23.sl.cloud9.ibm.com"
    "batcher24.sl.cloud9.ibm.com"
    "batcher31.sl.cloud9.ibm.com"
    "batcher32.sl.cloud9.ibm.com"
    "batcher33.sl.cloud9.ibm.com"
    "batcher34.sl.cloud9.ibm.com"
    "batcher41.sl.cloud9.ibm.com"
    "batcher42.sl.cloud9.ibm.com"
    "batcher43.sl.cloud9.ibm.com"
    "batcher44.sl.cloud9.ibm.com")

function copy_binaries() {
pids=""
i=0
for host in ${hosts[*]}; do
	host=yac-$host
	scp $ARMA_EXEC root@$host: &
	pids[${i}]=$!
	(( i += 1 ))
done


for pid in ${pids[*]}; do
	wait $pid
done
}

function copy_start_batchers {
for host in ${batchers[*]}; do
        host=yac-$host
	index=$( echo "$host" | cut -c13 )
	config="batcher_node_${index}_config.yaml"
	cat << EOF > start.sh
#!/bin/bash
chmod u+x arma
./arma batcher --config $config &> out.log &
EOF
	scp start.sh root@$host:
done
}

function copy_start_consenters {
for host in ${consenters[*]}; do
        host=yac-$host
        config="consenter_node_config.yaml"
        cat << EOF > start.sh
#!/bin/bash
chmod u+x arma
./arma consensus --config $config &> out.log &
EOF
        scp start.sh root@$host:
done
}

function copy_start_routers {
for host in ${routers[*]}; do
        host=yac-$host
        config="router_node_config.yaml"
        cat << EOF > start.sh
#!/bin/bash
chmod u+x arma
./arma router --config $config &> out.log &
EOF
        scp start.sh root@$host:
done
}

function copy_start_assemblers {
for host in arma1.sl.cloud9.ibm.com ; do
        host=yac-$host
        config="assembler_node_config.yaml"
        cat << EOF > start.sh
#!/bin/bash
chmod u+x arma
./arma assembler --config $config &> out.log &
EOF
        scp start.sh root@$host:
done
}

function copy_config() {
for i in 1 2 3 4; do for j in 1 2 3 4; do scp arma-config/Party$i/batcher_node_${j}_config.yaml root@yac-batcher$i$j.sl.cloud9.ibm.com:; done; done
for i in 1; do scp arma-config/Party${i}/assembler_node_config.yaml root@yac-arma${i}.sl.cloud9.ibm.com: ; done
for i in 1 2 3 4; do scp arma-config/Party${i}/router_node_config.yaml root@yac-router${i}.sl.cloud9.ibm.com: ; done
for i in 1 2 3 4 ; do scp arma-config/Party${i}/consenter_node_config.yaml root@yac-consensus${i}.sl.cloud9.ibm.com: ; done
}

function copy_start() {
	copy_start_assemblers
	copy_start_routers
	copy_start_consenters
	copy_start_batchers
}


function launch() {
pids=""
i=0

for host in ${hosts[*]}; do
        host=yac-$host
        echo "Starting" $host "..."
	ssh root@$host "bash start.sh" &
        pids[${i}]=$!
        (( i += 1 ))
done

for pid in ${pids[*]}; do
        wait $pid
done
}


function kill() {
pids=""
i=0
for host in ${hosts[*]}; do
        host=yac-$host
        echo "cleaning" $host "..."
        ssh root@$host "pkill arma || (rm -rf chains && rm -rf index && rm -rf batchDB && rm -rf wal)" &
	pids[${i}]=$!
	(( i += 1 ))
done

for pid in ${pids[*]}; do
        wait $pid
done
}

kill
kill
copy_binaries
launch
