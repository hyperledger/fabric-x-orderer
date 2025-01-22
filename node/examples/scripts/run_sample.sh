#!/usr/bin/env bash
set -eux
make binary

./bin/armageddon generate --config="node/examples/config/example-deployment.yaml" --output="/tmp/arma-sample/arma-config" --useTLS  --version=1
cd node/examples && docker-compose up -d  
sleep 10

docker run --name arma-config-vol -it --mount type=bind,source=/tmp/arma-sample/arma-config,target=/arma-config --entrypoint /usr/local/bin/armageddon --network examples_default arma submit --config /arma-config/Party1/user_config.yaml --transactions 1000 --rate 500 --txSize 64
