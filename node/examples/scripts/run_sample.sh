#!/usr/bin/env bash
set -eux

cd ../cmd/armageddon/main && go build -o armageddon
./armageddon generate --config="../../../examples/config/example-deployment.yaml"

cd ../../../examples && docker-compose up -d  
sleep 10

docker volume create --name arma-config-vol --opt type=none --opt device=$(pwd)/../cmd/armageddon/main/arma-config --opt o=bind
docker run -it --mount source=arma-config-vol,target=/arma-config --entrypoint /usr/local/bin/armageddon --network examples_default arma submit --config /arma-config/Party1/user_config.yaml --transactions 1000 --rate 500