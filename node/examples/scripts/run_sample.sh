#!/usr/bin/env bash
set -eux

docker-compose up -d

sleep 10

docker run -it --entrypoint "/usr/local/bin/armageddon" --network examples_default arma submit --user_config arma-config/Party1/user_config.yaml

