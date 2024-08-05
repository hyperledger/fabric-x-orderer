#!/usr/bin/env bash
set -eux

cd node/examples && docker-compose down
docker stop "arma-config-vol"
docker rm "arma-config-vol"

cd ../../ && rm -rf "/tmp/arma-sample"
make clean-binary
