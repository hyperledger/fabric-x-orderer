#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

make binary

./bin/armageddon generate --config="node/examples/config/example-deployment.yaml" --output="/tmp/arma-sample"
cd node/examples && docker compose up -d
sleep 10

docker run --name arma-config-vol -t --mount type=bind,source=/tmp/arma-sample/config,target=/config --mount type=bind,source=/tmp/arma-sample/crypto,target=/tmp/arma-sample/crypto --entrypoint /usr/local/bin/armageddon --network examples_default arma submit --config /config/party1/user_config.yaml --transactions 1000 --rate 500 --txSize 64