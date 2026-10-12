#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

EXAMPLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "${EXAMPLE_DIR}/.."

make binary

./bin/armageddon generate --config="${EXAMPLE_DIR}/config/example-deployment.yaml" --output="/tmp/arma-sample"
cd "${EXAMPLE_DIR}" && docker compose up -d
sleep 10

docker run --name arma-config-vol -t --mount type=bind,source=/tmp/arma-sample/config,target=/config --mount type=bind,source=/tmp/arma-sample/crypto,target=/tmp/arma-sample/crypto --entrypoint /usr/local/bin/armageddon --network examples_default arma submit --config /config/party1/user_config.yaml --transactions 1000 --rate 500 --txSize 64