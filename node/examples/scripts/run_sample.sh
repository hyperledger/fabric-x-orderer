#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

make binary

./bin/armageddon generate --config="node/examples/config/example-deployment.yaml" --output="/tmp/arma-sample"

# Expose a fixed operations port so the compose healthcheck can reach /healthz.
for config in /tmp/arma-sample/config/party*/local_config_*.yaml; do
  awk '
    /ListenAddress: 127.0.0.1/ {
      print "    ListenAddress: 0.0.0.0"
      print "    ListenPort: 8080"
      next
    }
    { print }
  ' "${config}" > "${config}.new"
  mv "${config}.new" "${config}"
done

cd node/examples && docker compose up -d --wait

docker run --name arma-config-vol -t --mount type=bind,source=/tmp/arma-sample/config,target=/config --mount type=bind,source=/tmp/arma-sample/crypto,target=/tmp/arma-sample/crypto --entrypoint /usr/local/bin/armageddon --network examples_default arma submit --config /config/party1/user_config.yaml --transactions 1000 --rate 500 --txSize 64