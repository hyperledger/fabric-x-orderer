#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

BASE=/tmp/arma-all-in-one
HOST_UID=$(id -u)
HOST_GID=$(id -g)

echo "Stopping all-in-one container..."

docker ps -q --filter "ancestor=arma-4p1s" | xargs -r docker stop

echo "Removing container..."

docker ps -aq --filter "ancestor=arma-4p1s" | xargs -r docker rm

echo "Fixing ownership and permissions..."

if [ -d "${BASE}" ]; then
  docker run --rm \
    -u 0:0 \
    -v "${BASE}:${BASE}" \
    alpine \
    sh -c "chown -R ${HOST_UID}:${HOST_GID} '${BASE}' && chmod -R u+rwX '${BASE}'"
fi

echo "Removing storage..."

rm -rf "${BASE}"

echo "Clean done."