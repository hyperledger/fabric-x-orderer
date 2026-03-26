#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

BASE=/tmp/arma-all-in-one

echo "Stopping all-in-one container..."

docker ps -q --filter "ancestor=arma-4p1s" | xargs -r docker stop

echo "Removing container..."

docker ps -aq --filter "ancestor=arma-4p1s" | xargs -r docker rm

echo "Fixing permissions..."

if [ -d "${BASE}" ]; then
  docker run --rm \
    -v ${BASE}:${BASE} \
    alpine \
    sh -c "chmod -R u+w ${BASE}"
fi

echo "Removing storage..."

rm -rf ${BASE}

echo "Clean done."