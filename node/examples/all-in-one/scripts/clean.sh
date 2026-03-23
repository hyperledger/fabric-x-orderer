#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

echo "Stopping all-in-one container..."

# stop running container (if exists)
docker ps -q --filter "ancestor=arma-4p1s" | xargs -r docker stop

echo "Removing container..."

# remove stopped container
docker ps -aq --filter "ancestor=arma-4p1s" | xargs -r docker rm

echo "Removing storage..."

rm -rf /tmp/arma-all-in-one

echo "Clean done."