#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

EXAMPLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

cd "${EXAMPLE_DIR}" && docker compose down
docker stop "arma-config-vol"
docker rm "arma-config-vol"

cd .. && rm -rf "/tmp/arma-sample"
make clean-binary
