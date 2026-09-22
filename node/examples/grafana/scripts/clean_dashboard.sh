#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eu

DOCKER_CMD=${DOCKER_CMD:-docker}
EXAMPLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

$DOCKER_CMD compose \
    -f "${EXAMPLE_DIR}/../compose.yaml" \
    -f "${EXAMPLE_DIR}/compose.yaml" \
    --profile client down --volumes --remove-orphans

rm -rf /tmp/arma-sample
