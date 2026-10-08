#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -eux

DOCKER_CMD=${DOCKER_CMD:-docker}
EXAMPLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)

$DOCKER_CMD build --target=arma --tag=arma -f "${EXAMPLE_DIR}/Dockerfile" "${EXAMPLE_DIR}/.."
