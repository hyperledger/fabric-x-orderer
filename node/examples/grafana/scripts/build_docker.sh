#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eu

# The same "arma" image the other examples use.
cd "$(dirname "${BASH_SOURCE[0]}")/../../../.."
${DOCKER_CMD:-docker} build --target=arma --tag=arma -f node/examples/Dockerfile .
