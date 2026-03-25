#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eux

REPO_ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd ${REPO_ROOT}

echo "Building Docker image..."

docker build -t arma-4p1s -f node/examples/all-in-one/Dockerfile .

echo "Build done."