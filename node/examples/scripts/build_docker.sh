#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -eux

DOCKER_CMD=${DOCKER_CMD:-docker}

$DOCKER_CMD build --target=arma --tag=arma -f ./Dockerfile ../../
