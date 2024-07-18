#!/usr/bin/env bash
set -eux

DOCKER_CMD=${DOCKER_CMD:-docker}

$DOCKER_CMD build --target=arma --tag=arma -f ./Dockerfile ../../
