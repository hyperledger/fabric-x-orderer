#!/bin/bash -e
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

docker build -t protobuilder -f scripts/Dockerfile .
docker run  --user $(id -u):$(id -g) -v `pwd`:/mnt protobuilder /bin/sh /mnt/scripts/compile_go_protos.sh
