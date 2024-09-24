#!/bin/bash -e

docker build -t protobuilder -f scripts/Dockerfile .
docker run  --user $(id -u):$(id -g) -v `pwd`:/mnt protobuilder /bin/sh /mnt/scripts/compile_go_protos.sh
