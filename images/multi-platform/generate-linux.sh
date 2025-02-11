#!/bin/bash

set -e


if [[ $# -ne 1 ]]; then
  echo "Usage:"
  echo $0 " <version>"
  echo " <version>: version number without \"v\", e.g.: 0.0.7"
  exit 2
fi

echo "=== Generating images for version: " $1
arma_ver=$1

echo "=== Checking buildx container"
docker buildx inspect --bootstrap
echo

echo "=== Building for s390x"
docker buildx build --platform linux/s390x --file Dockerfile --output type=docker,push=false,name=arma-node:linux-s390x-$arma_ver,dest=arma-node_linux-s390x-$arma_ver.tar ../../
echo

echo "=== Building for amd64"
docker buildx build --platform linux/amd64 --file Dockerfile --output type=docker,push=false,name=arma-node:linux-amd64-$arma_ver,dest=arma-node_linux-amd64-$arma_ver.tar ../../
echo

echo "=== Building for arm64"
docker buildx build --platform linux/arm64 --file Dockerfile --output type=docker,push=false,name=arma-node:linux-arm64-$arma_ver,dest=arma-node_linux-arm64-$arma_ver.tar ../../
echo

echo "=== Loading images to local container registry"
docker image load -i arma-node_linux-s390x-$arma_ver.tar
docker image load -i arma-node_linux-amd64-$arma_ver.tar
docker image load -i arma-node_linux-arm64-$arma_ver.tar
echo

echo "===Re-tag images to ibm cloud container registry"
docker image tag arma-node:linux-s390x-$arma_ver icr.io/cbdc/arma-node:linux-s390x-$arma_ver
docker image tag arma-node:linux-amd64-$arma_ver icr.io/cbdc/arma-node:linux-amd64-$arma_ver
docker image tag arma-node:linux-arm64-$arma_ver icr.io/cbdc/arma-node:linux-arm64-$arma_ver
echo

docker images |grep $arma_ver

echo
echo "=== Done!"
echo "Push to icr.io manually!!!"
echo "Do:"
echo
echo "docker image push icr.io/cbdc/arma-node:linux-s390x-"$arma_ver
echo "docker image push icr.io/cbdc/arma-node:linux-amd64-"$arma_ver
echo "docker image push icr.io/cbdc/arma-node:linux-arm64-"$arma_ver
