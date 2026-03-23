#!/usr/bin/env bash
set -eux

BASE=/tmp/arma-all-in-one

rm -rf ${BASE}
mkdir -p ${BASE}/storage

for i in 1 2 3 4; do
  mkdir -p ${BASE}/storage/party${i}/{router,assembler,batcher,consenter}
done

# 🔥 FIXED PATH (4 levels up)
REPO_ROOT=$(cd "$(dirname "$0")/../../../.." && pwd)
cd ${REPO_ROOT}

docker build -t arma-4p1s -f node/examples/all-in-one/Dockerfile .

cd node/examples/all-in-one/scripts

docker run -it \
  -p 6022:6022 -p 6023:6023 \
  -p 6122:6122 -p 6123:6123 \
  -p 6222:6222 -p 6223:6223 \
  -p 6322:6322 -p 6323:6323 \
  -v ${BASE}/storage:/storage \
  -v $(pwd)/../config:/config \
  arma-4p1s