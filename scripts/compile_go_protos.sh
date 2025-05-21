#!/bin/sh
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

proto_files="\
 node/protos/comm/communication.proto \
 node/comm/testdata/grpc/test.proto \
 config/protos/configuration.proto \
 common/ledger/blkstorage/storage.proto"


cd /mnt
for f in ${proto_files}; do
    echo "protoc compiling:" $f
    protoc "--go_out=plugins=grpc,paths=source_relative:." $f
done

echo "Done"