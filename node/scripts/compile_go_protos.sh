#!/bin/sh

cd /mnt
protoc "--go_out=plugins=grpc,paths=source_relative:." protos/comm/communication.proto