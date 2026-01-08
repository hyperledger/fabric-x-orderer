/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

//go:generate protoc --proto_path=./testdata/grpc --go_out=./testdata/grpc --go_opt=paths=source_relative  ./testdata/grpc/test.proto

package comm_test
