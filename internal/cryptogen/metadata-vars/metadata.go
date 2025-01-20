/*
Copyright London Stock Exchange 2016 All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package metadata

// This code originates from hyperledger fabric project. Source: https://github.com/hyperledger/fabric/blob/main/common/metadata/metadata.go

// Variables defined by the Makefile and passed in with ldflags
var (
	Version         = "latest"
	CommitSHA       = "development build"
	BaseDockerLabel = "org.hyperledger.fabric"
	DockerNamespace = "hyperledger"
)
