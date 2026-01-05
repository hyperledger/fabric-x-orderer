# Copyright IBM Corp All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

# This makefile defines the following targets

#   - check-deps: check for vendored dependencies that are no longer used
#   - linter: runs all code checks
#   - binary: compiles arma and tools (armageddon) into ./bin directory
#   - clean-binary: removes all contents of the ./bin directory
#   - protos: generate all protobuf artifacts based on .proto files
#   - linter-extra: runs extra lint checks on new changes since 'main'
#   - check-license: checks files for Apache license header
# 	- check-dco: check that commits include Signed-off-by
#	- docker-local: builds a single-platform image for the host’s OS/architecture
#   - docker-multiarch: wrapper that triggers docker builds for multiple platforms

# Docker image vars
DOCKERFILE ?= images/multi-platform/Dockerfile
IMAGE_NAMESPACE = docker.io/hyperledger
IMAGE_NAME = fabric-x-orderer
VERSION = latest

.PHONY: basic-checks
basic-checks: check-license check-dco check-protos linter

.PHONY: linter
linter: check-deps
	@echo "LINT: Running code checks.."
	./scripts/golinter.sh

.PHONY: check-deps
check-deps:
	@echo "DEP: Checking for dependency issues.."
	./scripts/check_deps.sh

.PHONY: binary
binary:
	mkdir -p ./bin
	go build -o ./bin/arma ./cmd/arma
	go build -o ./bin/armageddon ./cmd/armageddon

.PHONY: clean-binary
clean-binary:
	rm -rf ./bin/*

.PHONY: protos
protos: 
	@echo "Compiling non-API protos..."
	(bash ./scripts/compile_protos.sh)

.PHONY: linter-extra
linter-extra: check-deps
	@echo "LINT-Extra: Running code checks.."
	@! golangci-lint run --color=always --sort-results --new-from-rev=main 2>&1 | tee /dev/tty | grep -qE "(gofmt|goimports|misspell|whitespace|gocritic)" || \
	echo "\nRun 'golangci-lint run --fix --new-from-rev=main' to fix issues"

.PHONY: check-license
check-license: 
	@echo "Checking license headers..."
	@./scripts/check_license.sh

.PHONY: check-dco
check-dco: 
	@echo "Checking DCO..."
	@./scripts/check_dco.sh

.PHONY: check-protos
check-protos:
	@echo "Checking protos..."
	@./scripts/check_protos.sh

.PHONY: unit-tests
unit-tests:
	go test -race -timeout 20m ./...

.PHONY: sample-tests
sample-tests:
	set -e
	(cd node/examples; bash ./scripts/build_docker.sh)
	(bash ./node/examples/scripts/run_sample.sh)

# Build the HLFX Orderer image
.PHONY: build-image
build-image:
	@echo "Building the image ${IMAGE_NAMESPACE}/${IMAGE_NAME}:${VERSION}..."
	@./scripts/build_image.sh -t ${IMAGE_NAMESPACE}/${IMAGE_NAME}:${VERSION} -f ${DOCKERFILE} --build-arg VERSION=${VERSION}

# Build the HLFX Orderer multiplatform image
.PHONY: build-multiplatform-image
build-multiplatform-image:
	@echo "Building the multiplatform image ${IMAGE_NAMESPACE}/${IMAGE_NAME}:${VERSION}..."
	@./scripts/build_image.sh -t ${IMAGE_NAMESPACE}/${IMAGE_NAME}:${VERSION} -f ${DOCKERFILE} --multiplatform --build-arg VERSION=${VERSION}