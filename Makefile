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

TAG ?= latest
DOCKERFILE ?= images/multi-platform/Dockerfile
DOCKER_IMAGE ?= arma

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

.PHONY: docker
docker:
	docker build -t $(DOCKER_IMAGE) --no-cache -f $(DOCKERFILE) .

# Build for current OS/architecture
.PHONY: docker-local
docker-local:
	@echo "Building local Docker image: $(DOCKER_IMAGE):$(TAG)"
	docker build \
		-f images/multi-platform/Dockerfile \
		-t $(DOCKER_IMAGE):$(TAG) \
		.

.PHONY: docker-multiarch
docker-multiarch:
	@echo "Building images for all platforms locally..."
	make docker-linux-amd64
	make docker-linux-arm64
	make docker-linux-s390x

.PHONY: docker-linux-amd64
docker-linux-amd64:
	@echo "Building linux/amd64"
	docker buildx build --platform linux/amd64 -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-amd64 --load .

.PHONY: docker-linux-arm64
docker-linux-arm64:
	@echo "Building linux/arm64"
	docker buildx build --platform linux/arm64 -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-arm64 --load .

.PHONY: docker-linux-s390x
docker-linux-s390x:
	@echo "Building linux/s390x"
	docker buildx build --platform linux/s390x -f $(DOCKERFILE) -t $(DOCKER_IMAGE):linux-s390x --load .









