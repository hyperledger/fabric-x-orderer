# This makefile defines the following targets

#   - check-deps: check for vendored dependencies that are no longer used
#   - linter: runs all code checks
#   - binary: compiles arma and tools (armageddon) into ./bin directory
#   - clean-binary: removes all contents of the ./bin directory
#   - protos: generate all protobuf artifacts based on .proto files

.PHONY: linter
linter: check-deps
	@echo "LINT: Running code checks.."
	golangci-lint run --color=always --sort-results --new-from-rev=main

.PHONY: check-deps
check-deps:
	@echo "DEP: Checking for dependency issues.."
	./scripts/check_deps.sh

.PHONY: binary
binary:
	mkdir -p ./bin
	go build -o ./bin/arma ./node/cmd/arma/main
	go build -o ./bin/armageddon ./node/cmd/armageddon/main

.PHONY: clean-binary
clean-binary:
	rm -rf ./bin/*

.PHONY: protos
protos: 
	@echo "Compiling non-API protos..."
	(cd node && bash ./scripts/compile_protos.sh)
