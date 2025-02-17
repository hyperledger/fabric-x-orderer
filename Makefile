# This makefile defines the following targets

#   - check-deps: check for vendored dependencies that are no longer used
#   - linter: runs all code checks
#   - binary: compiles arma and tools (armageddon) into ./bin directory
#   - clean-binary: removes all contents of the ./bin directory
#   - protos: generate all protobuf artifacts based on .proto files
#   - linter-extra: runs extra lint checks on new changes since 'main'

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
	go build -o ./bin/arma ./cmd/arma/main
	go build -o ./bin/armageddon ./cmd/armageddon/main

.PHONY: clean-binary
clean-binary:
	rm -rf ./bin/*

.PHONY: protos
protos: 
	@echo "Compiling non-API protos..."
	(cd node && bash ./scripts/compile_protos.sh)

.PHONY: linter-extra
linter-extra: check-deps
	@echo "LINT-Extra: Running code checks.."
	@! golangci-lint run --color=always --sort-results --new-from-rev=main 2>&1 | tee /dev/tty | grep -qE "(gofmt|goimports|misspell|whitespace|gocritic)" || \
	echo "\nRun 'golangci-lint run --fix --new-from-rev=main' to fix issues"
