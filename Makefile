# This makefile defines the following targets

#   - check-deps: check for vendored dependencies that are no longer used
#   - linter: runs all code checks
#   - binary: compiles arma and tools (armageddon) into ./bin directory
#   - clean-binary: removes all contents of the ./bin directory.

# .PHONY: linter check-deps binary clean-binary
.PHONY: linter
linter: check-deps
	@echo "LINT: Running code checks.."
	./scripts/golinter.sh

.PHONY: check-deps
check-deps:
	@echo "DEP: Checking for dependency issues.."
	./scripts/check_deps.sh

binary:
	mkdir -p ./bin
	go build -o ./bin/arma ./node/cmd/arma/main/main.go
	go build -o ./bin/armageddon ./node/cmd/armageddon/main

clean-binary:
	rm -rf ./bin/*
