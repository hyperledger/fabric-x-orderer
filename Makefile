
# This makefile defines the following targets

#   - check-deps - check for vendored dependencies that are no longer used
#   - linter - runs all code checks




.PHONY: linter
linter: check-deps
	@echo "LINT: Running code checks.."
	./scripts/golinter.sh

.PHONY: check-deps
check-deps:
	@echo "DEP: Checking for dependency issues.."
	./scripts/check_deps.sh

