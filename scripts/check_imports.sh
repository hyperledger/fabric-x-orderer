#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Check that protoutil is imported from fabric-x-common only
FABRIC_PROTOUTIL="github.com/hyperledger/fabric/protoutil"
EXCLUDED="^vendor/|node/delivery/deliver_service.go|node/batcher/batcher_deliver_service.go|node/comm/util.go"

CHECK=$(git diff --name-only --diff-filter=ACMRTUXB origin/main...HEAD)

CHECK=$(echo "$CHECK" | grep '\.go$' | grep -Ev "$EXCLUDED" || true)
[[ -z "$CHECK" ]] && exit 0

found=$(echo "$CHECK" | xargs grep -n "$FABRIC_PROTOUTIL" || true)
[[ -z "$found" ]] && exit 0

echo "The following files import $FABRIC_PROTOUTIL:"
echo "$found"
echo "Use github.com/hyperledger/fabric-x-common/protoutil instead."

exit 1
