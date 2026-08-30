#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

# Check that packages are not imported from github.com/hyperledger/fabric/
FABRIC_IMPORT="github.com/hyperledger/fabric/"
FABRIC_PROTOUTIL="github.com/hyperledger/fabric/protoutil"

found=$(grep -rn --include='*.go' --exclude-dir=vendor "\"${FABRIC_IMPORT}" . || true)
[[ -z "$found" ]] && exit 0

echo "The following files import from $FABRIC_IMPORT:"
echo "$found"
echo "Use github.com/hyperledger/fabric-x-common (or fabric-lib-go / fabric-protos-go-apiv2) instead."

if echo "$found" | grep -q "$FABRIC_PROTOUTIL"; then
    echo "Use github.com/hyperledger/fabric-x-common/protoutil instead."
fi

exit 1
