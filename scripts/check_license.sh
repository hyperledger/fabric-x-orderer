#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
set -euo pipefail

source "$(cd "$(dirname "$0")" && pwd)/functions.sh"

REQUIRED_HEADER="SPDX-License-Identifier: Apache-2.0"

CHECK=$(git diff --name-only --diff-filter=ACMRTUXB HEAD | tr '\n' ' ')
if [[ -z "$CHECK" ]]; then
    CHECK=$(git diff-tree --no-commit-id --name-only --diff-filter=ACMRTUXB -r "HEAD^..HEAD" | tr '\n' ' ')
fi

FILTERED=$(filterExcludedAndGeneratedFiles $CHECK)
[[ -z "$FILTERED" ]] && exit 0

missing=$(echo "$FILTERED" | xargs ls -d 2>/dev/null | xargs grep -L "$REQUIRED_HEADER" || true)
[[ -z "$missing" ]] && exit 0

echo "The following files are missing headers:"
echo "$missing"
exit 1
