#!/usr/bin/env bash

#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

# Generates a THIRD-PARTY-NOTICES file by collecting every LICENSE, COPYING,
# and NOTICE file found in vendor/ (excluding the project's own module).
#
# The output is a single concatenated file with a header per dependency,
# suitable for inclusion in binary redistributions to satisfy MIT, BSD, and
# similar attribution requirements.
#
# Usage:
#   ./scripts/generate-third-party-notices.sh [OUTPUT_FILE]
#
# OUTPUT_FILE defaults to THIRD-PARTY-NOTICES in the repo root.

set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

OUTPUT="${1:-THIRD-PARTY-NOTICES}"
OWN_MODULE="github.com/hyperledger/fabric-x-orderer"

: > "${OUTPUT}"

{
  echo "This distribution contains third-party software with the following licenses."
  echo "The full text of each license is reproduced below."
  echo ""
} >> "${OUTPUT}"

# Find all license/notice files under vendor/, one level per module directory.
# vendor/<host>/<org>/<repo>/... — we collect files at any depth but deduplicate
# by the module root (the path segment that appears in vendor/modules.txt).
while IFS= read -r -d '' license_file; do
  # Strip leading "vendor/" to get the module-relative path.
  rel="${license_file#vendor/}"

  # Skip files belonging to the project's own module.
  if [[ "${rel}" == "${OWN_MODULE}"* ]]; then
    continue
  fi

  # Use the file path as the section header.
  {
    echo "========================================================================"
    echo "Module: ${rel%/*}"
    echo "File:   ${rel}"
    echo "------------------------------------------------------------------------"
    cat "${license_file}"
    echo ""
  } >> "${OUTPUT}"

done < <(find vendor -type f \( \
    -iname 'LICENSE' -o -iname 'LICENSE.txt' -o -iname 'LICENSE.md' \
    -o -iname 'LICENCE' -o -iname 'LICENCE.txt' \
    -o -iname 'COPYING' -o -iname 'COPYING.txt' \
    -o -iname 'NOTICE'  -o -iname 'NOTICE.txt' \
  \) -print0 | sort -z)

echo "Generated ${OUTPUT} ($(wc -l < "${OUTPUT}") lines)"
