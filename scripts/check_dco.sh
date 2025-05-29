#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

git fetch origin main > /dev/null 2>&1

COMMITS=$(git rev-list --no-merges origin/main..HEAD)

for COMMIT in $COMMITS; do
  if ! git show --quiet "$COMMIT" | grep -q "Signed-off-by:"; then
    MSG=$(git log -1 --pretty=format:"Commit %h (\"%s\")" "$COMMIT")
    echo "$MSG is missing Signed-off-by."
    exit 1
  fi
done

exit 0
