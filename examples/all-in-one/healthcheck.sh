#!/bin/sh
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

BASE_DIR=/tmp/arma-all-in-one

# Checks /healthz of the given node configs in all 4 parties; nodes without a fixed Operations ListenPort are skipped
for name in "$@"; do
  for i in 1 2 3 4; do
    port=$(awk '/^Operations:/ {ops = 1; next} /^[^ ]/ {ops = 0} ops && /ListenPort:/ {print $2}' "${BASE_DIR}/config/party${i}/${name}") || exit 1
    if [ -n "${port}" ] && [ "${port}" != 0 ]; then
      curl -sf "http://127.0.0.1:${port}/healthz" > /dev/null || exit 1
    fi
  done
done
