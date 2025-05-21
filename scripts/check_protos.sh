#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

output=$(make protos 2>&1)
if [ $? -ne 0 ]; then 
    echo "$output"
    exit 1 
fi 

git diff --quiet --exit-code -- '*.pb.go' || {
    echo "The following .pb.go files have been modified after compilation:"
    git diff --name-only -- '*.pb.go'
    exit 1
}
