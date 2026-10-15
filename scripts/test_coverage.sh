#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -euo pipefail

MODULE="github.com/hyperledger/fabric-x-orderer"

# Run tests and collect raw coverage data.
if [[ "$1" == run ]]; then
    mkdir -p "$3"

    gotmp=$(mktemp -d)
    export GOTMPDIR="$gotmp"
    export GOFLAGS="-cover -covermode=atomic -coverpkg=${MODULE}/... -work -count=1"

    status=0
    make "$2" || status=$?

    find "$gotmp" -type f -name 'covcounters.*' -exec dirname {} \; |
        sort -u |
        while read -r dir; do
            cp "$dir"/cov* "$3"/
        done

    rm -rf "$gotmp"

    if [[ "$status" -eq 0 ]] && ! ls "$3"/covcounters.* >/dev/null 2>&1; then
        echo "error: no coverage data produced" >&2
        exit 1
    fi

    exit "$status"
fi

# Generate a coverage profile and report from the collected data.
coverdata_root="$2"
profile="$3"

inputs=$(find "$coverdata_root" -type f -name 'covcounters.*' -exec dirname {} \; |
    sort -u | paste -sd, -)

go tool covdata textfmt -i="$inputs" -o="$profile"

filtered=$(mktemp)

sed -E -f - "$profile" > "$filtered" <<'EOF'
# Kept: test helpers that are linked into the shipped armageddon binary.
/\/testutil\/(fabric|tx|signutil)\//b
/\/cmd\/.*\/main\.go/d
/\.pb\.go/d
/\/(mocks?|fakes?)\//d
/\/test\/utils\//d
/\/testutil\//d
/\/node\/comm\/tlsgen\//d
EOF
mv "$filtered" "$profile"

lines=$(awk '
NR == 1 { print; next }
{
    split($1, location, ":")
    split(location[2], range, "[.,]")
    for (line = range[1] + 0; line <= range[3] + 0; line++) {
        key = location[1] ":" line
        covered[key] = covered[key] || $3 > 0
    }
}
END {
    for (key in covered) {
        print key, 1, covered[key]
    }
}
' "$profile")

total=$(awk 'NR > 1 { n++; c += $3 } END { printf "%.2f%%", n ? 100 * c / n : 0 }' <<< "$lines")

report() {
    echo "## Test coverage"
    echo
    echo "**Total: $total**"
    echo
    echo "| Package | Coverage |"
    echo "| --- | ---: |"

    awk '
    NR == 1 { next }
    {
        split($1, location, ":")
        pkg = location[1]
        sub("/[^/]*$", "", pkg)
        total[pkg] += $2
        if ($3 > 0) {
            covered[pkg] += $2
        }
    }
    END {
        for (pkg in total) {
            printf "%s\t%.1f%%\n", pkg, 100 * covered[pkg] / total[pkg]
        }
    }
    ' <<< "$lines" |
        sort |
        awk -F'\t' -v module="${MODULE}/" '
        {
            sub("^" module, "", $1)
            printf "| %s | %s |\n", $1, $2
        }'
}

if [[ -n "${GITHUB_STEP_SUMMARY:-}" ]]; then
    report | tee -a "$GITHUB_STEP_SUMMARY"
else
    report
fi
