#!/bin/bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -euo pipefail

MODULE="github.com/hyperledger/fabric-x-orderer"

if [[ $# -ne 3 || ( "$1" != run && "$1" != report ) ]]; then
    echo "usage: $0 run <make-target> <coverdir>" >&2
    echo "       $0 report <coverdata-root> <coverage-profile>" >&2
    exit 2
fi

if [[ "$1" == run ]]; then
    target="$2"
    mkdir -p "$3"
    coverdir=$(cd "$3" && pwd)
    gotmp=$(mktemp -d)

    export GOTMPDIR="$gotmp"
    export GOFLAGS="-cover -covermode=atomic -coverpkg=${MODULE}/... -work -count=1"

    harvest() {
        [[ -d "$gotmp" ]] || return 0
        find "$gotmp" -type f -name 'covcounters.*' -exec dirname {} \; |
            sort -u |
            while read -r dir; do cp "$dir"/cov* "$coverdir"/ || exit 1; done &&
            rm -rf "$gotmp"
    }

    trap harvest EXIT INT TERM

    status=0
    make "$target" || status=$?

    harvest || { [[ "$status" -ne 0 ]] || status=1; }
    trap - EXIT INT TERM

    if [[ "$status" -eq 0 ]] && ! ls "$coverdir"/covcounters.* >/dev/null 2>&1; then
        echo "error: ${target} produced no coverage data" >&2
        exit 1
    fi

    exit "$status"
fi

coverdata_root="$2"
profile="$3"

if [[ ! -d "$coverdata_root" ]]; then
    echo "error: no such coverage data directory: $coverdata_root" >&2
    exit 1
fi

inputs=$(find "$coverdata_root" -type f -name 'covcounters.*' -exec dirname {} \; |
    sort -u | paste -sd, -)

if [[ -z "$inputs" ]]; then
    echo "error: no coverage data found under $coverdata_root" >&2
    exit 1
fi

go tool covdata textfmt -i="$inputs" -o="$profile"

filtered=$(mktemp)
trap 'rm -f "$filtered"' EXIT

sed -E -f - "$profile" > "$filtered" <<'EOF'
/\/cmd\/.*\/main\.go/d
/\.pb\.go/d
/\/testutil\//d
EOF
mv "$filtered" "$profile"

total=$(go tool cover -func="$profile" | awk '/^total:/ { print $3 }')

if [[ -z "$total" ]]; then
    echo "error: failed to calculate total coverage" >&2
    exit 1
fi

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
    ' "$profile" |
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
