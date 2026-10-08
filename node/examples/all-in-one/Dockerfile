# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

ARG GO_VERSION=1.27.0
# Version and commit hash written into the binaries and the image labels; the release workflow passes the real values.
ARG VERSION=latest
ARG REVISION=unknown

###########################################
# Stage 1: cross-compiling builder
###########################################
FROM --platform=$BUILDPLATFORM docker.io/library/golang:${GO_VERSION} AS builder

# Build arguments used for cross-compiling
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV CGO_ENABLED=0

WORKDIR /src
COPY . .

# The .git folder is not copied into the image, so make cannot read the commit hash by itself.
# ":-" falls back to the default when a value is passed but empty, e.g. --build-arg VERSION=
ARG VERSION
ARG REVISION
RUN make binary VERSION=${VERSION:-latest} ORDERER_REVISION=${REVISION:-unknown}

###########################################
# Stage 2: all-in-one test runtime
###########################################
FROM docker.io/library/golang:${GO_VERSION}

ARG VERSION
ARG CREATED
ARG REVISION

# armageddon's sampleConfigPath auto-detection resolves testutil/fabric/sampleconfig
# relative to `go env GOMOD`, so the source tree (with go.mod) must be present at
# runtime, with WORKDIR set to it.
WORKDIR /src
COPY --from=builder /src /src

# binaries already cross-compiled → place them directly (no RUN, so no foreign-arch
# exec under QEMU: only RUN invokes a process in the emulated target platform)
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

# scripts
COPY --chmod=755 node/examples/all-in-one/entrypoint.sh /entrypoint.sh
COPY --chmod=755 node/examples/all-in-one/run_all.sh /run_all.sh

# network topology, so the image runs standalone without a /config mount
COPY node/examples/all-in-one/config /config

# OCI metadata labels
LABEL org.opencontainers.image.created="${CREATED}" \
    org.opencontainers.image.description="Hyperledger Fabric-X Orderer all-in-one test node: a 4-party, 1-shard Arma network plus the armageddon test client in a single container." \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.ref.name="library/golang" \
    org.opencontainers.image.revision="${REVISION:-unknown}" \
    org.opencontainers.image.source="https://github.com/hyperledger/fabric-x-orderer" \
    org.opencontainers.image.title="fabric-x-orderer-test-node" \
    org.opencontainers.image.url="https://github.com/hyperledger/fabric-x-orderer" \
    org.opencontainers.image.version="${VERSION:-latest}"

ENTRYPOINT ["/entrypoint.sh"]
