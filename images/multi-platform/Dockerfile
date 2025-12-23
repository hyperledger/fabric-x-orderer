#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

###########################################
# Stage 1: Build image
###########################################
FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.24.3 AS builder

# Build arguments used for cross-compiling
ARG TARGETOS
ARG TARGETARCH

# Environment vars
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV GO111MODULE=on
ENV CGO_ENABLED=0

WORKDIR /src

# Copy build files (managed through .dockerignore)
COPY . .

# Build using your Makefile target
RUN make binary

###########################################
# Stage 2: Production runtime image
###########################################
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.6 AS prod

ARG VERSION=1.0

# Add non-root user (10001) without installing extra packages
RUN /usr/sbin/useradd -u 10001 -r -g root -s /sbin/nologin -c "Hyperledger Fabric-X Orderer user" orderer && \
    mkdir -p /home/orderer && \
    chown -R 10001:0 /home/orderer && \
    chmod 0755 /home/orderer

# Copy in the statically built binaries from builder
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

# OCI metadata labels
LABEL name="fabric-x-orderer" \
    maintainer="IBM Research Decentralized Trust Group" \
    version="${VERSION}" \
    description="Hyperledger Fabric-X Orderer service packaged in a UBI image" \
    license="Apache-2.0" \
    vendor="IBM"

# Use non-root user and set workdir using BIN argument
USER 10001
WORKDIR /home/orderer

# Default entrypoint
ENTRYPOINT ["/usr/local/bin/arma"]