#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.25.5 AS builder
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

RUN ls -la .

# Build using your Makefile target
RUN make binary

###########################################
# Stage 2: Production runtime image
###########################################
FROM registry.access.redhat.com/ubi9/ubi-minimal:9.6 AS prod

ARG VERSION=1.0
ARG CREATED
ARG REVISION=1.0

# Add non-root user (10001) without installing extra packages
RUN /usr/sbin/useradd -u 10001 -r -g root -s /sbin/nologin -c "Hyperledger Fabric-X Orderer user" orderer && \
    mkdir -p /home/orderer && \
    chown -R 10001:0 /home/orderer && \
    chmod 0755 /home/orderer

# Copy in the statically built binaries from builder
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

# OCI metadata labels
LABEL org.opencontainers.image.created="${CREATED}" \
    org.opencontainers.image.description="Hyperledger Fabric-X Orderer service packaged in a UBI image." \
    org.opencontainers.image.licenses="Apache-2.0" \
    org.opencontainers.image.ref.name="ubi9/ubi-minimal" \
    org.opencontainers.image.revision="${REVISION}" \
    org.opencontainers.image.source="https://github.com/hyperledger/fabric-x-orderer" \
    org.opencontainers.image.title="fabric-x-orderer" \
    org.opencontainers.image.url="https://github.com/hyperledger/fabric-x-orderer" \
    org.opencontainers.image.version="${VERSION}"

# Use non-root user and set workdir using BIN argument
USER 10001
WORKDIR /home/orderer

# Default entrypoint
ENTRYPOINT ["/usr/local/bin/arma"]