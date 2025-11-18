# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.24.3 AS builder

# Build arguments used for cross-compiling
ARG TARGETOS
ARG TARGETARCH

ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}
ENV CGO_ENABLED=0 \
    GO111MODULE=on

WORKDIR /src

# Copy only dependency files first (better layer caching)
COPY go.mod go.sum ./
RUN go mod download

# Now copy rest of the source
COPY . .

# Build using your Makefile target
RUN make binary


# -----------------------------------------------------------------------------
# Stage 2: Production Image (UBI-micro)
# -----------------------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/ubi-micro:9.5 AS prod

# Add a non-root user for security (UBI-micro has no useradd)
# → use an explicit UID that matches OpenShift expectations
USER 10001

WORKDIR /app

# Copy in the statically built binaries from builder
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

# OCI labels (strongly recommended for prod images)
LABEL name="arma-service" \
      vendor="IBM" \
      maintainer="ZRL Decentralized Trust Group" \
      summary="ARMA Service Production Image" \
      description="Lightweight and secure Go application running on UBI-micro" \
      license="Apache-2.0"

ENTRYPOINT ["/usr/local/bin/arma"]
