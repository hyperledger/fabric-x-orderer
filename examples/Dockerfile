# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

ARG GO_VERSION=1.27.0
ARG BASE_IMAGE=alpine
# Version and commit hash written into the binaries; pass the real values with --build-arg.
ARG VERSION=latest
ARG REVISION=unknown

FROM golang:${GO_VERSION} AS builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

# The .git folder is not copied into the image, so make cannot read the commit hash by itself.
# ":-" falls back to the default when a value is passed but empty, e.g. --build-arg VERSION=
ARG VERSION
ARG REVISION
RUN make binary VERSION=${VERSION:-latest} ORDERER_REVISION=${REVISION:-unknown}
FROM ${BASE_IMAGE} AS arma

WORKDIR /
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

ENTRYPOINT ["arma"]
