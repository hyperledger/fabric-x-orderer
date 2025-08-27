# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

FROM --platform=$BUILDPLATFORM docker.io/library/golang:1.24.3 AS builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0

ARG TARGETOS
ARG TARGETARCH
ENV GOOS=${TARGETOS}
ENV GOARCH=${TARGETARCH}

RUN make binary

# At this point building of the application should be done
FROM registry.access.redhat.com/ubi9/ubi-micro:9.5 AS prod

WORKDIR /
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

ENTRYPOINT ["arma"]