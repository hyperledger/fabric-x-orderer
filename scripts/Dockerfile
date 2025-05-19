# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

ARG GO_VER=1.24.3
ARG PROTOC_VER=1.5.4
ARG PROTOTOOL_VER=1.10.0

FROM golang:${GO_VER}-alpine AS golang
RUN mkdir -p /tools
WORKDIR /tools
ARG PROTOC_VER
RUN apk update
RUN apk add git
RUN apk add curl
RUN git clone -q -c advice.detachedHead=false -b v${PROTOC_VER} --depth 1 https://github.com/golang/protobuf
WORKDIR protobuf
RUN GOBIN=/usr/local/bin go install ./protoc-gen-go

FROM golang AS prototool
ARG PROTOTOOL_VER=1.8.0
RUN curl -sL -o /usr/local/bin/prototool https://github.com/uber/prototool/releases/download/v${PROTOTOOL_VER}/prototool-Linux-x86_64 && chmod +x /usr/local/bin/prototool

FROM golang
RUN apk update && apk add --no-cache make protobuf-dev
COPY --from=golang /usr/local/bin/protoc-gen-go /usr/local/bin/
COPY --from=prototool /usr/local/bin/prototool /usr/local/bin/

