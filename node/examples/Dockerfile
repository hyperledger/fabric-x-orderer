ARG GO_VERSION=1.22
ARG BASE_IMAGE=alpine

FROM golang:${GO_VERSION} AS builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN make binary
FROM ${BASE_IMAGE} AS arma

WORKDIR /
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

ENTRYPOINT ["arma"]
