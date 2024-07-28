ARG GO_VERSION=1.22
ARG BASE_IMAGE=alpine

FROM golang:${GO_VERSION} AS builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN go build -o /arma node/cmd/arma/main/main.go
RUN go build -o /armageddon node/cmd/armageddon/main/main.go

FROM ${BASE_IMAGE} AS arma

WORKDIR /
COPY --from=builder /arma /usr/local/bin/arma
COPY --from=builder /armageddon /usr/local/bin/armageddon
ENTRYPOINT ["arma"]
