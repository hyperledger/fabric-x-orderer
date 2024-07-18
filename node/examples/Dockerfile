ARG GO_VERSION=1.22
ARG BASE_IMAGE=alpine

FROM golang:${GO_VERSION} as builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

COPY node/examples/config /tmp

RUN go build -o /arma node/cmd/arma/main/main.go
RUN go build -o /armageddon node/cmd/armageddon/main/main.go

FROM ${BASE_IMAGE} as arma

WORKDIR /
COPY --from=builder /arma /usr/local/bin/arma
COPY --from=builder /armageddon /usr/local/bin/armageddon
COPY --from=builder /tmp/example-deployment.yaml /tmp/example-deployment.yaml

RUN /usr/local/bin/armageddon generate --config /tmp/example-deployment.yaml

ENTRYPOINT ["arma"]
