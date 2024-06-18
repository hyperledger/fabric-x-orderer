ARG GO_VERSION=1.22
ARG BASE_IMAGE=alpine

FROM golang:${GO_VERSION} as builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux
RUN pwd
RUN ls
RUN go build -o /arma cmd/arma/main/main.go

FROM ${BASE_IMAGE} as arma

WORKDIR /
COPY --from=builder /arma /usr/local/bin/arma
ENTRYPOINT ["arma"]
