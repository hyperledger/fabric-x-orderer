ARG GO_VERSION=1.22
ARG BASE_IMAGE=alpine

FROM golang:${GO_VERSION} as builder

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN go build -o /arma cmd/arma/main/main.go
RUN go build -o /armageddon cmd/armageddon/main/main.go

FROM ${BASE_IMAGE} as arma

WORKDIR /
COPY --from=builder /arma /usr/local/bin/arma
COPY --from=builder /armageddon /usr/local/bin/armageddon
RUN cat <<EOF > config.yaml
Parties:
  - ID: 1
    AssemblerEndpoint: "assembler.p1:6023"
    ConsenterEndpoint: "consensus.p1:6025"
    RouterEndpoint: "router.p1:6022"
    BatchersEndpoints:
      - "batcher1.p1:6024"
      - "batcher2.p1:6024"
  - ID: 2
    AssemblerEndpoint: "assembler.p2:6023"
    ConsenterEndpoint: "consensus.p2:6025"
    RouterEndpoint: "router.p2:6022"
    BatchersEndpoints:
      - "batcher1.p2:6024"
      - "batcher2.p2:6024"
  - ID: 3
    AssemblerEndpoint: "assembler.p3:6023"
    ConsenterEndpoint: "consensus.p3:6025"
    RouterEndpoint: "router.p3:6022"
    BatchersEndpoints:
      - "batcher1.p3:6024"
      - "batcher2.p3:6024"
  - ID: 4
    AssemblerEndpoint: "assembler.p4:6023"
    ConsenterEndpoint: "consensus.p4:6025"
    RouterEndpoint: "router.p4:6022"
    BatchersEndpoints:
      - "batcher1.p4:6024"
      - "batcher2.p4:6024"
EOF

RUN /usr/local/bin/armageddon generate --config config.yaml

RUN rm config.yaml

ENTRYPOINT ["arma"]
