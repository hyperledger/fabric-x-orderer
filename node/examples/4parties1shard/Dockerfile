ARG GO_VERSION=1.25.5

FROM golang:${GO_VERSION}

WORKDIR /src
COPY . .

ENV CGO_ENABLED=0
ENV GOOS=linux

RUN make binary

# 🔥 binaries already built → just move them
RUN cp /src/bin/arma /usr/local/bin/arma && \
    cp /src/bin/armageddon /usr/local/bin/armageddon

# scripts
COPY node/examples/4parties1shard/entrypoint.sh /entrypoint.sh
COPY node/examples/4parties1shard/run_all.sh /run_all.sh

RUN chmod +x /entrypoint.sh /run_all.sh

ENTRYPOINT ["/entrypoint.sh"]