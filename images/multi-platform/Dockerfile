FROM docker.io/library/golang:1.23.4 AS builder

# If you need to access github.ibm.com for the build
# RUN apt update
# RUN apt install -y git
# ARG GITHUB_TOKEN
# ENV GOPRIVATE=github.ibm.com
# ENV TOKEN=$GITHUB_TOKEN
# RUN git config --global url."https://${TOKEN}:x-oauth-basic@github.ibm.com/".insteadOf "https://github.ibm.com/"

WORKDIR /src
COPY . .
ENV CGO_ENABLED=0
ENV GOOS=linux

RUN make binary

# At this point building of the application should be done
FROM registry.access.redhat.com/ubi9/ubi-micro:9.5 AS prod

WORKDIR /
COPY --from=builder /src/bin/arma /usr/local/bin/arma
COPY --from=builder /src/bin/armageddon /usr/local/bin/armageddon

ENTRYPOINT ["arma"]