#!/usr/bin/env bash
#
# Copyright IBM Corp. All Rights Reserved.
#
# SPDX-License-Identifier: Apache-2.0
#

set -eu

RATE=${RATE:-1000}
TX_SIZE=${TX_SIZE:-300}
DURATION_SECONDS=${DURATION_SECONDS:-300}
OPEN_BROWSER=${OPEN_BROWSER:-auto}
DOCKER_CMD=${DOCKER_CMD:-docker}

SAMPLE_DIR=/tmp/arma-sample
OPERATIONS_PORT=8080

EXAMPLE_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
DEPLOYMENT=${EXAMPLE_DIR}/../config/example-deployment.yaml
COMPOSE=($DOCKER_CMD compose -f "${EXAMPLE_DIR}/../compose.yaml" -f "${EXAMPLE_DIR}/compose.yaml")

command -v curl > /dev/null || { echo "curl is required" >&2; exit 1; }

cd "${EXAMPLE_DIR}/../../.."
make binary

rm -rf "${SAMPLE_DIR}"
./bin/armageddon generate --config="${DEPLOYMENT}" --output="${SAMPLE_DIR}"

# The generator leaves the operations port unset, which means a random one, and binds it
# to localhost. Prometheus scrapes it from another container and needs to know it in
# advance. Every node has its own container, so one port serves them all.
for config in "${SAMPLE_DIR}"/config/party*/local_config_*.yaml; do
    awk -v port="${OPERATIONS_PORT}" '
        /^Operations:/ { operations = 1 }
        operations && /ListenAddress:/ {
            print "    ListenAddress: 0.0.0.0"
            print "    ListenPort: " port
            next
        }
        { print }
    ' "${config}" > "${config}.new"
    mv "${config}.new" "${config}"
done

# Scrape every node of the network. The hosts come from the deployment, so to change the
# number of parties or shards edit ../config/example-deployment.yaml and ../compose.yaml.
hosts=$(grep -oE '"[a-z0-9.]+:[0-9]+"' "${DEPLOYMENT}" | tr -d '"' | cut -d: -f1 | sort -u)

{
    echo "global:"
    echo "  scrape_interval: 5s"
    echo
    echo "scrape_configs:"
    echo "  - job_name: arma"
    echo "    static_configs:"
    echo "      - targets:"
    for host in ${hosts}; do
        echo "          - ${host}:${OPERATIONS_PORT}"
    done
} > "${SAMPLE_DIR}/prometheus.yml"

mkdir -p "${SAMPLE_DIR}/grafana/provisioning/datasources" \
    "${SAMPLE_DIR}/grafana/provisioning/dashboards" \
    "${SAMPLE_DIR}/grafana/dashboards"
cp "${EXAMPLE_DIR}/grafana/datasource.yaml" "${SAMPLE_DIR}/grafana/provisioning/datasources/"
cp "${EXAMPLE_DIR}/grafana/dashboards.yaml" "${SAMPLE_DIR}/grafana/provisioning/dashboards/"
cp "${EXAMPLE_DIR}/grafana/arma-dashboard.json" "${SAMPLE_DIR}/grafana/dashboards/"

# Prometheus and Grafana run as their images' own users, so let them read what they mount.
chmod a+x "${SAMPLE_DIR}"
chmod -R a+rX "${SAMPLE_DIR}/grafana" "${SAMPLE_DIR}/prometheus.yml"

"${COMPOSE[@]}" up -d

PROMETHEUS_URL=http://localhost:$("${COMPOSE[@]}" port prometheus 9090 | cut -d: -f2)
GRAFANA_URL=http://localhost:$("${COMPOSE[@]}" port grafana 3000 | cut -d: -f2)
DASHBOARD_URL=${GRAFANA_URL}/d/arma-dashboard

nodes=$(echo "${hosts}" | wc -w)
echo "Waiting for ${nodes} nodes to be scraped"
for _ in $(seq 1 180); do
    up=$(curl -sf "${PROMETHEUS_URL}/api/v1/targets?state=active" | grep -o '"health":"up"' | wc -l)
    [ "${up}" -ge "${nodes}" ] && break
    sleep 1
done
[ "${up}" -ge "${nodes}" ] || echo "WARNING: only ${up} of ${nodes} nodes are being scraped" >&2

for _ in $(seq 1 60); do
    curl -sf "${GRAFANA_URL}/api/dashboards/uid/arma-dashboard" > /dev/null && break
    sleep 1
done
curl -sf "${GRAFANA_URL}/api/dashboards/uid/arma-dashboard" > /dev/null ||
    echo "WARNING: Grafana has not provisioned the dashboard" >&2

TRANSACTIONS=$((RATE * DURATION_SECONDS))
export TRANSACTIONS RATE TX_SIZE
"${COMPOSE[@]}" --profile client up -d submitter

if [ "${OPEN_BROWSER}" != false ]; then
    for opener in xdg-open open; do
        command -v ${opener} > /dev/null && { ${opener} "${DASHBOARD_URL}" > /dev/null 2>&1 & break; }
    done
fi

cat <<INFO

Submitting ${TRANSACTIONS} transactions of ${TX_SIZE}B at ${RATE} tx/s.

  Dashboard  : ${DASHBOARD_URL}
  Prometheus : ${PROMETHEUS_URL}
  Follow it  : $DOCKER_CMD logs -f arma-grafana-submitter-1
  Clean up   : bash ${EXAMPLE_DIR}/scripts/clean_sample.sh
INFO
