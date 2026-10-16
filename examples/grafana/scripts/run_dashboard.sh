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
COMPOSE=(${DOCKER_CMD} compose -f "${EXAMPLE_DIR}/compose.yaml")

cd "${EXAMPLE_DIR}/../.."
make binary

rm -rf "${SAMPLE_DIR}"
./bin/armageddon generate --config="${DEPLOYMENT}" --output="${SAMPLE_DIR}"

# Expose a fixed operations port so Prometheus can scrape the nodes.
for config in "${SAMPLE_DIR}"/config/party*/local_config_*.yaml; do
  awk -v port="${OPERATIONS_PORT}" '
    /ListenAddress: 127.0.0.1/ {
      print "    ListenAddress: 0.0.0.0"
      print "    ListenPort: " port
      next
    }
    { print }
  ' "${config}" > "${config}.new"
  mv "${config}.new" "${config}"
done

hosts=$(grep -oE '"[a-z0-9.]+:[0-9]+"' "${DEPLOYMENT}" | tr -d '"' | cut -d: -f1)

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

chmod -R a+rX "${EXAMPLE_DIR}/grafana" "${SAMPLE_DIR}/prometheus.yml"

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

TRANSACTIONS=$((RATE * DURATION_SECONDS))
export TRANSACTIONS RATE TX_SIZE
"${COMPOSE[@]}" up -d submitter

if [ "${OPEN_BROWSER}" != false ]; then
  for opener in xdg-open open; do
    if command -v "${opener}" > /dev/null; then
      "${opener}" "${DASHBOARD_URL}" > /dev/null 2>&1 &
      break
    fi
  done
fi

cat <<INFO

Submitting ${TRANSACTIONS} transactions of ${TX_SIZE}B at ${RATE} tx/s.

  Dashboard  : ${DASHBOARD_URL}
  Prometheus : ${PROMETHEUS_URL}
  Clean up   : bash ${EXAMPLE_DIR}/scripts/clean_dashboard.sh
INFO
