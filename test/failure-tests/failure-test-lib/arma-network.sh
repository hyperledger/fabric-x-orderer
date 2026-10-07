# Copyright the Hyperledger Fabric contributors. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0
#
# The ARMA network of the failure tests: config generation, launching nodes and health checks.

print_test_configuration() {
  echo "=========================================="
  echo "${TEST_NAME} Configuration"
  echo "=========================================="
  echo "Duration: ${DURATION} minutes"
  echo "TX Rate: ${TX_RATE} tx/s"
  echo "TX Size: ${TX_SIZE} bytes"
  echo "Total TXs: ${TOTAL_TXS}"
  echo "Parties: ${NUM_PARTIES}"
  echo "Shards: ${NUM_SHARDS}"
  echo "Failure Runner Enabled: ${FAILURE_RUNNER_ENABLED}"
  echo "Test directory: ${TEST_DIR}"
  echo "=========================================="
}

remove_leftovers_of_previous_run() {
  echo "Removing processes and logs left by a previous run..."
  pkill -f "/bin/arma " || true
  pkill -f "bin/armageddon submit" || true
  sleep 1
  rm -f submit.log failure_runner.log consenter*.log batcher*.log assembler*.log router*.log
}

generate_network_config() {
  local config_yaml="${TEST_DIR}/config.yaml"
  echo "Generating network config in ${TEST_DIR}..."
  echo "Parties:" > "$config_yaml"
  for i in $(seq 1 "$NUM_PARTIES"); do
    cat >> "$config_yaml" <<EOF
  - ID: $i
    AssemblerEndpoint: "127.0.0.1:$((8000 + i * 10 + 1))"
    ConsenterEndpoint: "127.0.0.1:$((8000 + i * 10 + 2))"
    RouterEndpoint: "127.0.0.1:$((8000 + i * 10 + 3))"
    BatchersEndpoints:
EOF
    for j in $(seq 1 "$NUM_SHARDS"); do
      echo "      - \"127.0.0.1:$((8000 + i * 10 + 3 + j))\"" >> "$config_yaml"
    done
  done
  cat >> "$config_yaml" <<EOF
UseTLSRouter: "none"
UseTLSAssembler: "none"
EOF

  ./bin/armageddon generate --config="$config_yaml" --output="$TEST_DIR" --sampleConfigPath=testutil/fabric/sampleconfig

  # Each node needs its own data directory, because LevelDB locks it
  local count=0 config_file component party data_dir
  for config_file in "${TEST_DIR}"/config/party*/local_config_*.yaml; do
    component=$(basename "$config_file" .yaml | sed 's/local_config_//')
    party=$(echo "$config_file" | grep -oP 'party\K[0-9]+')
    data_dir="${TEST_DIR}/data/${component}_party${party}"
    mkdir -p "$data_dir"
    sed -i "s|Location: /var/dec-trust.*|Location: ${data_dir}|; s|WALDir: /var/dec-trust.*|WALDir: ${data_dir}/wal|" "$config_file"
    count=$((count + 1))
  done
  echo "Data paths set for ${count} node configs under ${TEST_DIR}/data"
}

set_node_files() {
  local component=$1 party=$2 shard=$3
  # the consenter has mixed names in the generated files, which is why it gets its own option
  case "$component" in
    consensus)
      NODE_CONFIG="${TEST_DIR}/config/party${party}/local_config_consenter.yaml"
      NODE_LOG="consenter${party}.log"
      NODE_PID_FILE="${TEST_DIR}/pids/consensus${party}.pid"
      ;;
    batcher)
      NODE_CONFIG="${TEST_DIR}/config/party${party}/local_config_batcher${shard}.yaml"
      NODE_LOG="batcher${party}-${shard}.log"
      NODE_PID_FILE="${TEST_DIR}/pids/batcher${party}-${shard}.pid"
      ;;
    *)
      NODE_CONFIG="${TEST_DIR}/config/party${party}/local_config_${component}.yaml"
      NODE_LOG="${component}${party}.log"
      NODE_PID_FILE="${TEST_DIR}/pids/${component}${party}.pid"
      ;;
  esac
}

launch_node() {
  set_node_files "$1" "$2" "$3"
  ./bin/arma "$1" --config="$NODE_CONFIG" >> "$NODE_LOG" 2>&1 &
  NODE_PID=$!
  echo "$NODE_PID" > "$NODE_PID_FILE"
}

wait_for_healthz() {
  local log_file=$1 label=$2 url="" i
  for i in $(seq 1 60); do
    url=$(grep -oP 'Health check serving on URL:\s+\K(https?://\S+)' "$log_file" 2>/dev/null) || true
    if [ -n "$url" ]; then
      break
    fi
    sleep 1
  done
  if [ -z "$url" ]; then
    startup_failed "${label} failed to start: health check URL never appeared in log within 60s"
  fi

  for i in $(seq 1 60); do
    if curl -sf "$url" > /dev/null 2>&1; then
      echo "  ${label} healthy at ${url}"
      return 0
    fi
    sleep 1
  done
  startup_failed "${label} failed to start: /healthz at ${url} did not return HTTP 200 within 60s"
}

startup_failed() {
  echo "$1"
  mkdir -p "$RESULTS_DIR"
  echo "$1" > "${RESULTS_DIR}/startup_failure.txt"
  exit 1
}

start_arma_network() {
  echo "Starting ARMA network (${NUM_PARTIES} parties, ${NUM_SHARDS} shards)..."
  mkdir -p "${TEST_DIR}/pids"
  for i in $(seq 1 "$NUM_PARTIES"); do
    launch_node consensus "$i"
    wait_for_healthz "$NODE_LOG" "Consenter ${i} (PID ${NODE_PID})"
  done
  for i in $(seq 1 "$NUM_PARTIES"); do
    for j in $(seq 1 "$NUM_SHARDS"); do
      launch_node batcher "$i" "$j"
      wait_for_healthz "$NODE_LOG" "Batcher ${i}-${j} (PID ${NODE_PID})"
    done
  done
  for i in $(seq 1 "$NUM_PARTIES"); do
    launch_node assembler "$i"
    wait_for_healthz "$NODE_LOG" "Assembler ${i} (PID ${NODE_PID})"
  done
  for i in $(seq 1 "$NUM_PARTIES"); do
    launch_node router "$i"
    wait_for_healthz "$NODE_LOG" "Router ${i} (PID ${NODE_PID})"
  done
  echo "ARMA network started, all nodes healthy"
}
