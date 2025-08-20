#!/usr/bin/env bash

# ClickHouse container setup utility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/clickhouse.sh"

CLICKHOUSE_VERSION="${1:-25.7}"
CONTAINER_NAME="${2:-housekeeper-e2e-clickhouse}"

main() {
  log_info "Setting up ClickHouse container"
  log_info "Version: $CLICKHOUSE_VERSION"

  # Check if Docker is available
  if ! command_exists docker; then
    error "Docker is required but not available"
  fi

  # Start container
  log_step "Starting ClickHouse container..."
  CONTAINER_ID=$(start_clickhouse_container "$CLICKHOUSE_VERSION")
  DSN=$(get_clickhouse_dsn "$CONTAINER_ID")

  log_success "ClickHouse container started"
  log_info "Container ID: $CONTAINER_ID"
  log_info "DSN: $DSN"

  # Wait for readiness
  log_step "Waiting for ClickHouse to be ready..."
  wait_for_clickhouse "$DSN"

  # Verify connectivity
  log_step "Verifying connectivity..."
  VERSION=$(check_clickhouse_version "$DSN")

  log_success "ClickHouse is ready"
  log_info "ClickHouse version: $VERSION"

  # Output connection details for use by other scripts
  cat <<EOF

=== ClickHouse Container Ready ===
Container ID: $CONTAINER_ID
DSN: $DSN
Version: $VERSION

Export these variables to use in other scripts:
export CLICKHOUSE_CONTAINER_ID="$CONTAINER_ID"
export CLICKHOUSE_DSN="$DSN"

Stop the container with:
docker stop $CONTAINER_ID

EOF
}

main "$@"

