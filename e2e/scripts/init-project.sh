#!/usr/bin/env bash

# Project initialization utility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"

PROJECT_DIR="${1:-}"
TEMPLATE_DIR="${2:-$SCRIPT_DIR/../fixtures/project-template}"

usage() {
  cat <<EOF
Usage: $0 PROJECT_DIR [TEMPLATE_DIR]

Initialize a Housekeeper project for testing

Arguments:
  PROJECT_DIR   Directory where project will be created
  TEMPLATE_DIR  Template directory to copy from (optional)

Examples:
  $0 /tmp/test-project
  $0 ./my-test custom-template/
EOF
}

main() {
  if [[ -z "$PROJECT_DIR" ]]; then
    usage
    exit 1
  fi

  if [[ ! -d "$TEMPLATE_DIR" ]]; then
    error "Template directory does not exist: $TEMPLATE_DIR"
  fi

  log_info "Initializing Housekeeper project"
  log_info "Project directory: $PROJECT_DIR"
  log_info "Template directory: $TEMPLATE_DIR"

  # Create project directory
  log_step "Creating project directory..."
  mkdir -p "$PROJECT_DIR"

  # Copy template files
  log_step "Copying template files..."
  cp -r "$TEMPLATE_DIR/"* "$PROJECT_DIR/"

  # Initialize with Housekeeper
  log_step "Running housekeeper init..."
  cd "$PROJECT_DIR"
  "$PROJECT_ROOT/bin/housekeeper" init

  # Validate project structure
  log_step "Validating project structure..."
  check_file_exists "$PROJECT_DIR/housekeeper.yaml" "Config file"
  check_file_exists "$PROJECT_DIR/db/main.sql" "Main schema file"
  check_dir_exists "$PROJECT_DIR/db/migrations" "Migrations directory"

  log_success "Project initialized successfully"

  cat <<EOF

=== Project Ready ===
Location: $PROJECT_DIR

Files created:
- housekeeper.yaml (configuration)
- db/main.sql (main schema)
- db/migrations/ (migrations directory)

Next steps:
1. Add migration files to db/migrations/
2. Run: housekeeper migrate --url <clickhouse-dsn>

EOF
}

main "$@"

