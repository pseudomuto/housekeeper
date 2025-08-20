#!/usr/bin/env bash

# Migration execution utility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/clickhouse.sh"
source "$SCRIPT_DIR/../lib/validation.sh"

PROJECT_DIR="${1:-}"
CLICKHOUSE_DSN="${2:-}"
MIGRATION_FILES="${3:-}"

usage() {
  cat <<EOF
Usage: $0 PROJECT_DIR CLICKHOUSE_DSN [MIGRATION_FILES...]

Run migrations in a Housekeeper project

Arguments:
  PROJECT_DIR     Project directory with housekeeper.yaml
  CLICKHOUSE_DSN  ClickHouse connection string (host:port)
  MIGRATION_FILES Optional space-separated list of migration files to copy

Examples:
  $0 /tmp/project localhost:9000
  $0 ./project localhost:9000 "001_init.sql 002_users.sql"

Environment variables:
  DRY_RUN=true    Run in dry-run mode
EOF
}

main() {
  if [[ -z "$PROJECT_DIR" || -z "$CLICKHOUSE_DSN" ]]; then
    usage
    exit 1
  fi

  if [[ ! -d "$PROJECT_DIR" ]]; then
    error "Project directory does not exist: $PROJECT_DIR"
  fi

  DRY_RUN="${DRY_RUN:-false}"

  log_info "Running migrations"
  log_info "Project: $PROJECT_DIR"
  log_info "ClickHouse DSN: $CLICKHOUSE_DSN"
  log_info "Dry run: $DRY_RUN"

  # Validate prerequisites
  log_step "Validating prerequisites..."
  validate_connection "$CLICKHOUSE_DSN"
  validate_project_structure "$PROJECT_DIR"

  cd "$PROJECT_DIR"

  # Copy migration files if specified
  if [[ -n "$MIGRATION_FILES" ]]; then
    log_step "Copying migration files..."
    for migration in $MIGRATION_FILES; do
      local source_file="$SCRIPT_DIR/../fixtures/migrations/$migration"
      local dest_file="db/migrations/$migration"

      if [[ -f "$source_file" ]]; then
        cp "$source_file" "$dest_file"
        log_debug "Copied: $migration"
      else
        log_warning "Migration file not found: $source_file"
      fi
    done
  fi

  # List migrations to run
  local migration_count
  migration_count=$(find db/migrations -name "*.sql" ! -name "housekeeper.sum" | wc -l)
  log_info "Found $migration_count migration file(s)"

  if [[ $migration_count -eq 0 ]]; then
    log_warning "No migration files found in db/migrations/"
    return 0
  fi

  # Run migrations
  log_step "Running migrations..."
  if [[ "$DRY_RUN" == "true" ]]; then
    "$PROJECT_ROOT/bin/housekeeper" migrate --url "$CLICKHOUSE_DSN" --dry-run
  else
    "$PROJECT_ROOT/bin/housekeeper" migrate --url "$CLICKHOUSE_DSN"
  fi

  if [[ "$DRY_RUN" != "true" ]]; then
    # Validate migration results
    log_step "Validating migration results..."

    # Check revision records
    local revision_count
    revision_count=$(execute_clickhouse_query "$CLICKHOUSE_DSN" "SELECT count(*) FROM housekeeper.revisions" "TSV")
    log_info "Revision records: $revision_count"

    # Check for failed migrations
    local failed_count
    failed_count=$(execute_clickhouse_query "$CLICKHOUSE_DSN" "SELECT count(*) FROM housekeeper.revisions WHERE error IS NOT NULL" "TSV")

    if [[ "$failed_count" -gt 0 ]]; then
      log_error "Found $failed_count failed migration(s)"

      # Show failed migrations
      execute_clickhouse_query "$CLICKHOUSE_DSN" "SELECT version, error FROM housekeeper.revisions WHERE error IS NOT NULL ORDER BY executed_at" "TSV" |
        while IFS=$'\t' read -r version error; do
          log_error "  $version: $error"
        done

      exit 1
    fi

    log_success "All migrations completed successfully"
  fi

  # Generate report
  cat <<EOF

=== Migration Results ===
Project: $PROJECT_DIR
ClickHouse: $CLICKHOUSE_DSN
Migrations: $migration_count processed
Status: $(if [[ "$DRY_RUN" == "true" ]]; then echo "DRY RUN"; else echo "APPLIED"; fi)

EOF

  if [[ "$DRY_RUN" != "true" ]]; then
    echo "Revision records: $revision_count"
    echo "Failed migrations: $failed_count"
    echo
  fi
}

main "$@"

