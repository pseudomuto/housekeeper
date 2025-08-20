#!/usr/bin/env bash

# Migration snapshot testing utility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/clickhouse.sh"
source "$SCRIPT_DIR/../lib/validation.sh"

PROJECT_DIR="${1:-}"
CLICKHOUSE_DSN="${2:-}"
DESCRIPTION="${3:-E2E test snapshot}"

usage() {
  cat <<EOF
Usage: $0 PROJECT_DIR CLICKHOUSE_DSN [DESCRIPTION]

Test migration snapshot functionality

Arguments:
  PROJECT_DIR     Project directory with migrations
  CLICKHOUSE_DSN  ClickHouse connection string (host:port)  
  DESCRIPTION     Snapshot description (optional)

Examples:
  $0 /tmp/project localhost:9000
  $0 ./project localhost:9000 "Pre-production snapshot"
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

  log_info "Testing migration snapshot"
  log_info "Project: $PROJECT_DIR"
  log_info "ClickHouse DSN: $CLICKHOUSE_DSN"
  log_info "Description: $DESCRIPTION"

  cd "$PROJECT_DIR"

  # Validate prerequisites
  log_step "Validating prerequisites..."
  validate_connection "$CLICKHOUSE_DSN"
  validate_project_structure "$PROJECT_DIR"

  # Count migrations before snapshot
  local migrations_before
  migrations_before=$(find db/migrations -name "*.sql" ! -name "housekeeper.sum" | wc -l)
  log_info "Migrations before snapshot: $migrations_before"

  if [[ $migrations_before -eq 0 ]]; then
    error "No migrations found to snapshot"
  fi

  # List migrations that will be consolidated
  log_step "Migrations to be consolidated:"
  find db/migrations -name "*.sql" ! -name "housekeeper.sum" | sort | while read -r migration; do
    log_info "  $(basename "$migration")"
  done

  # Create snapshot
  log_step "Creating snapshot..."
  "$PROJECT_ROOT/bin/housekeeper" snapshot --description "$DESCRIPTION"

  # Validate snapshot creation
  log_step "Validating snapshot creation..."

  # Check snapshot file exists
  local snapshot_file
  snapshot_file=$(find db/migrations -name "*_snapshot.sql" | head -n1)

  if [[ -z "$snapshot_file" ]]; then
    error "No snapshot file created"
  fi

  check_file_exists "$snapshot_file" "Snapshot file"
  log_success "Snapshot file created: $(basename "$snapshot_file")"

  # Verify original migrations were removed
  local migrations_after
  migrations_after=$(find db/migrations -name "*.sql" ! -name "*_snapshot.sql" ! -name "housekeeper.sum" | wc -l)

  if [[ $migrations_after -gt 0 ]]; then
    error "Expected all migrations to be consolidated, but found $migrations_after remaining"
  fi

  log_success "Original migrations consolidated: $migrations_before → 1 snapshot"

  # Verify sum file updated
  check_file_exists "db/migrations/housekeeper.sum" "Sum file"

  # Validate snapshot content
  log_step "Validating snapshot content..."
  if grep -q "housekeeper:snapshot" "$snapshot_file"; then
    log_debug "Snapshot marker found"
  else
    error "Snapshot file missing required marker"
  fi

  if grep -q "$DESCRIPTION" "$snapshot_file"; then
    log_debug "Snapshot description found"
  else
    log_warning "Snapshot description not found in file"
  fi

  # Check revision records remain intact
  log_step "Validating revision records..."
  local revision_count
  revision_count=$(execute_clickhouse_query "$CLICKHOUSE_DSN" "SELECT count(*) FROM housekeeper.revisions WHERE kind = 'migration'" "TSV")

  if [[ "$revision_count" -lt "$migrations_before" ]]; then
    error "Revision records missing after snapshot (expected >= $migrations_before, got $revision_count)"
  fi

  log_success "Revision records preserved: $revision_count"

  # Test adding post-snapshot migration
  log_step "Testing post-snapshot migration capability..."

  # Copy a simple post-snapshot migration
  local post_migration="999_post_snapshot_test.sql"
  cat >"db/migrations/$post_migration" <<'EOF'
-- Test post-snapshot migration
CREATE VIEW IF NOT EXISTS analytics.snapshot_test_view AS 
SELECT 'snapshot_test' as test_marker;
EOF

  # Run the post-snapshot migration
  log_step "Running post-snapshot migration..."
  "$PROJECT_ROOT/bin/housekeeper" migrate --url "$CLICKHOUSE_DSN"

  # Verify post-snapshot migration worked
  local test_view_count
  test_view_count=$(execute_clickhouse_query "$CLICKHOUSE_DSN" "SELECT count(*) FROM system.tables WHERE database = 'analytics' AND name = 'snapshot_test_view'" "TSV")

  if [[ "$test_view_count" -ne 1 ]]; then
    error "Post-snapshot migration failed - test view not created"
  fi

  log_success "Post-snapshot migration successful"

  # Cleanup test view
  execute_clickhouse_query "$CLICKHOUSE_DSN" "DROP VIEW IF EXISTS analytics.snapshot_test_view" >/dev/null
  rm -f "db/migrations/$post_migration"

  # Final state validation
  log_step "Final state validation..."
  local final_files
  final_files=$(find db/migrations -name "*.sql" | wc -l)

  log_success "Snapshot testing completed successfully"

  # Generate summary
  cat <<EOF

=== Snapshot Test Results ===
Project: $PROJECT_DIR
Snapshot file: $(basename "$snapshot_file")
Description: $DESCRIPTION

Migration consolidation:
  Before: $migrations_before files
  After: 1 snapshot file
  
Revision records: $revision_count preserved
Post-snapshot capability: ✓ Verified

Final migration directory:
  Files: $final_files (snapshot + sum file)
  
All tests passed! ✅

EOF
}

main "$@"

