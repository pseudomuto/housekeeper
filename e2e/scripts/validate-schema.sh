#!/usr/bin/env bash

# Schema validation utility

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/../lib/common.sh"
source "$SCRIPT_DIR/../lib/clickhouse.sh"
source "$SCRIPT_DIR/../lib/validation.sh"

CLICKHOUSE_DSN="${1:-}"
VALIDATION_QUERY="${2:-}"
REPORT_FILE="${3:-}"

usage() {
  cat <<EOF
Usage: $0 CLICKHOUSE_DSN [VALIDATION_QUERY] [REPORT_FILE]

Validate ClickHouse schema state

Arguments:
  CLICKHOUSE_DSN     ClickHouse connection string (host:port)
  VALIDATION_QUERY   Specific validation SQL file to run (optional)
  REPORT_FILE        Output file for validation report (optional, default: stdout)

Examples:
  $0 localhost:9000
  $0 localhost:9000 check-final-state.sql
  $0 localhost:9000 check-tables.sql /tmp/validation-report.txt

Available validation queries:
EOF

  if [[ -d "$SCRIPT_DIR/../fixtures/validation" ]]; then
    find "$SCRIPT_DIR/../fixtures/validation" -name "*.sql" -exec basename {} \; | sort | sed 's/^/  /'
  fi
}

main() {
  if [[ -z "$CLICKHOUSE_DSN" ]]; then
    usage
    exit 1
  fi

  log_info "Validating ClickHouse schema"
  log_info "DSN: $CLICKHOUSE_DSN"

  # Validate connection
  log_step "Validating connection..."
  validate_connection "$CLICKHOUSE_DSN"

  if [[ -n "$VALIDATION_QUERY" ]]; then
    # Run specific validation query
    local query_file="$SCRIPT_DIR/../fixtures/validation/$VALIDATION_QUERY"
    if [[ ! -f "$query_file" ]]; then
      error "Validation query file not found: $VALIDATION_QUERY"
    fi

    log_step "Running validation query: $VALIDATION_QUERY"
    run_validation_query "$CLICKHOUSE_DSN" "$query_file"
    log_success "Validation passed: $VALIDATION_QUERY"

  else
    # Run all available validation queries
    log_step "Running all validation queries..."

    local validation_dir="$SCRIPT_DIR/../fixtures/validation"
    if [[ ! -d "$validation_dir" ]]; then
      error "Validation directory not found: $validation_dir"
    fi

    local total_queries=0
    local passed_queries=0

    find "$validation_dir" -name "*.sql" | sort | while read -r query_file; do
      total_queries=$((total_queries + 1))
      local query_name
      query_name=$(basename "$query_file")

      log_step "Running: $query_name"

      if run_validation_query "$CLICKHOUSE_DSN" "$query_file"; then
        passed_queries=$((passed_queries + 1))
        log_success "  ✓ $query_name"
      else
        log_error "  ✗ $query_name"
      fi
    done

    log_info "Validation summary: $passed_queries/$total_queries queries passed"
  fi

  # Generate validation report if requested
  if [[ -n "$REPORT_FILE" ]]; then
    log_step "Generating validation report..."
    generate_validation_report "$CLICKHOUSE_DSN" "$REPORT_FILE"
    log_success "Report saved to: $REPORT_FILE"
  fi

  log_success "Schema validation completed"
}

main "$@"

