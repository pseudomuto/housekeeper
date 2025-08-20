#!/usr/bin/env bash

# Validation helper functions

# Run validation query and check results
run_validation_query() {
  local dsn="$1"
  local sql_file="$2"

  if [[ ! -f "$sql_file" ]]; then
    error "Validation SQL file not found: $sql_file"
  fi

  log_debug "Running validation query: $(basename "$sql_file")"

  # Execute the validation query
  local result
  result=$(execute_clickhouse_file "$dsn" "$sql_file" "TSV")
  local exit_code=$?

  if [[ $exit_code -ne 0 ]]; then
    error "Validation query failed: $(basename "$sql_file")"
  fi

  # Check if we got results (validation queries should always return results)
  if [[ -z "$result" ]]; then
    error "Validation query returned no results: $(basename "$sql_file")"
  fi

  # Parse and validate results
  local failed_checks=0
  local total_checks=0

  while IFS=$'\t' read -r check_name result_count; do
    total_checks=$((total_checks + 1))

    if [[ "$result_count" -eq 0 ]] || [[ -z "$result_count" ]]; then
      log_error "  ✗ $check_name (expected: non-zero, got: ${result_count:-empty})"
      failed_checks=$((failed_checks + 1))
    else
      log_debug "  ✓ $check_name (result: $result_count)"
    fi
  done <<<"$result"

  if [[ $failed_checks -gt 0 ]]; then
    error "Validation failed: $failed_checks/$total_checks checks failed in $(basename "$sql_file")"
  fi

  log_debug "Validation passed: $total_checks/$total_checks checks passed in $(basename "$sql_file")"
}

# Validate database connectivity
validate_connection() {
  local dsn="$1"

  log_debug "Validating ClickHouse connection: $dsn"

  local result
  result=$(execute_clickhouse_query "$dsn" "SELECT 1" "TSV")

  if [[ "$result" != "1" ]]; then
    error "Failed to connect to ClickHouse or invalid response: expected '1', got '$result'"
  fi

  log_debug "ClickHouse connection validated"
}

# Validate Housekeeper binary exists and is executable
validate_housekeeper_binary() {
  local binary_path="${1:-bin/housekeeper}"

  if [[ ! -f "$binary_path" ]]; then
    error "Housekeeper binary not found: $binary_path"
  fi

  if [[ ! -x "$binary_path" ]]; then
    error "Housekeeper binary is not executable: $binary_path"
  fi

  log_debug "Housekeeper binary validated: $binary_path"
}

# Validate project structure
validate_project_structure() {
  local project_dir="$1"

  log_debug "Validating project structure in: $project_dir"

  # Check required files
  check_file_exists "$project_dir/housekeeper.yaml" "Configuration file"
  check_file_exists "$project_dir/db/main.sql" "Main schema file"
  check_dir_exists "$project_dir/db/migrations" "Migrations directory"

  log_debug "Project structure validated"
}

# Validate migration directory state
validate_migration_state() {
  local migrations_dir="$1"
  local expected_files="$2"

  log_debug "Validating migration directory state: $migrations_dir"

  if [[ ! -d "$migrations_dir" ]]; then
    error "Migrations directory does not exist: $migrations_dir"
  fi

  local actual_files
  actual_files=$(find "$migrations_dir" -name "*.sql" | wc -l)

  if [[ $actual_files -ne $expected_files ]]; then
    error "Expected $expected_files migration files, found $actual_files"
  fi

  # Check sum file exists
  check_file_exists "$migrations_dir/housekeeper.sum" "Sum file"

  log_debug "Migration state validated: $actual_files files"
}

# Validate revision records
validate_revision_records() {
  local dsn="$1"
  local expected_count="$2"

  log_debug "Validating revision records (expected: $expected_count)"

  local actual_count
  actual_count=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM housekeeper.revisions" "TSV")

  if [[ "$actual_count" -lt "$expected_count" ]]; then
    error "Expected at least $expected_count revisions, found $actual_count"
  fi

  # Check for failed revisions
  local failed_count
  failed_count=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM housekeeper.revisions WHERE error IS NOT NULL" "TSV")

  if [[ "$failed_count" -gt 0 ]]; then
    error "Found $failed_count failed revision(s)"
  fi

  log_debug "Revision records validated: $actual_count total, 0 failed"
}

# Validate schema objects count
validate_schema_objects() {
  local dsn="$1"
  local expected_tables="$2"
  local expected_dictionaries="$3"
  local expected_views="$4"

  log_debug "Validating schema objects (tables: $expected_tables, dicts: $expected_dictionaries, views: $expected_views)"

  # Validate tables (excluding system)
  local actual_tables
  actual_tables=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM system.tables WHERE database IN ('housekeeper', 'analytics')" "TSV")

  if [[ "$actual_tables" -ne "$expected_tables" ]]; then
    error "Expected $expected_tables tables, found $actual_tables"
  fi

  # Validate dictionaries
  local actual_dictionaries
  actual_dictionaries=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM system.dictionaries WHERE database = 'analytics'" "TSV")

  if [[ "$actual_dictionaries" -ne "$expected_dictionaries" ]]; then
    error "Expected $expected_dictionaries dictionaries, found $actual_dictionaries"
  fi

  # Validate views (materialized + regular)
  local actual_views
  actual_views=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM system.tables WHERE database = 'analytics' AND engine LIKE '%View%'" "TSV")

  if [[ "$actual_views" -ne "$expected_views" ]]; then
    error "Expected $expected_views views, found $actual_views"
  fi

  log_debug "Schema objects validated: $actual_tables tables, $actual_dictionaries dictionaries, $actual_views views"
}

# Generate validation report
generate_validation_report() {
  local dsn="$1"
  local output_file="${2:-/dev/stdout}"

  {
    echo "=== ClickHouse Schema Validation Report ==="
    echo "Generated: $(timestamp)"
    echo "DSN: $dsn"
    echo

    echo "--- Databases ---"
    execute_clickhouse_query "$dsn" "SELECT name, engine FROM system.databases WHERE name NOT LIKE 'system%' ORDER BY name" "TSV" |
      while IFS=$'\t' read -r name engine; do
        echo "  $name ($engine)"
      done
    echo

    echo "--- Tables ---"
    execute_clickhouse_query "$dsn" "SELECT database, name, engine FROM system.tables WHERE database IN ('housekeeper', 'analytics') ORDER BY database, name" "TSV" |
      while IFS=$'\t' read -r database name engine; do
        echo "  $database.$name ($engine)"
      done
    echo

    echo "--- Dictionaries ---"
    execute_clickhouse_query "$dsn" "SELECT database, name, status FROM system.dictionaries WHERE database = 'analytics' ORDER BY name" "TSV" |
      while IFS=$'\t' read -r database name status; do
        echo "  $database.$name ($status)"
      done
    echo

    echo "--- Revisions ---"
    execute_clickhouse_query "$dsn" "SELECT version, kind, executed_at, CASE WHEN error IS NULL THEN 'SUCCESS' ELSE 'FAILED' END FROM housekeeper.revisions ORDER BY executed_at" "TSV" |
      while IFS=$'\t' read -r version kind executed_at status; do
        echo "  $version [$kind] $executed_at - $status"
      done
    echo

  } >"$output_file"
}

