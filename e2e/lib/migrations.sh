#!/usr/bin/env bash

# Migration management functions for e2e testing

# Create housekeeper database and revisions table
create_housekeeper_database() {
  local dsn="$1"
  
  log_debug "Creating housekeeper database and revisions table..."
  
  # Create housekeeper database
  execute_clickhouse_query "$dsn" "CREATE DATABASE IF NOT EXISTS housekeeper ENGINE = Atomic"
  
  # Create revisions table (matching housekeeper's schema)
  execute_clickhouse_query "$dsn" "
    CREATE TABLE IF NOT EXISTS housekeeper.revisions (
      version String,
      kind Enum('migration' = 1, 'snapshot' = 2),
      executed_at DateTime DEFAULT now(),
      error Nullable(String)
    ) ENGINE = MergeTree()
    ORDER BY (version, executed_at)
  "
}

# Apply a migration file and track it in revisions
apply_migration() {
  local dsn="$1"
  local migration_file="$2"
  local kind="${3:-migration}"  # 'migration' or 'snapshot'
  
  if [[ ! -f "$migration_file" ]]; then
    error "Migration file does not exist: $migration_file"
  fi
  
  local version=$(basename "$migration_file" .sql)
  log_debug "Applying migration: $version"
  
  # Check if migration was already applied
  local count
  count=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM housekeeper.revisions WHERE version = '$version' AND error IS NULL" || echo "0")
  
  if [[ "$count" != "0" ]]; then
    log_debug "Migration $version already applied, skipping"
    return 0
  fi
  
  # Apply the migration
  local error_msg=""
  if ! execute_clickhouse_file "$dsn" "$migration_file"; then
    error_msg="Failed to apply migration"
    log_error "Failed to apply migration: $version"
  fi
  
  # Record in revisions table
  if [[ -z "$error_msg" ]]; then
    execute_clickhouse_query "$dsn" "
      INSERT INTO housekeeper.revisions (version, kind, executed_at, error)
      VALUES ('$version', '$kind', now(), NULL)
    "
    log_debug "Migration $version applied successfully"
  else
    execute_clickhouse_query "$dsn" "
      INSERT INTO housekeeper.revisions (version, kind, executed_at, error)
      VALUES ('$version', '$kind', now(), '$error_msg')
    "
    return 1
  fi
}

# Apply all migrations in a directory
apply_all_migrations() {
  local dsn="$1"
  local migrations_dir="$2"
  
  # Ensure housekeeper database exists
  create_housekeeper_database "$dsn"
  
  # Apply migrations in order
  local count=0
  for migration in "$migrations_dir"/*.sql; do
    if [[ -f "$migration" ]]; then
      if apply_migration "$dsn" "$migration"; then
        ((count++))
      else
        log_error "Migration failed: $(basename "$migration")"
        return 1
      fi
    fi
  done
  
  log_debug "Applied $count migrations"
  return 0
}

# Get applied migrations count
get_applied_migrations_count() {
  local dsn="$1"
  
  execute_clickhouse_query "$dsn" "SELECT count(*) FROM housekeeper.revisions WHERE error IS NULL" || echo "0"
}

# Get last applied migration
get_last_applied_migration() {
  local dsn="$1"
  
  execute_clickhouse_query "$dsn" "SELECT version FROM housekeeper.revisions WHERE error IS NULL ORDER BY executed_at DESC LIMIT 1" || echo "none"
}

# Check if a specific migration was applied
is_migration_applied() {
  local dsn="$1"
  local version="$2"
  
  local count
  count=$(execute_clickhouse_query "$dsn" "SELECT count(*) FROM housekeeper.revisions WHERE version = '$version' AND error IS NULL" || echo "0")
  
  [[ "$count" != "0" ]]
}