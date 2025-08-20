#!/usr/bin/env bash

# ClickHouse container management functions

# Start ClickHouse container
start_clickhouse_container() {
  local version="$1"
  local container_name="housekeeper-e2e-$(date +%s)"

  log_debug "Starting ClickHouse container with version $version"

  # Start container with basic configuration
  # Create a test user to avoid conflicts with default user
  local container_id
  container_id=$(docker run -d \
    --name "$container_name" \
    --rm \
    -p 0:9000 \
    -p 0:8123 \
    --ulimit nofile=262144:262144 \
    -e CLICKHOUSE_USER=test \
    -e CLICKHOUSE_PASSWORD=test \
    -e CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1 \
    "clickhouse/clickhouse-server:$version" 2>/dev/null)

  if [[ -z "$container_id" ]] || [[ ${#container_id} -lt 12 ]]; then
    log_error "Failed to start ClickHouse container"
    error "Container startup failed"
  fi

  log_debug "ClickHouse container started: $container_id"
  
  # Give container a moment to initialize
  sleep 3
  
  echo "$container_id"
}

# Get ClickHouse DSN from container
get_clickhouse_dsn() {
  local container_id="$1"

  # Wait for container to be running
  local attempts=0
  while [[ $attempts -lt 30 ]]; do
    if docker inspect "$container_id" --format='{{.State.Running}}' | grep -q true; then
      break
    fi
    sleep 1
    ((attempts++))
  done

  if [[ $attempts -eq 30 ]]; then
    error "Container failed to start within 30 seconds"
  fi

  # Get the mapped port
  local port
  port=$(docker port "$container_id" 9000 | cut -d: -f2)

  if [[ -z "$port" ]]; then
    error "Failed to get ClickHouse port from container"
  fi

  # Return full DSN with test credentials
  echo "clickhouse://test:test@localhost:$port/default"
}

# Wait for ClickHouse to be ready
wait_for_clickhouse() {
  local dsn="$1"
  local max_attempts=30
  local attempt=1

  log_debug "Waiting for ClickHouse to be ready at $dsn"

  # Extract container name from DSN (we need the container ID for exec)
  # For this test setup, we'll look for running containers
  local container_id
  container_id=$(docker ps --filter "name=housekeeper-e2e" --format "{{.ID}}" | head -n1)
  
  if [[ -z "$container_id" ]]; then
    log_error "Could not find ClickHouse container"
    return 1
  fi

  while [[ $attempt -le $max_attempts ]]; do
    # Test connection from inside the container (which we know works)
    if docker exec "$container_id" clickhouse-client --user test --password test --query "SELECT 1" >/dev/null 2>&1; then
      log_debug "ClickHouse is ready after $attempt attempts"
      return 0
    fi
    
    log_debug "ClickHouse not ready (attempt $attempt/$max_attempts)"
    sleep 2
    ((attempt++))
  done

  log_error "ClickHouse failed to become ready within $((max_attempts * 2)) seconds"
  return 1
}

# Troubleshoot ClickHouse container issues
troubleshoot_clickhouse() {
  local container_id="$1"
  local dsn="$2"
  
  log_error "Troubleshooting ClickHouse container..."
  
  # Validate container ID first
  if [[ -z "$container_id" ]]; then
    log_error "Container ID is empty"
    return 1
  fi
  
  # Check if container exists and is running
  local inspect_output
  if inspect_output=$(docker inspect "$container_id" 2>&1); then
    local state
    state=$(docker inspect "$container_id" --format='{{.State.Status}}' 2>/dev/null || echo "unknown")
    log_error "Container status: $state"
    
    if [[ "$state" == "running" ]]; then
      # Show recent logs
      log_error "Recent container logs:"
      if docker logs --tail 20 "$container_id" 2>/dev/null; then
        docker logs --tail 20 "$container_id" 2>&1 | sed 's/^/  /'
      else
        log_error "  Failed to get container logs"
      fi
      
      # Check port mapping
      log_error "Port mappings:"
      if docker port "$container_id" 2>/dev/null; then
        docker port "$container_id" 2>/dev/null | sed 's/^/  /'
      else
        log_error "  No port mappings found or failed to get ports"
      fi
      
      # Check if port is accessible
      if [[ -n "$dsn" ]]; then
        local host port
        host=$(echo "$dsn" | cut -d: -f1)
        port=$(echo "$dsn" | cut -d: -f2)
        
        log_error "Testing connectivity to $host:$port..."
        if command -v nc >/dev/null 2>&1; then
          if nc -z "$host" "$port" 2>/dev/null; then
            log_error "Port $port is accessible on $host"
          else
            log_error "Port $port is NOT accessible on $host"
          fi
        else
          log_error "netcat (nc) not available for port testing"
        fi
      fi
    elif [[ "$state" == "exited" ]]; then
      log_error "Container has exited"
      log_error "Exit code: $(docker inspect "$container_id" --format='{{.State.ExitCode}}' 2>/dev/null || echo "unknown")"
      log_error "Recent container logs:"
      docker logs --tail 20 "$container_id" 2>&1 | sed 's/^/  /' || log_error "  Failed to get logs"
    else
      log_error "Container is in unexpected state: $state"
    fi
  else
    log_error "Container $container_id does not exist or cannot be inspected"
    log_error "Docker inspect error: $inspect_output"
    
    # List running containers for context
    log_error "Currently running containers:"
    if docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}" 2>/dev/null; then
      docker ps --format "table {{.ID}}\t{{.Names}}\t{{.Status}}" 2>/dev/null | sed 's/^/  /'
    else
      log_error "  Failed to list containers"
    fi
  fi
}

# Execute ClickHouse query
execute_clickhouse_query() {
  local dsn="$1"
  local query="$2"
  local format="${3:-TabSeparated}"

  # Find the ClickHouse container
  local container_id
  container_id=$(docker ps --filter "name=housekeeper-e2e" --format "{{.ID}}" | head -n1)
  
  if [[ -z "$container_id" ]]; then
    log_error "Could not find ClickHouse container"
    return 1
  fi

  # Execute query inside the container
  docker exec "$container_id" clickhouse-client \
    --user test \
    --password test \
    --format "$format" \
    --query "$query" 2>/dev/null
}

# Execute ClickHouse query from file
execute_clickhouse_file() {
  local dsn="$1"
  local file="$2"
  local format="${3:-TabSeparated}"

  if [[ ! -f "$file" ]]; then
    error "SQL file does not exist: $file"
  fi

  # Find the ClickHouse container
  local container_id
  container_id=$(docker ps --filter "name=housekeeper-e2e" --format "{{.ID}}" | head -n1)
  
  if [[ -z "$container_id" ]]; then
    log_error "Could not find ClickHouse container"
    return 1
  fi

  # Copy file to container and execute
  docker cp "$file" "$container_id:/tmp/query.sql"
  docker exec "$container_id" clickhouse-client \
    --user test \
    --password test \
    --format "$format" \
    --queries-file /tmp/query.sql 2>/dev/null
}

# Check if ClickHouse container is running
is_clickhouse_running() {
  local container_id="$1"

  if [[ -z "$container_id" ]]; then
    return 1
  fi

  docker inspect "$container_id" --format='{{.State.Running}}' 2>/dev/null | grep -q true
}

# Stop ClickHouse container
stop_clickhouse_container() {
  local container_id="$1"

  if [[ -z "$container_id" ]]; then
    return 0
  fi

  log_debug "Stopping ClickHouse container: $container_id"

  if is_clickhouse_running "$container_id"; then
    docker stop "$container_id" >/dev/null 2>&1 || true
  fi

  # Container will be automatically removed due to --rm flag
}

# Get ClickHouse container logs
get_clickhouse_logs() {
  local container_id="$1"
  local lines="${2:-50}"

  if [[ -z "$container_id" ]]; then
    return 1
  fi

  docker logs --tail "$lines" "$container_id" 2>&1
}

# Check ClickHouse version
check_clickhouse_version() {
  local dsn="$1"

  execute_clickhouse_query "$dsn" "SELECT version()" "TSV"
}

