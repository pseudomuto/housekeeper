#!/usr/bin/env bash

# Cleanup utility for E2E test artifacts

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "$SCRIPT_DIR/lib/common.sh"
source "$SCRIPT_DIR/lib/clickhouse.sh"

TEST_PROJECT_DIR="${1:-}"
CONTAINER_ID="${2:-}"
FORCE_CLEANUP="${3:-false}"

usage() {
  cat <<EOF
Usage: $0 [TEST_PROJECT_DIR] [CONTAINER_ID] [FORCE_CLEANUP]

Clean up E2E test artifacts including temporary directories and containers

Arguments:
  TEST_PROJECT_DIR  Test project directory to remove (optional)
  CONTAINER_ID      ClickHouse container ID to stop/remove (optional)
  FORCE_CLEANUP     Force cleanup without confirmation (true/false, default: false)

Examples:
  $0                                    # Interactive cleanup (prompts for artifacts to clean)
  $0 /tmp/test-project container123     # Clean specific project and container
  $0 "" container123                    # Clean only container
  $0 /tmp/test-project                  # Clean only project directory
  $0 /tmp/test-project container123 true # Force cleanup without prompts

Environment variables:
  CLEANUP_CONTAINERS=true              # Find and clean all housekeeper test containers
  CLEANUP_TEMP_DIRS=true              # Find and clean temp directories matching pattern
EOF
}

# Find and list housekeeper test containers
find_test_containers() {
  log_debug "Searching for housekeeper test containers..."
  docker ps -a --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.CreatedAt}}" \
    --filter "name=housekeeper-e2e" 2>/dev/null || true
}

# Find and list temporary test directories
find_temp_directories() {
  log_debug "Searching for temporary test directories..."
  find /tmp -maxdepth 1 -type d -name "housekeeper-e2e-*" 2>/dev/null | head -20 || true
}

# Clean up ClickHouse container
cleanup_container() {
  local container_id="$1"

  if [[ -z "$container_id" ]]; then
    return 0
  fi

  log_step "Cleaning up ClickHouse container: $container_id"

  # Check if container exists
  if ! docker inspect "$container_id" >/dev/null 2>&1; then
    log_warning "Container $container_id does not exist"
    return 0
  fi

  # Stop container if running
  if docker inspect "$container_id" --format='{{.State.Running}}' 2>/dev/null | grep -q true; then
    log_debug "Stopping container $container_id..."
    docker stop "$container_id" >/dev/null 2>&1 || true
  fi

  # Remove container
  log_debug "Removing container $container_id..."
  docker rm "$container_id" >/dev/null 2>&1 || true

  log_success "Container cleaned up: $container_id"
}

# Clean up test project directory
cleanup_project_directory() {
  local project_dir="$1"

  if [[ -z "$project_dir" || ! -d "$project_dir" ]]; then
    return 0
  fi

  log_step "Cleaning up test project directory: $project_dir"

  # Safety check - ensure it looks like a test directory
  if [[ ! "$project_dir" =~ (test|tmp|e2e) ]] && [[ "$FORCE_CLEANUP" != "true" ]]; then
    log_warning "Directory path doesn't look like a test directory: $project_dir"
    log_warning "Use FORCE_CLEANUP=true to override this safety check"
    return 1
  fi

  # Check if it contains housekeeper files
  if [[ -f "$project_dir/housekeeper.yaml" ]] || [[ -d "$project_dir/db" ]]; then
    log_debug "Found Housekeeper project files"
  else
    log_warning "Directory doesn't appear to contain Housekeeper project files"
  fi

  rm -rf "$project_dir"
  log_success "Project directory cleaned up: $project_dir"
}

# Interactive cleanup
interactive_cleanup() {
  log_info "Starting interactive cleanup..."

  # Find containers
  log_step "Looking for ClickHouse test containers..."
  local containers
  containers=$(docker ps -a -q --filter "name=housekeeper-e2e" 2>/dev/null || true)

  if [[ -n "$containers" ]]; then
    echo
    echo "Found ClickHouse test containers:"
    find_test_containers
    echo

    if confirm "Clean up these containers?"; then
      for container_id in $containers; do
        cleanup_container "$container_id"
      done
    fi
  else
    log_info "No ClickHouse test containers found"
  fi

  # Find temporary directories
  log_step "Looking for temporary test directories..."
  local temp_dirs
  temp_dirs=$(find_temp_directories)

  if [[ -n "$temp_dirs" ]]; then
    echo
    echo "Found temporary test directories:"
    echo "$temp_dirs"
    echo

    if confirm "Clean up these directories?"; then
      while IFS= read -r dir; do
        if [[ -n "$dir" ]]; then
          cleanup_project_directory "$dir"
        fi
      done <<<"$temp_dirs"
    fi
  else
    log_info "No temporary test directories found"
  fi

  # Check for running ClickHouse containers (not just test ones)
  log_step "Checking for other ClickHouse containers..."
  local other_containers
  other_containers=$(docker ps -q --filter "ancestor=clickhouse/clickhouse-server" 2>/dev/null || true)

  if [[ -n "$other_containers" ]]; then
    echo
    echo "Found other ClickHouse containers (not cleaning automatically):"
    docker ps --filter "ancestor=clickhouse/clickhouse-server" --format "table {{.ID}}\t{{.Names}}\t{{.Status}}\t{{.Ports}}"
    echo
    log_info "Use 'docker stop <container-id>' to stop these containers manually if needed"
  fi
}

# Confirmation prompt
confirm() {
  local message="$1"

  if [[ "$FORCE_CLEANUP" == "true" ]]; then
    log_debug "Force cleanup enabled, auto-confirming: $message"
    return 0
  fi

  echo -n "$message (y/N): "
  read -r response
  case "$response" in
  [yY][eE][sS] | [yY])
    return 0
    ;;
  *)
    return 1
    ;;
  esac
}

# Batch cleanup from environment variables
batch_cleanup() {
  log_info "Running batch cleanup from environment variables..."

  if [[ "${CLEANUP_CONTAINERS:-false}" == "true" ]]; then
    log_step "Cleaning up all housekeeper test containers..."
    local containers
    containers=$(docker ps -a -q --filter "name=housekeeper-e2e" 2>/dev/null || true)

    if [[ -n "$containers" ]]; then
      for container_id in $containers; do
        cleanup_container "$container_id"
      done
    else
      log_info "No housekeeper test containers found"
    fi
  fi

  if [[ "${CLEANUP_TEMP_DIRS:-false}" == "true" ]]; then
    log_step "Cleaning up temporary test directories..."
    local temp_dirs
    temp_dirs=$(find_temp_directories)

    if [[ -n "$temp_dirs" ]]; then
      while IFS= read -r dir; do
        if [[ -n "$dir" ]]; then
          cleanup_project_directory "$dir"
        fi
      done <<<"$temp_dirs"
    else
      log_info "No temporary test directories found"
    fi
  fi
}

# Main execution
main() {
  log_info "E2E Test Cleanup Utility"

  # Check if Docker is available
  if ! command_exists docker; then
    log_warning "Docker not available - container cleanup will be skipped"
  fi

  # Specific cleanup if arguments provided
  if [[ -n "$TEST_PROJECT_DIR" || -n "$CONTAINER_ID" ]]; then
    log_info "Performing specific cleanup..."

    if [[ -n "$CONTAINER_ID" ]]; then
      cleanup_container "$CONTAINER_ID"
    fi

    if [[ -n "$TEST_PROJECT_DIR" ]]; then
      cleanup_project_directory "$TEST_PROJECT_DIR"
    fi

    log_success "Specific cleanup completed"
    return 0
  fi

  # Batch cleanup from environment
  if [[ "${CLEANUP_CONTAINERS:-false}" == "true" || "${CLEANUP_TEMP_DIRS:-false}" == "true" ]]; then
    batch_cleanup
    return 0
  fi

  # Interactive cleanup
  interactive_cleanup

  log_success "Cleanup completed"
}

# Show usage if help requested
case "${1:-}" in
-h | --help)
  usage
  exit 0
  ;;
esac

main "$@"

