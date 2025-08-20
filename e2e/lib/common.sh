#!/usr/bin/env bash

# Common utility functions for E2E testing

# Colors for output (if not already defined)
if [[ -z "${RED:-}" ]]; then
  RED='\033[0;31m'
  GREEN='\033[0;32m'
  YELLOW='\033[1;33m'
  BLUE='\033[0;34m'
  NC='\033[0m' # No Color
fi

# Logging functions
log_info() {
  echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
  echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
  echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
  echo -e "${RED}[ERROR]${NC} $*"
}

log_debug() {
  if [[ "${DEBUG:-false}" == "true" ]]; then
    echo -e "${YELLOW}[DEBUG]${NC} $*" >&2
  fi
}

log_phase() {
  echo
  echo -e "${GREEN}==================== $1 ====================${NC}"
  echo
}

log_step() {
  echo -e "${BLUE}→${NC} $*"
}

# Error handling
error() {
  log_error "$*"
  exit 1
}

# File/directory validation
check_file_exists() {
  local file="$1"
  local description="${2:-File}"

  if [[ ! -f "$file" ]]; then
    error "$description does not exist: $file"
  fi
  log_debug "$description exists: $file"
}

check_dir_exists() {
  local dir="$1"
  local description="${2:-Directory}"

  if [[ ! -d "$dir" ]]; then
    error "$description does not exist: $dir"
  fi
  log_debug "$description exists: $dir"
}

# Wait with timeout
wait_with_timeout() {
  local timeout="$1"
  local interval="${2:-1}"
  local command="$3"

  local elapsed=0
  while [[ $elapsed -lt $timeout ]]; do
    if eval "$command" >/dev/null 2>&1; then
      return 0
    fi
    sleep "$interval"
    elapsed=$((elapsed + interval))
  done

  return 1
}

# Check if command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# Ensure required commands are available
check_requirements() {
  local missing=()

  if ! command_exists docker; then
    missing+=("docker")
  fi

  if ! command_exists mktemp; then
    missing+=("mktemp")
  fi

  if [[ ${#missing[@]} -gt 0 ]]; then
    error "Missing required commands: ${missing[*]}"
  fi
}

# Safe cleanup with error handling
safe_cleanup() {
  local resource="$1"
  local cleanup_command="$2"

  if [[ -n "$resource" ]]; then
    log_debug "Cleaning up $resource..."
    if ! eval "$cleanup_command" >/dev/null 2>&1; then
      log_warning "Failed to cleanup $resource"
    fi
  fi
}

# Get timestamp for logging
timestamp() {
  date '+%Y-%m-%d %H:%M:%S'
}

# Progress indicator
show_progress() {
  local pid=$1
  local message="${2:-Processing}"

  local chars="/-\|"
  local i=0

  while kill -0 "$pid" 2>/dev/null; do
    printf "\r%s %s" "$message" "${chars:$((i % 4)):1}"
    sleep 0.1
    ((i++))
  done
  printf "\r%s ✓\n" "$message"
}

# Retry mechanism
retry() {
  local max_attempts="$1"
  local delay="$2"
  shift 2
  local command="$*"

  local attempt=1
  while [[ $attempt -le $max_attempts ]]; do
    if eval "$command"; then
      return 0
    fi

    if [[ $attempt -lt $max_attempts ]]; then
      log_debug "Attempt $attempt failed, retrying in ${delay}s..."
      sleep "$delay"
    fi

    ((attempt++))
  done

  log_error "Command failed after $max_attempts attempts: $command"
  return 1
}

