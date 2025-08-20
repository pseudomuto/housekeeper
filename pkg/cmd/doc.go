// Package cmd provides CLI commands for the housekeeper tool.
//
// This package implements the command-line interface for housekeeper,
// providing commands for project management, schema operations, and
// database migrations. It supports both standalone operations and
// project-based workflows with comprehensive ClickHouse integration.
//
// # Available Commands
//
// The cmd package currently provides:
//   - init: Initialize a new housekeeper project structure
//   - bootstrap: Create a new project from an existing ClickHouse server
//   - schema dump: Extract schema from live ClickHouse instances
//   - schema compile: Compile and format project schema files
//   - diff: Compare schema with database and generate migrations (planned)
//
// # Command Structure
//
// Each command is implemented as a separate function that returns a
// *cli.Command, following the urfave/cli/v3 pattern. Commands are
// designed to be composable and testable, with proper error handling
// and comprehensive help text.
//
// # Global Options
//
// All commands support global flags:
//   - --dir, -d: Specify project directory (defaults to current directory)
//   - --help, -h: Display command help
//   - --version: Display version information
//
// # Example Usage
//
// Commands are registered in the main application and can be invoked
// from the command line:
//
//	housekeeper init                                         # Initialize project
//	housekeeper bootstrap --url localhost:9000              # Bootstrap from ClickHouse server
//	housekeeper bootstrap --url host:9000 --cluster prod    # Bootstrap with cluster support
//	housekeeper schema dump --url localhost:9000            # Dump schema from ClickHouse
//	housekeeper schema compile --env production              # Compile project schema
//	housekeeper diff --url host:9000 ...                   # Generate migrations (planned)
//
// # ClickHouse Integration
//
// The bootstrap and schema dump commands provide comprehensive ClickHouse integration:
//   - Flexible DSN parsing (host:port, clickhouse://, tcp://)
//   - Distributed cluster support with automatic ON CLUSTER injection
//   - Complete schema extraction (databases, tables, dictionaries, views)
//   - Professional SQL formatting with consistent styling
//   - Organized project structure generation for bootstrapped schemas
//
// Each command provides comprehensive help and validation to ensure
// proper usage and clear error messages.
package cmd
