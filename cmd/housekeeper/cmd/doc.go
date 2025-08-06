// Package cmd provides CLI commands for the housekeeper tool.
//
// This package implements the command-line interface for housekeeper,
// providing commands for project management, schema operations, and
// database migrations.
//
// # Available Commands
//
// The cmd package currently provides:
//   - init: Initialize a new housekeeper project structure
//   - diff: Compare schema with database and generate migrations (planned)
//   - parse: Parse and validate schema files (planned)
//   - format: Format SQL files with professional styling (planned)
//
// # Command Structure
//
// Each command is implemented as a separate function that returns a
// *cli.Command, following the urfave/cli/v3 pattern. Commands are
// designed to be composable and testable.
//
// # Example Usage
//
// Commands are registered in the main application and can be invoked
// from the command line:
//
//	housekeeper init                           # Initialize project
//	housekeeper diff --dsn host:9000 ...      # Generate migrations
//	housekeeper parse --env production        # Parse schema
//	housekeeper format --file schema.sql     # Format SQL files
//
// Each command provides comprehensive help and validation to ensure
// proper usage and clear error messages.
package cmd
