// Housekeeper is a comprehensive ClickHouse schema management tool that provides
// robust DDL parsing, intelligent migration generation, and project management
// capabilities for ClickHouse databases.
//
// The tool supports complete ClickHouse DDL operations including databases,
// tables, dictionaries, and views, with smart comparison algorithms and
// professional SQL formatting.
//
// Key features:
//   - Complete ClickHouse DDL parser with expression and query support
//   - Project management with schema compilation and import directives
//   - Intelligent migration generation with rename detection
//   - Professional SQL formatting with configurable styling
//   - Multi-platform builds with Docker image support
//
// Usage:
//
//	# Initialize a new project
//	housekeeper init
//
//	# Generate migrations by comparing schema with database
//	housekeeper diff --dsn localhost:9000 --schema ./db --migrations ./migrations --name setup_schema
//
// For more information and examples, see: https://github.com/pseudomuto/housekeeper
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/pseudomuto/housekeeper/cmd/housekeeper/cmd"
	"github.com/urfave/cli/v3"
)

// Build-time variables set by GoReleaser during release builds.
var (
	version string = "local"                               // Software version (e.g., "v1.0.0")
	commit  string = "local"                               // Git commit hash
	date    string = time.Now().UTC().Format(time.RFC3339) // Build timestamp
)

func main() {
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, "Version:", version)
		fmt.Fprintln(cmd.Writer, "Commit:", commit)
		fmt.Fprintln(cmd.Writer, "Date:", date)
	}

	if err := cmd.Run(context.Background(), version, os.Args); err != nil {
		log.Fatal(err)
	}
}
