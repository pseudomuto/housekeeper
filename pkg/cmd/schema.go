package cmd

import (
	"context"
	"os"

	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/urfave/cli/v3"
)

// schema returns a CLI command that provides schema-related operations for
// housekeeper projects. This command serves as a parent for all schema
// manipulation commands and requires a valid housekeeper project.
//
// The command requires that the current directory (or specified --dir) contains
// a housekeeper.yaml file, indicating it's a valid housekeeper project. If no
// project is detected, the command will fail with an error.
//
// Available subcommands:
//   - compile: Compile and format the project schema
//
// Example usage:
//
//	# Compile project schema
//	housekeeper schema compile
//
//	# Compile and save to file
//	housekeeper schema compile --out compiled.sql
//
// The command automatically validates project structure before executing
// any subcommands.
func schema(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "Commands for working with schemas",
		Commands: []*cli.Command{
			schemaDump(),
			schemaParse(cfg),
		},
	}
}

// schemaDump returns a CLI command that extracts schema from a live ClickHouse
// instance and outputs formatted DDL statements. The command connects to ClickHouse,
// extracts all schema objects (databases, tables, dictionaries, views), and
// formats them with professional SQL styling.
//
// The command supports distributed ClickHouse deployments through the --cluster
// flag, which automatically injects ON CLUSTER clauses into all extracted DDL
// statements. This addresses the limitation where ClickHouse system tables don't
// include cluster information in dumped schemas.
//
// Required flags:
//   - --url, -u: ClickHouse connection DSN (supports various formats)
//
// Optional flags:
//   - --cluster, -c: Cluster name for distributed deployments
//   - --out, -o: Output file (defaults to stdout)
//
// DSN formats supported:
//   - Simple host:port: "localhost:9000"
//   - Full DSN: "clickhouse://user:pass@host:port/database"
//   - TCP protocol: "tcp://host:port?username=user&database=db"
//
// Example usage:
//
//	# Dump schema to stdout
//	housekeeper schema dump --url localhost:9000
//
//	# Dump with cluster support for distributed deployments
//	housekeeper schema dump --url localhost:9000 --cluster production_cluster
//
//	# Dump to file with authentication
//	housekeeper schema dump --url "clickhouse://user:pass@host:9000/mydb" --out schema.sql
//
// The command extracts all non-system schema objects and validates them through
// the parser before outputting formatted DDL suitable for deployment or version control.
func schemaDump() *cli.Command {
	return &cli.Command{
		Name:  "dump",
		Usage: "Extract and format schema from a ClickHouse instance",
		Flags: []cli.Flag{
			urlFlag,
			&cli.StringFlag{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "Cluster name to inject ON CLUSTER clauses for distributed deployments",
			},
			&cli.StringSliceFlag{
				Name:    "ignore-databases",
				Aliases: []string{"i"},
				Usage:   "Databases to exclude from schema dump (can be specified multiple times)",
			},
			&cli.StringFlag{
				Name:        "out",
				Aliases:     []string{"o"},
				Usage:       "Output file path for dumped schema",
				DefaultText: "stdout",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			client, err := clickhouse.NewClientWithOptions(
				ctx,
				cmd.String("url"),
				clickhouse.ClientOptions{
					Cluster:         cmd.String("cluster"),
					IgnoreDatabases: cmd.StringSlice("ignore-databases"),
				},
			)
			if err != nil {
				return err
			}
			defer func() { _ = client.Close() }()

			schema, err := client.GetSchema(ctx)
			if err != nil {
				return err
			}

			w := cmd.Writer
			if cmd.String("out") != "" {
				f, err := os.Create(cmd.String("out"))
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				w = f
			}

			return format.FormatSQL(w, format.Defaults, schema)
		},
	}
}

// schemaParse returns a CLI command that compiles and formats schema files
// for the project. The command processes schema files with import
// directives, resolves dependencies, and outputs formatted ClickHouse DDL.
//
// The compilation process:
//  1. Reads the main schema file defined in project configuration
//  2. Processes -- housekeeper:import directives recursively
//  3. Parses all ClickHouse DDL statements
//  4. Formats the output with professional styling
//  5. Outputs to stdout or specified file
//
// Optional flags:
//   - --out, -o: Output file path (defaults to stdout)
//
// Example usage:
//
//	# Compile project schema to stdout
//	housekeeper schema compile
//
//	# Compile project schema to file
//	housekeeper schema compile --out schema.sql
//
//	# Compile with custom project directory
//	housekeeper --dir /path/to/project schema compile
//
// The command validates that the project is properly initialized and that
// all imported schema files are accessible.
func schemaParse(cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:  "compile",
		Usage: "Compile the project schema",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "out",
				Aliases: []string{"o"},
				Usage:   "File to write the output to",
			},
		},
		Before: requireConfig(cfg),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// Compile project schema using shared utility
			statements, err := compileProjectSchema(cfg)
			if err != nil {
				return err
			}

			sql := &parser.SQL{Statements: statements}

			w := cmd.Writer
			if path := cmd.String("out"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				w = f
			}

			return format.FormatSQL(w, format.Defaults, sql)
		},
	}
}
