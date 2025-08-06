package cmd

import (
	"context"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/format"
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
//   - compile: Compile and format schema for a specific environment
//
// Example usage:
//
//	# Compile schema for development environment
//	housekeeper schema compile --env dev
//
//	# Compile and save to file
//	housekeeper schema compile --env production --out compiled.sql
//
// The command automatically validates project structure before executing
// any subcommands.
func schema() *cli.Command {
	return &cli.Command{
		Name:  "schema",
		Usage: "Commands for working with schemas",
		Commands: []*cli.Command{
			schemaParse(),
		},
		Before: func(ctx context.Context, cmd *cli.Command) error {
			if currentProject == nil {
				return errors.Errorf("not a housekeeper project. Dir: %s", cmd.String("dir"))
			}

			return nil
		},
	}
}

// schemaParse returns a CLI command that compiles and formats schema files
// for a specific environment. The command processes schema files with import
// directives, resolves dependencies, and outputs formatted ClickHouse DDL.
//
// The compilation process:
//  1. Reads the main schema file for the specified environment
//  2. Processes -- housekeeper:import directives recursively
//  3. Parses all ClickHouse DDL statements
//  4. Formats the output with professional styling
//  5. Outputs to stdout or specified file
//
// Required flags:
//   - --env, -e: Environment name to compile (must exist in project config)
//
// Optional flags:
//   - --out, -o: Output file path (defaults to stdout)
//
// Example usage:
//
//	# Compile development environment to stdout
//	housekeeper schema compile --env dev
//
//	# Compile production environment to file
//	housekeeper schema compile --env production --out prod-schema.sql
//
//	# Compile with custom project directory
//	housekeeper --dir /path/to/project schema compile --env staging
//
// The command validates that the specified environment exists in the project
// configuration and that all imported schema files are accessible.
func schemaParse() *cli.Command {
	return &cli.Command{
		Name:  "compile",
		Usage: "Compile the schema for the specified environment",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "env",
				Aliases:  []string{"e"},
				Usage:    "The environment to compile for",
				Required: true,
			},
			&cli.StringFlag{
				Name:    "out",
				Aliases: []string{"o"},
				Usage:   "File to write the output to",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			grammar, err := currentProject.ParseSchema(cmd.String("env"))
			if err != nil {
				return err
			}

			w := cmd.Writer
			if path := cmd.String("out"); path != "" {
				f, err := os.Create(path)
				if err != nil {
					return err
				}
				defer func() { _ = f.Close() }()
				w = f
			}

			return format.FormatGrammar(w, format.Defaults, grammar)
		},
	}
}
