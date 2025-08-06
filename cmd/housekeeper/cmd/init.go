package cmd

import (
	"context"

	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// initCmd returns a CLI command that initializes a new housekeeper project
// in the current directory. This command creates the standard project
// structure with configuration files and directory layout.
//
// The initialization process is idempotent - running it multiple times
// will not overwrite existing files, making it safe to run in existing
// directories.
//
// Created structure:
//   - housekeeper.yaml: Configuration file with environment definitions
//   - db/: Main database schema directory
//   - db/main.sql: Template schema file with examples
//   - db/migrations/: Directory for generated migration files
//   - db/migrations/dev/: Development environment migrations
//   - db/schemas/: Organized schema file storage
//
// Example usage:
//
//	# Initialize a project in current directory
//	housekeeper init
//
// The command will create the necessary files and directories while
// preserving any existing content, making it safe to run in populated
// directories.
func initCmd() *cli.Command {
	return &cli.Command{
		Name:  "init",
		Usage: "Initialize a project in the current directory",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			p := project.New(".")
			if err := p.Initialize(); err != nil {
				return err
			}

			return nil
		},
	}
}
