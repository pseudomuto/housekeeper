package cmd

import (
	"context"
	"os"

	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

var currentProject *project.Project

// Run creates and executes the main housekeeper CLI application with the given
// version and command-line arguments. This function serves as the main entry
// point for all CLI operations and handles global configuration.
//
// The function creates a CLI application with:
//   - Global --dir flag for specifying project directory
//   - Project auto-detection based on housekeeper.yaml presence
//   - Command registration and routing
//   - Context propagation for cancellation support
//
// Global Flags:
//   - --dir, -d: Project directory (defaults to current directory)
//
// The application automatically detects housekeeper projects by looking for
// housekeeper.yaml in the specified directory. If found, it initializes the
// global currentProject variable for use by subcommands.
//
// Example usage:
//
//	# Run in current directory (auto-detect project)
//	err := Run(ctx, "v1.0.0", []string{"housekeeper", "init"})
//
//	# Run in specific directory
//	err := Run(ctx, "v1.0.0", []string{"housekeeper", "--dir", "/path/to/project", "schema", "compile", "--env", "dev"})
//
// Returns an error if command execution fails or if project detection
// encounters issues.
func Run(ctx context.Context, version string, args []string) error {
	app := &cli.Command{
		Name:  "housekeeper",
		Usage: "A tool for managing ClickHouse schema migrations",
		Description: `housekeeper is a CLI tool that helps you manage ClickHouse database 
schema migrations by comparing desired schema definitions with the current 
database state and generating appropriate migration files.`,
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:        "dir",
				Aliases:     []string{"d"},
				Usage:       "the project directory",
				Value:       ".",
				DefaultText: "Current directory",
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
		},
		Before: func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
			projectDir := cmd.String("dir")

			// Change to project directory first
			if err := os.Chdir(projectDir); err != nil {
				return ctx, err
			}

			// Check if this is a housekeeper project
			_, err := os.Stat("housekeeper.yaml")
			if os.IsNotExist(err) {
				return ctx, nil
			}

			if err != nil {
				return ctx, err
			}

			// Create project instance using current directory (since we've already changed to it)
			pwd, _ := os.Getwd()
			currentProject = project.New(pwd)
			return ctx, nil
		},
		Commands: []*cli.Command{
			bootstrap(),
			dev(),
			initCmd(),
			schema(),
		},
	}

	return app.Run(ctx, args)
}
