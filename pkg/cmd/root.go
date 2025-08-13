package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

var currentProject *project.Project

type (
	Params struct {
		fx.In

		Args       []string
		Commands   []*cli.Command `group:"commands"`
		Ctx        context.Context
		Lifecycle  fx.Lifecycle
		Shutdowner fx.Shutdowner
		Version    *Version
	}

	Version struct {
		Version   string
		Commit    string
		Timestamp string
	}
)

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
func Run(p Params) {
	cli.VersionPrinter = func(cmd *cli.Command) {
		fmt.Fprintln(cmd.Writer, "Version:", p.Version.Version)
		fmt.Fprintln(cmd.Writer, "Commit:", p.Version.Commit)
		fmt.Fprintln(cmd.Writer, "Date:", p.Version.Timestamp)
	}

	app := &cli.Command{
		Name:  "housekeeper",
		Usage: "A tool for managing ClickHouse schema migrations",
		Description: `housekeeper is a CLI tool that helps you manage ClickHouse database 
schema migrations by comparing desired schema definitions with the current 
database state and generating appropriate migration files.`,
		Version: p.Version.Version,
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

			// Create project instance with current working directory
			pwd, err := os.Getwd()
			if err != nil {
				return ctx, errors.Wrap(err, "failed to get current working directory")
			}

			currentProject = project.New(project.ProjectParams{
				Dir:       pwd,
				Formatter: format.New(format.Defaults),
			})
			return ctx, nil
		},
		Commands: p.Commands,
	}

	p.Lifecycle.Append(fx.StartHook(func() {
		if err := app.Run(p.Ctx, p.Args); err != nil {
			slog.Error("Error running command", "err", err)
			_ = p.Shutdowner.Shutdown(fx.ExitCode(1))
		}

		_ = p.Shutdowner.Shutdown(fx.ExitCode(0))
	}))
}

func requireConfig(cfg *config.Config) func(context.Context, *cli.Command) (context.Context, error) {
	return func(ctx context.Context, cmd *cli.Command) (context.Context, error) {
		if cfg == nil {
			return ctx, errors.New("housekeeper.yaml not found")
		}

		return ctx, nil
	}
}
