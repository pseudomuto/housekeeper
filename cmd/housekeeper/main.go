// Housekeeper is a comprehensive ClickHouse schema management tool that provides
// robust DDL parsing, intelligent migration generation, and project management
// capabilities for ClickHouse databases.
//
// The tool supports complete ClickHouse DDL operations including databases,
// tables, dictionaries, and views, with smart comparison algorithms and
// professional SQL formatting. It includes full support for distributed
// ClickHouse clusters with automatic ON CLUSTER clause injection.
//
// Key features:
//   - Complete ClickHouse DDL parser with expression and query support
//   - Project management with schema compilation and import directives
//   - Project bootstrapping from existing ClickHouse instances
//   - Intelligent migration generation with rename detection
//   - Professional SQL formatting with configurable styling
//   - Multi-platform builds with Docker image support
//   - Distributed cluster support with ON CLUSTER injection
//   - Schema dumping from live ClickHouse instances
//   - Flexible DSN parsing for various connection formats
//
// Usage:
//
//	# Initialize a new project
//	housekeeper init
//
//	# Bootstrap project from existing ClickHouse server
//	housekeeper bootstrap --url localhost:9000
//
//	# Bootstrap with cluster support for distributed deployments
//	housekeeper bootstrap --url localhost:9000 --cluster production_cluster
//
//	# Dump schema from ClickHouse instance
//	housekeeper schema dump --url localhost:9000
//
//	# Compile schema for specific environment
//	housekeeper schema compile --env production
//
//	# Generate migrations by comparing schema with database (planned)
//	housekeeper diff --dsn localhost:9000 --schema ./db --migrations ./migrations --name setup_schema
//
// For more information and examples, see: https://github.com/pseudomuto/housekeeper
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/cmd"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"go.uber.org/fx"
)

// Build-time variables set by GoReleaser during release builds.
var (
	version string = "local"                               // Software version (e.g., "v1.0.0")
	commit  string = "local"                               // Git commit hash
	date    string = time.Now().UTC().Format(time.RFC3339) // Build timestamp
)

type Params struct {
	fx.Out

	Dir           string `name:"project_dir"`
	FormatOptions format.FormatterOptions
	Version       *cmd.Version
}

func parseDirFlag(args []string) (string, []string) {
	var dir string
	var newArgs []string

	for i := 0; i < len(args); i++ {
		arg := args[i]

		if arg == "-d" || arg == "--dir" {
			if i+1 < len(args) {
				dir = args[i+1]
				i++
			}
		} else if len(arg) > 6 && arg[:6] == "--dir=" {
			dir = arg[6:]
		} else {
			newArgs = append(newArgs, arg)
		}
	}

	return dir, newArgs
}

func main() {
	dir, args := parseDirFlag(os.Args)
	if dir != "" {
		if err := os.Chdir(dir); err != nil {
			log.Fatal(err)
		}
	}

	pwd, _ := os.Getwd()

	app := fx.New(
		fx.Supply(
			args,
			Params{
				Dir:           pwd,
				FormatOptions: format.Defaults,
				Version: &cmd.Version{
					Version:   version,
					Commit:    commit,
					Timestamp: date,
				},
			},
		),
		fx.Provide(
			context.Background,
			project.New,
			newDockerClient,
		),
		cmd.Module,
		config.Module,
		format.Module,
		fx.NopLogger,
	)

	app.Run()

	if err := app.Err(); err != nil {
		log.Fatal(err)
	}
}

func newDockerClient() (docker.DockerClient, error) {
	dc, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create docker client")
	}

	return dc, nil
}
