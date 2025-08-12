package cmd

import (
	"context"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// bootstrap returns a CLI command that extracts schema from an existing ClickHouse server
// into an already initialized housekeeper project. This command reads the cluster configuration
// from the existing housekeeper.yaml and uses it for connecting to ClickHouse.
//
// Prerequisites:
//  1. Project must already be initialized with `housekeeper init`
//  2. housekeeper.yaml must exist in the current directory
//
// The bootstrap process:
//  1. Loads configuration from existing housekeeper.yaml
//  2. Connects to the specified ClickHouse server using cluster config
//  3. Extracts all schema objects (databases, tables, dictionaries, views)
//  4. Organizes the schema into the existing project layout
//  5. Creates individual SQL files for each database object
//  6. Generates import directives for modular schema management
//
// The resulting project structure adds to existing:
//   - db/main.sql: Updated main schema file with imports to all databases
//   - db/schemas/<database>/schema.sql: Database-specific schema with imports
//   - db/schemas/<database>/tables/<table>.sql: Individual table files
//   - db/schemas/<database>/dictionaries/<dict>.sql: Individual dictionary files
//   - db/schemas/<database>/views/<view>.sql: Individual view files
//
// Example usage:
//
//	# First initialize a project
//	housekeeper init --cluster production
//
//	# Then bootstrap from ClickHouse server (uses cluster from config)
//	housekeeper bootstrap --url localhost:9000
//
//	# Bootstrap using environment variable for connection
//	export CH_DATABASE_URL=tcp://localhost:9000
//	housekeeper bootstrap
//
// The command handles all ClickHouse object types and uses the cluster configuration
// from the existing project for proper ON CLUSTER injection.
func bootstrap() *cli.Command {
	return &cli.Command{
		Name:  "bootstrap",
		Usage: "Extract schema from an existing ClickHouse server into initialized project",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "url",
				Aliases:  []string{"u"},
				Usage:    "ClickHouse connection DSN (host:port, clickhouse://..., tcp://...)",
				Sources:  cli.EnvVars("CH_DATABASE_URL"),
				Required: true,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dir := "."
			if path := cmd.String("dir"); path != "" {
				dir = path
			}

			// Load existing project configuration
			configPath := filepath.Join(dir, "housekeeper.yaml")
			if _, err := os.Stat(configPath); os.IsNotExist(err) {
				return errors.New("housekeeper.yaml not found - please run 'housekeeper init' first to initialize the project")
			}

			// Load configuration to get cluster info
			cfg, err := config.LoadConfigFile(configPath)
			if err != nil {
				return errors.Wrap(err, "failed to load project configuration")
			}

			// Use cluster from existing configuration
			client, err := clickhouse.NewClientWithOptions(
				ctx,
				cmd.String("url"),
				clickhouse.ClientOptions{
					Cluster: cfg.ClickHouse.Cluster,
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

			// Create project instance and bootstrap from the extracted schema
			proj := project.New(dir)
			return proj.BootstrapFromSchema(schema)
		},
	}
}
