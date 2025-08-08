package cmd

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
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
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "ClickHouse cluster name to use in configuration (defaults to 'cluster')",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dir := "."
			if path := cmd.String("dir"); path != "" {
				dir = path
			}

			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return err
			}

			options := project.InitOptions{
				Cluster: cmd.String("cluster"),
			}

			return project.New(dir).Initialize(options)
		},
	}
}

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

			config, err := project.LoadConfigFile(configPath)
			if err != nil {
				return errors.Wrap(err, "failed to load existing housekeeper.yaml")
			}

			// Use cluster from existing configuration
			client, err := clickhouse.NewClientWithOptions(
				ctx,
				cmd.String("url"),
				clickhouse.ClientOptions{
					Cluster: config.ClickHouse.Cluster,
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

			// Generate file system image from schema
			fsImage, err := project.GenerateImage(schema)
			if err != nil {
				return errors.Wrap(err, "failed to generate schema image")
			}

			// Overlay the image onto the project directory
			return overlayImage(dir, fsImage)
		},
	}
}

// overlayImage writes the contents of an fs.FS to a directory,
// creating the necessary directory structure and files that don't already exist.
//
// This function traverses the virtual file system created by project.GenerateImage()
// and materializes it to the actual file system. It handles:
//   - Creating directory structures as needed
//   - Writing file contents with proper permissions (0644) only if files don't exist
//   - Preserving existing files (especially housekeeper.yaml and other user configs)
//   - Preserving the hierarchical organization of the schema files
//
// The function is used by the bootstrap command to convert the virtual
// file system representation of the extracted schema into actual files
// and directories on disk without overwriting existing files.
//
// Parameters:
//   - targetDir: The root directory where files should be written
//   - fsImage: The virtual file system containing the schema organization
//
// Returns an error if any file or directory operation fails.
func overlayImage(targetDir string, fsImage fs.FS) error {
	return fs.WalkDir(fsImage, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		targetPath := filepath.Join(targetDir, path)

		if d.IsDir() {
			// Create directory if it doesn't exist
			return os.MkdirAll(targetPath, os.ModePerm)
		}

		// Check if file already exists
		if _, err := os.Stat(targetPath); err == nil {
			// File exists, skip it to preserve user modifications
			return nil
		} else if !os.IsNotExist(err) {
			// Some other error occurred
			return errors.Wrapf(err, "failed to stat file %s", targetPath)
		}

		// File doesn't exist, create it
		file, err := fsImage.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open file %s", path)
		}
		defer file.Close()

		data, err := io.ReadAll(file)
		if err != nil {
			return errors.Wrapf(err, "failed to read file %s", path)
		}

		// Create directory for file if it doesn't exist
		if err := os.MkdirAll(filepath.Dir(targetPath), os.ModePerm); err != nil {
			return errors.Wrapf(err, "failed to create directory for %s", targetPath)
		}

		// Write file
		if err := os.WriteFile(targetPath, data, os.FileMode(0o644)); err != nil {
			return errors.Wrapf(err, "failed to write file %s", targetPath)
		}

		return nil
	})
}
