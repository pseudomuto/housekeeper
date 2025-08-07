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
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dir := "."
			if path := cmd.String("dir"); path != "" {
				dir = path
			}

			if err := os.MkdirAll(dir, os.ModePerm); err != nil {
				return err
			}

			return project.New(dir).Initialize()
		},
	}
}

// bootstrap returns a CLI command that creates a new housekeeper project
// by extracting schema from an existing ClickHouse server. This command
// combines project initialization with schema extraction and organization.
//
// The bootstrap process:
//  1. Initializes a standard housekeeper project structure
//  2. Connects to the specified ClickHouse server
//  3. Extracts all schema objects (databases, tables, dictionaries, views)
//  4. Organizes the schema into a structured project layout
//  5. Creates individual SQL files for each database object
//  6. Generates import directives for modular schema management
//
// The resulting project structure:
//   - housekeeper.yaml: Configuration file
//   - db/main.sql: Main schema file with imports to all databases
//   - db/schemas/<database>/schema.sql: Database-specific schema with imports
//   - db/schemas/<database>/tables/<table>.sql: Individual table files
//   - db/schemas/<database>/dictionaries/<dict>.sql: Individual dictionary files
//   - db/schemas/<database>/views/<view>.sql: Individual view files
//   - db/migrations/: Directory for future migration files
//
// Example usage:
//
//	# Bootstrap from local ClickHouse instance
//	housekeeper bootstrap --url localhost:9000
//
//	# Bootstrap with cluster support for distributed deployments
//	housekeeper bootstrap --url clickhouse://prod-cluster:9000 --cluster production
//
//	# Bootstrap using environment variable for connection
//	export CH_DATABASE_URL=tcp://localhost:9000
//	housekeeper bootstrap
//
// The command handles all ClickHouse object types and creates a fully
// functional housekeeper project ready for schema management and migrations.
func bootstrap() *cli.Command {
	return &cli.Command{
		Name:  "bootstrap",
		Usage: "Create a new project from an existing ClickHouse server",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "url",
				Aliases:  []string{"u"},
				Usage:    "ClickHouse connection DSN (host:port, clickhouse://..., tcp://...)",
				Sources:  cli.EnvVars("CH_DATABASE_URL"),
				Required: true,
			},
			&cli.StringFlag{
				Name:    "cluster",
				Aliases: []string{"c"},
				Usage:   "Cluster name to inject ON CLUSTER clauses for distributed deployments",
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

			p := project.New(dir)
			if err := p.Initialize(); err != nil {
				return err
			}

			client, err := clickhouse.NewClientWithOptions(
				ctx,
				cmd.String("url"),
				clickhouse.ClientOptions{
					Cluster: cmd.String("cluster"),
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
// creating the necessary directory structure and files.
//
// This function traverses the virtual file system created by project.GenerateImage()
// and materializes it to the actual file system. It handles:
//   - Creating directory structures as needed
//   - Writing file contents with proper permissions (0644)
//   - Preserving the hierarchical organization of the schema files
//
// The function is used by the bootstrap command to convert the virtual
// file system representation of the extracted schema into actual files
// and directories on disk.
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

		// Create file
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
