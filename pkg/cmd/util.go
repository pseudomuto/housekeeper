package cmd

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	schemapkg "github.com/pseudomuto/housekeeper/pkg/schema"
)

// runContainer starts a ClickHouse container with the given options, loads and executes
// existing migrations, and returns the container and client for further use.
func runContainer(ctx context.Context, w io.Writer, opts docker.DockerOptions, cfg *config.Config, dockerClient docker.DockerClient) (*docker.ClickHouseContainer, *clickhouse.Client, error) {
	// 1. Load and validate migrations before creating container
	migrationDir, err := migrator.LoadMigrationDir(os.DirFS(cfg.Dir))
	if err != nil { // nolint: nestif
		// If migrations directory doesn't exist, that's okay - just no migrations to apply
		if os.IsNotExist(errors.Cause(err)) {
			migrationDir = &migrator.MigrationDir{Migrations: []*migrator.Migration{}}
		} else {
			return nil, nil, errors.Wrap(err, "failed to load migration directory")
		}
	} else {
		// Validate existing migrations
		isValid, err := migrationDir.Validate()
		if err != nil {
			return nil, nil, errors.Wrap(err, "failed to validate migrations")
		}
		if !isValid {
			return nil, nil, errors.New("migration directory failed validation - files have been modified")
		}
	}

	// 2. Create and start container
	container, err := docker.NewWithOptions(dockerClient, opts)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create ClickHouse container")
	}

	if err := container.Start(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "failed to start ClickHouse container")
	}

	// 3. Get DSN and connect to ClickHouse
	dsn, err := container.GetDSN(ctx)
	if err != nil {
		_ = container.Stop(ctx) // Clean up container on error
		return nil, nil, errors.Wrap(err, "failed to get container DSN")
	}

	// Create client with cluster and ignore databases configuration
	client, err := clickhouse.NewClientWithOptions(ctx, dsn, clickhouse.ClientOptions{
		Cluster:         cfg.ClickHouse.Cluster,
		IgnoreDatabases: cfg.ClickHouse.IgnoreDatabases,
	})
	if err != nil {
		_ = container.Stop(ctx) // Clean up container on error
		return nil, nil, errors.Wrap(err, "failed to create ClickHouse client")
	}

	// 4. Apply existing migrations
	if len(migrationDir.Migrations) > 0 {
		fmt.Fprintf(w, "Applying %d migrations...\n", len(migrationDir.Migrations))
		fmtr := format.New(format.Defaults)

		for _, migration := range migrationDir.Migrations {
			fmt.Fprintf(w, "Applying migration %s...\n", migration.Version)
			for _, stmt := range migration.Statements {
				// Skip comment-only statements as they cannot be executed
				if stmt.CommentStatement != nil {
					continue
				}

				// Format and execute the statement
				buf := new(bytes.Buffer)
				if err := fmtr.Format(buf, stmt); err != nil {
					_ = container.Stop(ctx)
					_ = client.Close()
					return nil, nil, errors.Wrap(err, "failed to format SQL statement")
				}

				if err := client.ExecuteMigration(ctx, buf.String()); err != nil {
					_ = container.Stop(ctx)
					_ = client.Close()
					return nil, nil, errors.Wrapf(err, "failed to execute statement: %s", buf.String())
				}
			}
		}
		fmt.Fprintln(w, "All migrations applied successfully")
	} else {
		fmt.Fprintln(w, "No migrations found to apply")
	}

	return container, client, nil
}

// compileProjectSchema compiles the project schema from the configured entrypoint
// and returns the parsed SQL statements. This is used by multiple commands that
// need to work with the compiled project schema (diff, schema compile, snapshot --bootstrap).
//
// Example usage:
//
//	statements, err := compileProjectSchema(cfg)
//	if err != nil {
//		return err
//	}
//
//	// Use statements for further processing
func compileProjectSchema(cfg *config.Config) ([]*parser.Statement, error) {
	// Compile project schema
	var schemaBuf bytes.Buffer
	if err := schemapkg.Compile(cfg.Entrypoint, &schemaBuf); err != nil {
		return nil, errors.Wrapf(err, "failed to compile project schema from: %s", cfg.Entrypoint)
	}

	// Parse compiled schema
	sql, err := parser.ParseString(schemaBuf.String())
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse compiled project schema")
	}

	// Inject ON CLUSTER if configured
	statements := sql.Statements
	if cfg.ClickHouse.Cluster != "" {
		statements = parser.InjectOnCluster(statements, cfg.ClickHouse.Cluster)
	}

	return statements, nil
}
