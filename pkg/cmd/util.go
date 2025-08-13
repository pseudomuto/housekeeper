package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
)

// runContainer starts a ClickHouse container with the given options, loads and executes
// existing migrations, and returns the container and client for further use.
func runContainer(ctx context.Context, opts docker.DockerOptions, cfg *config.Config, dockerClient docker.DockerClient) (*docker.ClickHouseContainer, *clickhouse.Client, error) {
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

	client, err := clickhouse.NewClient(ctx, dsn)
	if err != nil {
		_ = container.Stop(ctx) // Clean up container on error
		return nil, nil, errors.Wrap(err, "failed to create ClickHouse client")
	}

	// 4. Apply existing migrations
	if len(migrationDir.Migrations) > 0 {
		fmt.Printf("Applying %d migrations...\n", len(migrationDir.Migrations))
		fmtr := format.New(format.Defaults)

		for _, migration := range migrationDir.Migrations {
			fmt.Printf("Applying migration %s...\n", migration.Version)
			for _, stmt := range migration.Statements {
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
		fmt.Println("All migrations applied successfully")
	} else {
		fmt.Println("No migrations found to apply")
	}

	return container, client, nil
}
