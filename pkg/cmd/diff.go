package cmd

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	schemapkg "github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/urfave/cli/v3"
)

func diff(cfg *config.Config, formatter *format.Formatter) *cli.Command {
	return &cli.Command{
		Name:   "diff",
		Usage:  "Generate any missing migrations",
		Before: requireConfig(cfg),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return runDiff(ctx, cfg, formatter)
		},
	}
}

func runDiff(ctx context.Context, cfg *config.Config, formatter *format.Formatter) error {
	// 1. Load and validate migrations before creating container
	migrationDir, err := loadAndValidateMigrations(cfg.Dir)
	if err != nil {
		return err
	}

	// 2. Set up container and client
	client, cleanup, err := setupClickHouseContainer(ctx, cfg)
	if err != nil {
		return err
	}
	defer cleanup()

	// 3. Execute migrations and generate diff
	return executeMigrationsAndGenerateDiff(ctx, client, migrationDir, cfg, formatter)
}

func loadAndValidateMigrations(migrationsPath string) (*migrator.MigrationDir, error) {
	migrationDir, err := migrator.LoadMigrationDir(os.DirFS(migrationsPath))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load migration directory")
	}

	isValid, err := migrationDir.Validate()
	if err != nil {
		return nil, errors.Wrap(err, "failed to validate migrations")
	}
	if !isValid {
		return nil, errors.New("migration directory failed validation - files have been modified")
	}

	return migrationDir, nil
}

func setupClickHouseContainer(ctx context.Context, cfg *config.Config) (*clickhouse.Client, func(), error) {
	container, err := docker.NewWithOptions(docker.DockerOptions{
		Version:   cfg.ClickHouse.Version,
		ConfigDir: cfg.ClickHouse.ConfigDir,
	})
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to create ClickHouse container")
	}

	if err := container.Start(ctx); err != nil {
		return nil, nil, errors.Wrap(err, "failed to start ClickHouse container")
	}

	cleanup := func() {
		if stopErr := container.Stop(ctx); stopErr != nil {
			fmt.Printf("Warning: failed to stop container: %v\n", stopErr)
		}
	}

	dsn, err := container.GetDSN(ctx)
	if err != nil {
		cleanup()
		return nil, nil, errors.Wrap(err, "failed to get container DSN")
	}

	client, err := clickhouse.NewClient(ctx, dsn)
	if err != nil {
		cleanup()
		return nil, nil, errors.Wrap(err, "failed to create ClickHouse client")
	}

	fullCleanup := func() {
		client.Close()
		cleanup()
	}

	return client, fullCleanup, nil
}

func executeMigrationsAndGenerateDiff(ctx context.Context, client *clickhouse.Client, migrationDir *migrator.MigrationDir, cfg *config.Config, formatter *format.Formatter) error {
	// Execute migrations statement by statement
	for _, migration := range migrationDir.Migrations {
		for i, stmt := range migration.Statements {
			// Format the statement as SQL
			var stmtBuf bytes.Buffer
			if err := formatter.Format(&stmtBuf, stmt); err != nil {
				return errors.Wrapf(err, "failed to format statement %d in migration %s", i+1, migration.Version)
			}

			stmtSQL := stmtBuf.String()
			if err := client.ExecuteMigration(ctx, stmtSQL); err != nil {
				return errors.Wrapf(err, "failed to execute statement %d in migration %s: %s", i+1, migration.Version, stmtSQL)
			}
		}
	}

	// Get current and target schemas
	currentSchema, err := client.GetSchema(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to dump current schema")
	}

	var targetBuf bytes.Buffer
	if err := schemapkg.Compile(cfg.Entrypoint, &targetBuf); err != nil {
		return errors.Wrap(err, "failed to compile target schema")
	}

	targetSchema, err := parser.ParseString(targetBuf.String())
	if err != nil {
		return errors.Wrap(err, "failed to parse target schema")
	}

	// Check if there are differences
	_, err = schemapkg.GenerateDiff(currentSchema, targetSchema)
	if err != nil {
		if errors.Is(err, schemapkg.ErrNoDiff) {
			return nil // No changes needed
		}
		return errors.Wrap(err, "failed to generate schema diff")
	}

	// Generate migration file
	filename, err := schemapkg.GenerateMigrationFile(cfg.Dir, currentSchema, targetSchema)
	if err != nil {
		return errors.Wrap(err, "failed to generate migration file")
	}

	// Rehash migration directory to include the new migration
	if err := migrationDir.Rehash(); err != nil {
		return errors.Wrap(err, "failed to rehash migration directory")
	}

	// Write the updated sum file
	sumFilePath := filepath.Join(cfg.Dir, "housekeeper.sum")
	sumFile, err := os.Create(sumFilePath)
	if err != nil {
		return errors.Wrapf(err, "failed to create sum file: %s", sumFilePath)
	}
	defer sumFile.Close()

	if _, err := migrationDir.SumFile.WriteTo(sumFile); err != nil {
		return errors.Wrap(err, "failed to write sum file")
	}

	fmt.Printf("Generated migration: %s\n", filename)
	fmt.Printf("Updated sum file: housekeeper.sum\n")
	return nil
}
