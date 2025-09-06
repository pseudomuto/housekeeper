package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	schemapkg "github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/urfave/cli/v3"
)

// diff creates a CLI command for generating schema migration files by comparing
// the current database state with the target schema definition.
func diff(cfg *config.Config, client docker.DockerClient) *cli.Command {
	return &cli.Command{
		Name:   "diff",
		Usage:  "Generate any missing migrations",
		Before: requireConfig(cfg),
		Action: func(ctx context.Context, cmd *cli.Command) error {
			// 1. Start container, run migrations, get client
			container, client, err := runContainer(ctx, cmd.Writer, docker.DockerOptions{
				Version:   cfg.ClickHouse.Version,
				ConfigDir: cfg.ClickHouse.ConfigDir,
				Name:      "housekeeper-diff",
			}, cfg, client)
			if err != nil {
				return err
			}
			defer func() {
				_ = client.Close()
				if stopErr := container.Stop(ctx); stopErr != nil {
					fmt.Fprintf(cmd.ErrWriter, "Warning: failed to stop container: %v\n", stopErr)
				}
			}()

			// 2. Load project schema and generate diff
			return generateDiff(ctx, cmd.Writer, client, cfg)
		},
	}
}

// generateDiff compares the current database schema with the target schema
// and generates a migration file if differences are found.
func generateDiff(ctx context.Context, w io.Writer, client *clickhouse.Client, cfg *config.Config) error {
	// NB: Migrations have already been applied by runContainer
	// Get current and target schemas
	currentSchema, err := client.GetSchema(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to dump current schema")
	}

	targetStatements, err := compileProjectSchema(cfg)
	if err != nil {
		return err
	}

	targetSchema := &parser.SQL{Statements: targetStatements}

	// Check if there are differences
	_, err = schemapkg.GenerateDiff(currentSchema, targetSchema)
	if err != nil {
		if errors.Is(err, schemapkg.ErrNoDiff) {
			fmt.Fprintln(w, "No differences found between current and target schemas")
			return nil // No changes needed
		}
		return errors.Wrap(err, "failed to generate schema diff")
	}

	// Generate migration file using normalized schemas for consistent output
	filename, err := schemapkg.GenerateMigrationFile(cfg.Dir, currentSchema, targetSchema)
	if err != nil {
		return errors.Wrap(err, "failed to generate migration file")
	}

	// Reload and rehash migration directory to include the new migration
	migrationDir, err := migrator.LoadMigrationDir(os.DirFS(cfg.Dir))
	if err != nil {
		return errors.Wrap(err, "failed to reload migration directory")
	}

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

	fmt.Fprintf(w, "Generated migration: %s\n", filename)
	fmt.Fprintf(w, "Updated sum file: housekeeper.sum\n")
	return nil
}
