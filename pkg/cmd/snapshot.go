package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// snapshot creates a CLI command for creating snapshots from migrations or project schema.
//
// The command supports two modes:
//
// Normal Mode (default): Consolidates existing migration files into a single timestamped
// snapshot file and removes the original migration files. This helps reduce the number of
// migration files while preserving the complete schema definition.
//
// Bootstrap Mode (--bootstrap): Creates a snapshot from the compiled project schema instead
// of existing migrations. This solves the chicken-and-egg problem when bootstrapping an
// existing database where no migrations exist yet.
//
// The snapshot process:
// 1. Normal: Loads existing migrations OR Bootstrap: Compiles project schema
// 2. Creates a timestamped snapshot file containing the schema content
// 3. Normal: Removes individual migration files OR Bootstrap: No files removed
// 4. Updates the migration sum file accordingly
//
// Example usage:
//
//	# Create snapshot from existing migrations
//	housekeeper snapshot --description "Consolidate migrations"
//
//	# Create bootstrap snapshot from project schema (no migrations required)
//	housekeeper snapshot --bootstrap --description "Initial database baseline"
func snapshot(p *project.Project, cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:  "snapshot",
		Usage: "Create a snapshot from migrations or project schema",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:  "description",
				Usage: "Description for the snapshot",
				Value: "Schema snapshot",
			},
			&cli.BoolFlag{
				Name:  "bootstrap",
				Usage: "Create snapshot from project schema instead of existing migrations",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			isBootstrap := cmd.Bool("bootstrap")
			migrationsDir := p.MigrationsDir()
			version := time.Now().UTC().Format("20060102150405") + "_snapshot"
			description := cmd.String("description")

			var cp *migrator.Snapshot
			var err error

			if isBootstrap {
				cp, err = createBootstrapSnapshot(cmd.Writer, cfg, version, description)
			} else {
				cp, err = createMigrationSnapshot(cmd.Writer, migrationsDir, version, description)
			}
			if err != nil {
				return err
			}

			// Write snapshot to disk and update migration directory
			if err := writeSnapshotAndUpdateMigrations(cmd.Writer, migrationsDir, version, cp, isBootstrap); err != nil {
				return err
			}

			fmt.Fprintf(cmd.Writer, "\nSnapshot created successfully!\n")
			if isBootstrap {
				fmt.Fprintf(cmd.Writer, "- Created bootstrap snapshot %s from project schema\n", version)
			} else {
				fmt.Fprintf(cmd.Writer, "- Consolidated %d migrations into snapshot %s\n", len(cp.IncludedMigrations), version)
			}
			fmt.Fprintf(cmd.Writer, "- Description: %s\n", description)

			return nil
		},
	}
}

// createBootstrapSnapshot creates a snapshot from the compiled project schema
func createBootstrapSnapshot(w io.Writer, cfg *config.Config, version, description string) (*migrator.Snapshot, error) {
	fmt.Fprintf(w, "Creating bootstrap snapshot %s from project schema...\n", version)

	// Compile project schema using shared utility
	statements, err := compileProjectSchema(cfg)
	if err != nil {
		return nil, err
	}

	// Create snapshot from schema statements
	return &migrator.Snapshot{
		Version:            version,
		Description:        description,
		CreatedAt:          time.Now().UTC(),
		IncludedMigrations: []string{}, // No migrations included in bootstrap
		CumulativeHash:     "",         // Will be computed during WriteTo
		Statements:         statements,
	}, nil
}

// createMigrationSnapshot creates a snapshot from existing migrations
func createMigrationSnapshot(w io.Writer, migrationsDir, version, description string) (*migrator.Snapshot, error) {
	// Load existing migrations
	dir, err := migrator.LoadMigrationDir(os.DirFS(migrationsDir))
	if err != nil {
		return nil, err
	}

	// Check if there are migrations to snapshot
	if len(dir.Migrations) == 0 {
		return nil, cli.Exit("No migrations found to create snapshot from", 1)
	}

	fmt.Fprintf(w, "Creating snapshot %s with %d migrations...\n", version, len(dir.Migrations))

	// Create the snapshot
	cp, err := dir.CreateSnapshot(version, description)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create snapshot")
	}

	return cp, nil
}

// writeSnapshotAndUpdateMigrations writes the snapshot to disk, removes old migration files
// (if not bootstrap mode), and updates the migration directory hash
func writeSnapshotAndUpdateMigrations(w io.Writer, migrationsDir, version string, snapshot *migrator.Snapshot, isBootstrap bool) error {
	// Write the snapshot file to the migrations directory
	snapshotPath := filepath.Join(migrationsDir, version+".sql")
	snapshotFile, err := os.Create(snapshotPath)
	if err != nil {
		return errors.Wrapf(err, "failed to create snapshot file")
	}
	defer snapshotFile.Close()

	if _, err := snapshot.WriteTo(snapshotFile); err != nil {
		return errors.Wrapf(err, "failed to write snapshot content")
	}

	fmt.Fprintf(w, "✓ Snapshot file created: %s\n", snapshotPath)

	// Remove the migration files that are included in the snapshot (only for normal mode)
	if !isBootstrap {
		for _, migVersion := range snapshot.IncludedMigrations {
			migPath := filepath.Join(migrationsDir, migVersion+".sql")
			if err := os.Remove(migPath); err != nil {
				// Log warning but continue - file might already be removed or not exist
				fmt.Fprintf(w, "Warning: Could not remove migration file %s: %v\n", migPath, err)
			} else {
				fmt.Fprintf(w, "✓ Removed migration file: %s\n", migPath)
			}
		}
	}

	// Load/reload migration directory and rehash
	dir, err := migrator.LoadMigrationDir(os.DirFS(migrationsDir))
	if err != nil {
		return errors.Wrap(err, "failed to reload migration directory")
	}

	if err := dir.Rehash(); err != nil {
		return errors.Wrap(err, "failed to compute new migration hash")
	}

	sf, err := os.Create(filepath.Join(migrationsDir, "housekeeper.sum"))
	if err != nil {
		return errors.Wrapf(err, "failed to create sum file")
	}
	defer func() { _ = sf.Close() }()

	if _, err := dir.SumFile.WriteTo(sf); err != nil {
		return errors.Wrapf(err, "failed to write sum file")
	}

	return nil
}
