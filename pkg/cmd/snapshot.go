package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// snapshot creates a CLI command for consolidating existing migrations into a single snapshot file.
//
// The command consolidates all existing migration files into a single timestamped snapshot
// file and removes the original migration files. This helps reduce the number of migration
// files while preserving the complete schema definition.
//
// The snapshot process:
// 1. Loads all existing migrations from the migrations directory
// 2. Creates a timestamped snapshot file containing all migration content
// 3. Removes the individual migration files that were consolidated
// 4. Updates the migration sum file accordingly
//
// Example usage:
//
//	# Create snapshot with default description
//	housekeeper snapshot
//
//	# Create snapshot with custom description
//	housekeeper snapshot --description "Initial schema baseline"
func snapshot(p *project.Project) *cli.Command {
	return &cli.Command{
		Name:  "snapshot",
		Usage: "Create a snapshot from existing migrations",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "description",
				Aliases: []string{"d"},
				Usage:   "Description for the snapshot",
				Value:   "Schema snapshot",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			migrationsDir := p.MigrationsDir()
			dir, err := migrator.LoadMigrationDir(os.DirFS(migrationsDir))
			if err != nil {
				return err
			}

			// Check if there are migrations to snapshot
			if len(dir.Migrations) == 0 {
				return cli.Exit("No migrations found to create snapshot from", 1)
			}

			// Generate snapshot version and get description
			version := time.Now().UTC().Format("20060102150405") + "_snapshot"
			description := cmd.String("description")

			fmt.Fprintf(cmd.Writer, "Creating snapshot %s with %d migrations...\n", version, len(dir.Migrations))

			// Create the snapshot
			cp, err := dir.CreateSnapshot(version, description)
			if err != nil {
				return fmt.Errorf("failed to create snapshot: %w", err)
			}

			// Write the snapshot file to the migrations directory
			snapshotPath := filepath.Join(migrationsDir, version+".sql")
			snapshotFile, err := os.Create(snapshotPath)
			if err != nil {
				return fmt.Errorf("failed to create snapshot file: %w", err)
			}
			defer snapshotFile.Close()

			if _, err := cp.WriteTo(snapshotFile); err != nil {
				return fmt.Errorf("failed to write snapshot content: %w", err)
			}

			fmt.Fprintf(cmd.Writer, "✓ Snapshot file created: %s\n", snapshotPath)

			// Remove the migration files that are included in the snapshot
			for _, migVersion := range cp.IncludedMigrations {
				migPath := filepath.Join(migrationsDir, migVersion+".sql")
				if err := os.Remove(migPath); err != nil {
					// Log warning but continue - file might already be removed or not exist
					fmt.Fprintf(cmd.Writer, "Warning: Could not remove migration file %s: %v\n", migPath, err)
				} else {
					fmt.Fprintf(cmd.Writer, "✓ Removed migration file: %s\n", migPath)
				}
			}

			fmt.Fprintf(cmd.Writer, "\nSnapshot created successfully!\n")
			fmt.Fprintf(cmd.Writer, "- Consolidated %d migrations into snapshot %s\n", len(cp.IncludedMigrations), version)
			fmt.Fprintf(cmd.Writer, "- Description: %s\n", description)

			return nil
		},
	}
}
