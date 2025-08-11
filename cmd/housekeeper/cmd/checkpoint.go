package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/urfave/cli/v3"
)

func checkpoint() *cli.Command {
	return &cli.Command{
		Name:   "checkpoint",
		Usage:  "Create a checkpoint from existing migrations",
		Before: requireProject,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "description",
				Aliases: []string{"d"},
				Usage:   "Description for the checkpoint",
				Value:   "Schema checkpoint",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			migrationsDir := currentProject.MigrationsDir()
			dir, err := migrator.LoadMigrationDir(os.DirFS(migrationsDir))
			if err != nil {
				return err
			}

			// Check if there are migrations to checkpoint
			if len(dir.Migrations) == 0 {
				return cli.Exit("No migrations found to create checkpoint from", 1)
			}

			// Generate checkpoint version and get description
			version := time.Now().UTC().Format("20060102150405") + "_checkpoint"
			description := cmd.String("description")

			fmt.Fprintf(cmd.Writer, "Creating checkpoint %s with %d migrations...\n", version, len(dir.Migrations))

			// Create the checkpoint
			cp, err := dir.CreateCheckpoint(version, description)
			if err != nil {
				return fmt.Errorf("failed to create checkpoint: %w", err)
			}

			// Write the checkpoint file to the migrations directory
			checkpointPath := filepath.Join(migrationsDir, version+".sql")
			checkpointFile, err := os.Create(checkpointPath)
			if err != nil {
				return fmt.Errorf("failed to create checkpoint file: %w", err)
			}
			defer checkpointFile.Close()

			if _, err := cp.WriteTo(checkpointFile); err != nil {
				return fmt.Errorf("failed to write checkpoint content: %w", err)
			}

			fmt.Fprintf(cmd.Writer, "✓ Checkpoint file created: %s\n", checkpointPath)

			// Remove the migration files that are included in the checkpoint
			for _, migVersion := range cp.IncludedMigrations {
				migPath := filepath.Join(migrationsDir, migVersion+".sql")
				if err := os.Remove(migPath); err != nil {
					// Log warning but continue - file might already be removed or not exist
					fmt.Fprintf(cmd.Writer, "Warning: Could not remove migration file %s: %v\n", migPath, err)
				} else {
					fmt.Fprintf(cmd.Writer, "✓ Removed migration file: %s\n", migPath)
				}
			}

			fmt.Fprintf(cmd.Writer, "\nCheckpoint created successfully!\n")
			fmt.Fprintf(cmd.Writer, "- Consolidated %d migrations into checkpoint %s\n", len(cp.IncludedMigrations), version)
			fmt.Fprintf(cmd.Writer, "- Description: %s\n", description)

			return nil
		},
	}
}
