package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// rehash creates a CLI command for regenerating the sum file for all migrations.
//
// The command loads all migration files from the migrations directory and recalculates
// their SHA256 hashes, updating the sum file with the current state. This is useful for:
//   - Verifying migration file integrity after potential modifications
//   - Regenerating the sum file after adding or modifying migrations
//   - Detecting unauthorized changes to migration files
//
// The rehash process:
// 1. Loads all existing migrations from the migrations directory
// 2. Recalculates SHA256 hashes for each migration file
// 3. Generates a new sum file with updated integrity verification data
// 4. Writes the updated sum file to disk
//
// Example usage:
//
//	# Regenerate sum file for all migrations
//	housekeeper rehash
//
// The command will output the status of the rehashing operation and indicate
// how many migration files were processed.
func rehash(p *project.Project) *cli.Command {
	return &cli.Command{
		Name:  "rehash",
		Usage: "Regenerate the sum file for all migrations",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			migrationsDir := p.MigrationsDir()

			// Check if migrations directory exists
			if _, err := os.Stat(migrationsDir); os.IsNotExist(err) {
				return errors.Errorf("migrations directory does not exist: %s", migrationsDir)
			}

			// Load migration directory
			migrationDir, err := migrator.LoadMigrationDir(os.DirFS(migrationsDir))
			if err != nil {
				return errors.Wrap(err, "failed to load migration directory")
			}

			// Rehash all migrations
			if err := migrationDir.Rehash(); err != nil {
				return errors.Wrap(err, "failed to rehash migrations")
			}

			// Write the updated sum file
			sumFilePath := filepath.Join(migrationsDir, "housekeeper.sum")
			sumFile, err := os.Create(sumFilePath)
			if err != nil {
				return errors.Wrapf(err, "failed to create sum file: %s", sumFilePath)
			}
			defer sumFile.Close()

			_, err = migrationDir.SumFile.WriteTo(sumFile)
			if err != nil {
				return errors.Wrap(err, "failed to write sum file")
			}

			// Set appropriate file permissions
			if err := os.Chmod(sumFilePath, consts.ModeFile); err != nil {
				return errors.Wrapf(err, "failed to set permissions on sum file: %s", sumFilePath)
			}

			// Output success message
			migrationCount := len(migrationDir.Migrations)
			fmt.Fprintf(cmd.Writer, "Successfully rehashed %d migration(s) and updated sum file\n", migrationCount)

			return nil
		},
	}
}

// TestableRehash creates a testable version of the rehash command for use in unit tests.
// This function exposes the same functionality as the main rehash command but allows
// for easier testing by accepting a project parameter directly.
func TestableRehash(p *project.Project) *cli.Command {
	return rehash(p)
}
