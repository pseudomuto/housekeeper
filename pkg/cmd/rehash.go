package cmd

import (
	"context"
	"fmt"
	"os"
	"path/filepath"

	F "github.com/IBM/fp-go/v2/function"
	O "github.com/IBM/fp-go/v2/option"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
)

// resolveMigrationsDir picks the migrations directory honouring the user's
// housekeeper.yaml config (cfg.Dir) when set, falling back to the project's
// default db/migrations layout otherwise. Relative paths are resolved against
// the project root so the command works regardless of the caller's CWD.
func resolveMigrationsDir(p *project.Project, cfg *config.Config) string {
	resolve := func(d string) string {
		if filepath.IsAbs(d) {
			return d
		}
		return filepath.Join(p.Dir(), d)
	}
	return F.Pipe3(
		O.FromNillable(cfg),
		O.Map(func(c *config.Config) string { return c.Dir }),
		O.Chain(O.FromPredicate(func(s string) bool { return s != "" })),
		O.Fold(p.MigrationsDir, resolve),
	)
}

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
func rehash(p *project.Project, cfg *config.Config) *cli.Command {
	return &cli.Command{
		Name:  "rehash",
		Usage: "Regenerate the sum file for all migrations",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			migrationsDir := resolveMigrationsDir(p, cfg)

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
func TestableRehash(p *project.Project, cfg *config.Config) *cli.Command {
	return rehash(p, cfg)
}
