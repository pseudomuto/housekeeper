package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/executor"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

type migrateParams struct {
	fx.In

	Config    *config.Config
	Formatter *format.Formatter
	Project   *project.Project
	Version   *Version
}

// NewMigrateCommand creates the migrate command for applying pending migrations.
//
// The migrate command executes all pending migrations against the specified ClickHouse
// instance, updating the database schema to match the current migration state.
// It provides comprehensive progress reporting and error handling.
//
// Command flags:
//   - --dsn: ClickHouse connection string (required)
//   - --dry-run: Show what would be executed without applying changes
//   - --cluster: ClickHouse cluster name for distributed deployments
//
// Example usage:
//
//	# Apply all pending migrations
//	housekeeper migrate --dsn localhost:9000
//
//	# Show what would be executed without applying
//	housekeeper migrate --dsn localhost:9000 --dry-run
//
//	# Apply migrations with cluster support
//	housekeeper migrate --dsn localhost:9000 --cluster production_cluster
func NewMigrateCommand(p migrateParams) *cli.Command {
	return &cli.Command{
		Name:    "migrate",
		Aliases: []string{"apply"},
		Usage:   "Apply pending migrations to ClickHouse",
		Description: `Apply all pending migrations to the specified ClickHouse instance.

The migrate command executes migrations in chronological order, updating the database
schema to match the current migration state. Each migration is executed atomically -
if any statement fails, the migration is marked as failed and execution stops.

The command automatically handles:
- Bootstrap of housekeeper.revisions tracking table on first run
- Detection of already-applied migrations to avoid duplicate execution
- Comprehensive error reporting with statement-level details
- Progress tracking and execution timing
- Integration with cluster-aware ClickHouse deployments

Migration files are loaded from the db/migrations/ directory.
The command expects migration files to follow the standard naming
convention: yyyyMMddHHmmss_description.sql`,
		Before: requireConfig(p.Config),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:     "dsn",
				Usage:    "ClickHouse connection string (e.g., localhost:9000)",
				Required: true,
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
			&cli.BoolFlag{
				Name:  "dry-run",
				Usage: "Show what would be executed without applying changes",
				Value: false,
			},
			&cli.StringFlag{
				Name:  "cluster",
				Usage: "ClickHouse cluster name for distributed deployments",
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return runMigrate(ctx, cmd, p)
		},
	}
}

func runMigrate(ctx context.Context, cmd *cli.Command, p migrateParams) error {
	dsn := cmd.String("dsn")
	dryRun := cmd.Bool("dry-run")
	cluster := cmd.String("cluster")

	slog.Info("Starting migration execution",
		"dsn", dsn,
		"dry_run", dryRun,
		"cluster", cluster,
	)

	// Load migrations from the configured directory
	migrationDir, err := migrator.LoadMigrationDir(os.DirFS(p.Config.Dir))
	if err != nil {
		return errors.Wrap(err, "failed to load migrations")
	}

	migrations := migrationDir.Migrations
	if len(migrations) == 0 {
		fmt.Printf("No migrations found in %s\n", p.Config.Dir)
		return nil
	}

	slog.Info("Loaded migrations", "count", len(migrations))

	// Create ClickHouse client
	var client *clickhouse.Client
	if cluster != "" {
		client, err = clickhouse.NewClientWithOptions(ctx, dsn, clickhouse.ClientOptions{
			Cluster: cluster,
		})
	} else {
		client, err = clickhouse.NewClient(ctx, dsn)
	}
	if err != nil {
		return errors.Wrap(err, "failed to create ClickHouse client")
	}
	defer client.Close()

	// Test connection
	if err := testConnection(ctx, client); err != nil {
		return errors.Wrap(err, "failed to connect to ClickHouse")
	}

	slog.Info("Connected to ClickHouse successfully")

	if dryRun {
		return runDryRun(ctx, client, migrations, p)
	}

	// Create executor
	exec := executor.New(executor.Config{
		ClickHouse:         client,
		Formatter:          p.Formatter,
		HousekeeperVersion: p.Version.Version,
	})

	// Check if bootstrap is needed
	bootstrapped, err := exec.IsBootstrapped(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to check bootstrap status")
	}

	if !bootstrapped {
		fmt.Println("Initializing housekeeper migration tracking infrastructure...")
	}

	// Execute migrations
	results, err := exec.Execute(ctx, migrations)
	if err != nil {
		return errors.Wrap(err, "failed to execute migrations")
	}

	// Report results
	return reportResults(results)
}

func testConnection(ctx context.Context, client *clickhouse.Client) error {
	_, err := client.Query(ctx, "SELECT 1")
	if err != nil {
		return err
	}
	return nil
}

func runDryRun(ctx context.Context, client *clickhouse.Client, migrations []*migrator.Migration, p migrateParams) error {
	// Load existing revisions to determine what would be executed
	revisionSet, err := migrator.LoadRevisions(ctx, client)
	if err != nil {
		// If revisions table doesn't exist, treat as all pending
		slog.Warn("Could not load existing revisions (likely first run)", "error", err)
		revisionSet = migrator.NewRevisionSet([]*migrator.Revision{})
	}

	fmt.Println("Dry run: showing migrations that would be executed")
	fmt.Println()

	pendingCount := 0
	skippedCount := 0

	for _, migration := range migrations {
		if revisionSet.IsCompleted(migration) {
			fmt.Printf("  ⏭  %s (already applied)\n", migration.Version)
			skippedCount++
		} else {
			fmt.Printf("  ▶  %s (%d statements)\n", migration.Version, len(migration.Statements))
			pendingCount++

			// Show first few statements for preview
			for i, stmt := range migration.Statements {
				if i >= 3 { // Show max 3 statements
					fmt.Printf("     ... and %d more statements\n", len(migration.Statements)-3)
					break
				}

				stmtSQL, err := formatStatement(p.Formatter, stmt)
				if err != nil {
					return errors.Wrapf(err, "failed to format statement %d in migration %s", i+1, migration.Version)
				}

				// Truncate long statements
				if len(stmtSQL) > 80 {
					stmtSQL = stmtSQL[:77] + "..."
				}
				fmt.Printf("     %s\n", stmtSQL)
			}
		}
	}

	fmt.Println()
	fmt.Printf("Summary: %d migrations would be executed, %d already applied\n",
		pendingCount, skippedCount)

	if pendingCount == 0 {
		fmt.Println("All migrations are up to date.")
	}

	return nil
}

func reportResults(results []*executor.ExecutionResult) error {
	fmt.Println()
	fmt.Println("Migration execution results:")
	fmt.Println()

	var (
		successCount int
		failedCount  int
		skippedCount int
		lastError    error
	)

	for _, result := range results {
		switch result.Status {
		case executor.StatusSuccess:
			fmt.Printf("  ✅ %s completed in %v (%d/%d statements)\n",
				result.Version,
				result.ExecutionTime,
				result.StatementsApplied,
				result.TotalStatements,
			)
			successCount++

		case executor.StatusFailed:
			fmt.Printf("  ❌ %s failed after %v (%d/%d statements)\n",
				result.Version,
				result.ExecutionTime,
				result.StatementsApplied,
				result.TotalStatements,
			)
			if result.Error != nil {
				fmt.Printf("     Error: %v\n", result.Error)
				lastError = result.Error
			}
			failedCount++

		case executor.StatusSkipped:
			fmt.Printf("  ⏭  %s (already applied)\n", result.Version)
			skippedCount++
		}
	}

	fmt.Println()
	fmt.Printf("Summary: %d successful, %d failed, %d skipped\n",
		successCount, failedCount, skippedCount)

	if failedCount > 0 {
		fmt.Println()
		fmt.Println("❌ Migration execution failed. Please review the errors above.")
		fmt.Println("   Failed migrations can be retried after fixing the issues.")
		return lastError
	}

	if successCount > 0 {
		fmt.Println()
		fmt.Println("✅ All migrations executed successfully.")
	} else if skippedCount > 0 {
		fmt.Println()
		fmt.Println("ℹ️  All migrations are up to date.")
	}

	return nil
}

// formatStatement formats a single statement using the formatter.
func formatStatement(formatter *format.Formatter, stmt *parser.Statement) (string, error) {
	var buf strings.Builder
	if err := formatter.Format(&buf, stmt); err != nil {
		return "", err
	}
	return buf.String(), nil
}

func init() {
	fx.Provide(NewMigrateCommand)
}
