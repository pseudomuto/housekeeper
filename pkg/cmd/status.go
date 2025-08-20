package cmd

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/urfave/cli/v3"
	"go.uber.org/fx"
)

type statusParams struct {
	fx.In

	Config  *config.Config
	Project *project.Project
}

// NewStatusCommand creates the status command for showing migration status.
//
// The status command displays comprehensive information about the current
// migration state, including which migrations have been applied, which are
// pending, and any that have failed.
//
// Command flags:
//   - --dsn: ClickHouse connection string (required)
//   - --env: Environment name for migration directory (default: "migrations")
//   - --cluster: ClickHouse cluster name for distributed deployments
//   - --verbose: Show detailed migration information
//
// Example usage:
//
//	# Show basic migration status
//	housekeeper status --dsn localhost:9000
//
//	# Show detailed information about each migration
//	housekeeper status --dsn localhost:9000 --verbose
//
//	# Show status with cluster support
//	housekeeper status --dsn localhost:9000 --cluster production_cluster
func NewStatusCommand(p statusParams) *cli.Command {
	return &cli.Command{
		Name:  "status",
		Usage: "Show migration status",
		Description: `Display the current migration status for the specified ClickHouse instance.

The status command shows:
- Total number of migration files found
- Number of completed, pending, and failed migrations
- Execution history with timing information (when --verbose is used)
- Last migration execution details
- Bootstrap status of housekeeper infrastructure

This command is useful for:
- Checking if migrations need to be applied
- Debugging failed migrations
- Auditing migration execution history
- Verifying the state of your database schema`,
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
			&cli.StringFlag{
				Name:  "cluster",
				Usage: "ClickHouse cluster name for distributed deployments",
				Config: cli.StringConfig{
					TrimSpace: true,
				},
			},
			&cli.BoolFlag{
				Name:  "verbose",
				Usage: "Show detailed migration information",
				Value: false,
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return runStatus(ctx, cmd, p)
		},
	}
}

func runStatus(ctx context.Context, cmd *cli.Command, p statusParams) error {
	dsn := cmd.String("dsn")
	cluster := cmd.String("cluster")
	verbose := cmd.Bool("verbose")

	slog.Info("Checking migration status",
		"dsn", dsn,
		"cluster", cluster,
	)

	// Load and validate migrations
	migrations, err := loadAndValidateMigrations(p.Config.Dir)
	if err != nil {
		return err
	}

	if len(migrations) == 0 {
		fmt.Println("No migration files found.")
		return nil
	}

	// Setup ClickHouse connection
	client, err := setupClickHouseClient(ctx, dsn, cluster)
	if err != nil {
		return err
	}
	defer client.Close()

	// Check bootstrap status
	bootstrapped, err := checkBootstrapStatus(ctx, client)
	if err != nil {
		return errors.Wrap(err, "failed to check bootstrap status")
	}

	if !bootstrapped {
		showUnbootstrappedStatus(migrations)
		return nil
	}

	// Display status with revisions
	return displayStatusWithRevisions(ctx, client, migrations, verbose)
}

func loadAndValidateMigrations(dir string) ([]*migrator.Migration, error) {
	migrationDir, err := migrator.LoadMigrationDir(os.DirFS(dir))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load migrations")
	}

	fmt.Printf("Migration Status\n")
	fmt.Printf("Migration directory: %s\n", dir)
	fmt.Println()

	return migrationDir.Migrations, nil
}

func setupClickHouseClient(ctx context.Context, dsn, cluster string) (*clickhouse.Client, error) {
	client, err := clickhouse.NewClientWithOptions(ctx, dsn, clickhouse.ClientOptions{
		Cluster: cluster,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ClickHouse client")
	}

	if err := testConnection(ctx, client); err != nil {
		client.Close()
		return nil, errors.Wrap(err, "failed to connect to ClickHouse")
	}

	return client, nil
}

func showUnbootstrappedStatus(migrations []*migrator.Migration) {
	fmt.Println("❗ Housekeeper infrastructure not initialized")
	fmt.Println("   Run 'housekeeper migrate --dsn <dsn>' to initialize and apply migrations")
	fmt.Println()
	fmt.Printf("Found %d migration files:\n", len(migrations))
	for _, migration := range migrations {
		fmt.Printf("  📄 %s (%d statements)\n", migration.Version, len(migration.Statements))
	}
}

func displayStatusWithRevisions(ctx context.Context, client *clickhouse.Client, migrations []*migrator.Migration, verbose bool) error {
	revisionSet, err := migrator.LoadRevisions(ctx, client)
	if err != nil {
		return errors.Wrap(err, "failed to load revisions")
	}

	migrationDir := &migrator.MigrationDir{Migrations: migrations}
	completed := revisionSet.GetCompleted(migrationDir)
	pending := revisionSet.GetPending(migrationDir)
	failed := revisionSet.GetFailed(migrationDir)

	showStatusSummary(completed, pending, failed, migrations)
	showLastMigration(completed, revisionSet)
	showFailedMigrations(failed, revisionSet)
	showPendingMigrations(pending)

	if verbose {
		showVerboseStatus(migrations, revisionSet)
	}

	showRecommendations(pending, failed)
	return nil
}

func showStatusSummary(completed, pending, failed, migrations []*migrator.Migration) {
	fmt.Printf("Total migrations: %d\n", len(migrations))
	fmt.Printf("✅ Completed: %d\n", len(completed))
	fmt.Printf("⏳ Pending: %d\n", len(pending))
	fmt.Printf("❌ Failed: %d\n", len(failed))
	fmt.Println()
}

func showLastMigration(completed []*migrator.Migration, revisionSet *migrator.RevisionSet) {
	if len(completed) > 0 {
		lastCompleted := completed[len(completed)-1]
		lastRevision := revisionSet.GetRevision(lastCompleted)
		fmt.Printf("Last applied: %s at %s\n",
			lastCompleted.Version,
			lastRevision.ExecutedAt.Format("2006-01-02 15:04:05 UTC"))
		fmt.Println()
	}
}

func showFailedMigrations(failed []*migrator.Migration, revisionSet *migrator.RevisionSet) {
	if len(failed) > 0 {
		fmt.Println("❌ Failed migrations:")
		for _, migration := range failed {
			revision := revisionSet.GetRevision(migration)
			fmt.Printf("  %s (failed at %s)\n",
				migration.Version,
				revision.ExecutedAt.Format("2006-01-02 15:04:05"))
			if revision.Error != nil {
				fmt.Printf("    Error: %s\n", *revision.Error)
			}
		}
		fmt.Println()
	}
}

func showPendingMigrations(pending []*migrator.Migration) {
	if len(pending) > 0 {
		fmt.Println("⏳ Pending migrations:")
		for _, migration := range pending {
			fmt.Printf("  %s (%d statements)\n", migration.Version, len(migration.Statements))
		}
		fmt.Println()
	}
}

func showRecommendations(pending, failed []*migrator.Migration) {
	if len(pending) > 0 {
		fmt.Println("💡 Run 'housekeeper migrate --dsn <dsn>' to apply pending migrations")
	} else if len(failed) > 0 {
		fmt.Println("💡 Fix failed migrations and run 'housekeeper migrate --dsn <dsn>' to retry")
	} else {
		fmt.Println("✅ All migrations are up to date")
	}
}

func checkBootstrapStatus(ctx context.Context, client *clickhouse.Client) (bool, error) {
	// Check if housekeeper database exists
	rows, err := client.Query(ctx, "SELECT 1 FROM system.databases WHERE name = 'housekeeper'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	if !rows.Next() {
		return false, nil
	}

	// Check if revisions table exists
	rows, err = client.Query(ctx, "SELECT 1 FROM system.tables WHERE database = 'housekeeper' AND name = 'revisions'")
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), nil
}

func showVerboseStatus(migrations []*migrator.Migration, revisionSet *migrator.RevisionSet) {
	fmt.Println("📊 Detailed migration history:")
	fmt.Println()

	for _, migration := range migrations {
		revision := revisionSet.GetRevision(migration)

		if revision == nil {
			fmt.Printf("  📄 %s - Not executed\n", migration.Version)
			continue
		}

		status := "✅"
		statusText := "Completed"

		if revision.Error != nil {
			status = "❌"
			statusText = "Failed"
		}

		fmt.Printf("  %s %s - %s\n", status, migration.Version, statusText)
		fmt.Printf("     Executed: %s\n", revision.ExecutedAt.Format("2006-01-02 15:04:05 UTC"))
		fmt.Printf("     Duration: %v\n", revision.ExecutionTime)
		fmt.Printf("     Statements: %d/%d applied\n", revision.Applied, revision.Total)
		fmt.Printf("     Housekeeper version: %s\n", revision.HousekeeperVersion)

		if revision.Error != nil {
			fmt.Printf("     Error: %s\n", *revision.Error)
		}

		fmt.Println()
	}

	// Show snapshot information if any
	if revisionSet.HasSnapshot() {
		lastSnapshot := revisionSet.GetLastSnapshot()
		fmt.Printf("📸 Last snapshot: %s at %s\n",
			lastSnapshot.Version,
			lastSnapshot.ExecutedAt.Format("2006-01-02 15:04:05 UTC"))

		migrationsAfterSnapshot := revisionSet.GetMigrationsAfterSnapshot()
		fmt.Printf("   Migrations since snapshot: %d\n", len(migrationsAfterSnapshot))
		fmt.Println()
	}
}

func init() {
	fx.Provide(NewStatusCommand)
}
