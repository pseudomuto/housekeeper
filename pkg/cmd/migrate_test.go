package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/stretchr/testify/require"
)

func TestMigrateCommand_Integration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ctx := context.Background()

	// Start real ClickHouse container for integration testing
	_, dsn := testutil.StartClickHouseContainer(t, "")

	// Create temporary project directory
	projectDir := t.TempDir()

	// Create project structure
	require.NoError(t, os.MkdirAll(filepath.Join(projectDir, "db", "migrations"), consts.ModeDir))

	// Create housekeeper.yaml
	config := `clickhouse:
  version: "25.7"
schema: "db/main.sql"
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "housekeeper.yaml"),
		[]byte(config),
		consts.ModeFile,
	))

	// Create main schema file
	schema := `-- Main schema file
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "main.sql"),
		[]byte(schema),
		consts.ModeFile,
	))

	// Create test migrations
	migration1 := `-- Create database and users table
CREATE DATABASE IF NOT EXISTS analytics ENGINE = Atomic COMMENT 'Analytics database';

CREATE TABLE analytics.users (
    id UInt64,
    name String,
    email String,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY id
COMMENT 'User profiles';
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "migrations", "20240101120000_create_users.sql"),
		[]byte(migration1),
		consts.ModeFile,
	))

	migration2 := `-- Create events table
CREATE TABLE analytics.events (
    id UInt64,
    user_id UInt64,
    event_type String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (user_id, timestamp)
COMMENT 'User events';
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "migrations", "20240101130000_create_events.sql"),
		[]byte(migration2),
		consts.ModeFile,
	))

	// Create test dependencies
	cfg := testutil.DefaultConfig()
	cfg.Dir = filepath.Join(projectDir, "db", "migrations") // Set absolute path to migrations
	formatter := format.New(format.Defaults)
	version := &Version{Version: "test-1.0.0"}

	// Test initial migration
	t.Run("initial migrate command", func(t *testing.T) {
		// Create migrate command
		command := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Execute command
		err := testutil.RunCommand(t, command, []string{"--url", dsn}) //nolint:contextcheck
		require.NoError(t, err)

		// Verify databases and tables were created
		client, err := clickhouse.NewClient(ctx, dsn)
		require.NoError(t, err)
		defer client.Close()

		// Check housekeeper database
		rows, err := client.Query(ctx, "SELECT 1 FROM system.databases WHERE name = 'housekeeper'")
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())

		// Check analytics database
		rows, err = client.Query(ctx, "SELECT 1 FROM system.databases WHERE name = 'analytics'")
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())

		// Check users table
		rows, err = client.Query(ctx, "SELECT 1 FROM system.tables WHERE database = 'analytics' AND name = 'users'")
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())

		// Check events table
		rows, err = client.Query(ctx, "SELECT 1 FROM system.tables WHERE database = 'analytics' AND name = 'events'")
		require.NoError(t, err)
		defer rows.Close()
		require.True(t, rows.Next())

		// Check revisions were recorded
		rows, err = client.Query(ctx, "SELECT version FROM housekeeper.revisions ORDER BY version")
		require.NoError(t, err)
		defer rows.Close()

		var versions []string
		for rows.Next() {
			var version string
			require.NoError(t, rows.Scan(&version))
			versions = append(versions, version)
		}

		expected := []string{"20240101120000_create_users", "20240101130000_create_events"}
		require.Equal(t, expected, versions)
	})

	// Test running migrations again (should be skipped)
	t.Run("migrate command with no changes", func(t *testing.T) {
		// Create migrate command
		command := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Execute command
		err := testutil.RunCommand(t, command, []string{"--url", dsn}) //nolint:contextcheck
		require.NoError(t, err)
	})

	// Test dry run
	t.Run("dry run", func(t *testing.T) {
		// Add a new migration
		migration3 := `-- Add index to users table
ALTER TABLE analytics.users ADD INDEX idx_email email TYPE minmax GRANULARITY 4;
`
		require.NoError(t, os.WriteFile(
			filepath.Join(projectDir, "db", "migrations", "20240101140000_add_user_index.sql"),
			[]byte(migration3),
			consts.ModeFile,
		))

		// Create migrate command
		command := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Execute command with dry-run
		err := testutil.RunCommand(t, command, []string{"--url", dsn, "--dry-run"}) //nolint:contextcheck
		require.NoError(t, err)

		// Verify the new migration wasn't actually executed
		client, err := clickhouse.NewClient(ctx, dsn)
		require.NoError(t, err)
		defer client.Close()

		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM housekeeper.revisions WHERE version = '20240101140000_add_user_index'")
		require.NoError(t, err)
		defer rows.Close()

		var count uint64
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&count))
		require.Equal(t, uint64(0), count, "Migration should not have been executed in dry run")
	})

	// Test applying the new migration
	t.Run("migrate command with new migration", func(t *testing.T) {
		// Create migrate command
		command := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Execute command
		err := testutil.RunCommand(t, command, []string{"--url", dsn}) //nolint:contextcheck
		require.NoError(t, err)

		// Verify the migration was executed
		client, err := clickhouse.NewClient(ctx, dsn)
		require.NoError(t, err)
		defer client.Close()

		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM housekeeper.revisions WHERE version = '20240101140000_add_user_index'")
		require.NoError(t, err)
		defer rows.Close()

		var count uint64
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&count))
		require.Equal(t, uint64(1), count, "Migration should have been executed")
	})

	// Test connection failure
	t.Run("connection failure", func(t *testing.T) {
		// Create migrate command
		command := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Execute command with invalid DSN
		err := testutil.RunCommand(t, command, []string{"--url", "invalid:9999"}) //nolint:contextcheck
		require.Error(t, err, "Should fail with invalid connection")
	})
}

func TestMigrateCommand_Aliases(t *testing.T) {
	// Create test dependencies
	projectDir := t.TempDir()
	cfg := testutil.DefaultConfig()
	cfg.Dir = filepath.Join(projectDir, "db", "migrations")
	formatter := format.New(format.Defaults)
	version := &Version{Version: "test-1.0.0"}

	// Test that command has correct name and aliases
	command := migrate(migrateParams{
		Config:    cfg,
		Formatter: formatter,
		Version:   version,
	})

	require.Equal(t, "migrate", command.Name)
	require.Contains(t, command.Aliases, "apply")
	require.Equal(t, "Apply pending migrations to ClickHouse", command.Usage)
}
