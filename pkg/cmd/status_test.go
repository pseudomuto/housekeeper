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
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStatusCommand_Integration(t *testing.T) {
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
CREATE DATABASE status_test ENGINE = Atomic COMMENT 'Status test database';
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "main.sql"),
		[]byte(schema),
		consts.ModeFile,
	))

	// Create test migrations
	migration1 := `-- Create database and users table
CREATE DATABASE IF NOT EXISTS status_test ENGINE = Atomic COMMENT 'Status test database';

CREATE TABLE status_test.users (
    id UInt64,
    name String
) ENGINE = MergeTree() ORDER BY id;
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "migrations", "20240101120000_create_users.sql"),
		[]byte(migration1),
		consts.ModeFile,
	))

	migration2 := `-- Create events table
CREATE TABLE status_test.events (
    id UInt64,
    user_id UInt64
) ENGINE = MergeTree() ORDER BY id;
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "migrations", "20240101130000_create_events.sql"),
		[]byte(migration2),
		consts.ModeFile,
	))

	migration3 := `-- Create analytics table
CREATE TABLE status_test.analytics (
    date Date,
    count UInt64
) ENGINE = MergeTree() ORDER BY date;
`
	require.NoError(t, os.WriteFile(
		filepath.Join(projectDir, "db", "migrations", "20240101140000_create_analytics.sql"),
		[]byte(migration3),
		consts.ModeFile,
	))

	// Create test dependencies
	cfg := testutil.DefaultConfig()
	cfg.Dir = filepath.Join(projectDir, "db", "migrations") // Set absolute path to migrations
	formatter := format.New(format.Defaults)

	t.Run("status before bootstrap", func(t *testing.T) {
		// Create status command
		command := status(statusParams{
			Config: cfg,
		})

		// Execute command
		err := testutil.RunCommand(t, command, []string{"--url", dsn})
		require.NoError(t, err)
	})

	t.Run("status after partial execution", func(t *testing.T) {
		// First, apply some migrations using the migrate command
		version := &Version{Version: "test-1.0.0"}
		migrateCommand := migrate(migrateParams{
			Config:    cfg,
			Formatter: formatter,
			Version:   version,
		})

		// Apply migrations
		err := testutil.RunCommand(t, migrateCommand, []string{"--url", dsn}) //nolint:contextcheck
		require.NoError(t, err)

		// Check status
		statusCommand := status(statusParams{
			Config: cfg,
		})

		err = testutil.RunCommand(t, statusCommand, []string{"--url", dsn}) //nolint:contextcheck
		require.NoError(t, err)

		// Verify that migrations were applied
		client, err := clickhouse.NewClient(ctx, dsn)
		require.NoError(t, err)
		defer client.Close()

		// Check that revisions were recorded
		rows, err := client.Query(ctx, "SELECT COUNT(*) FROM housekeeper.revisions")
		require.NoError(t, err)
		defer rows.Close()

		var count uint64
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&count))
		assert.Positive(t, count, "Should have recorded revisions")
	})

	t.Run("status with verbose flag", func(t *testing.T) {
		// Create status command
		command := status(statusParams{
			Config: cfg,
		})

		// Execute command with verbose flag
		err := testutil.RunCommand(t, command, []string{"--url", dsn, "--verbose"})
		require.NoError(t, err)
	})

	t.Run("status with connection failure", func(t *testing.T) {
		// Create status command
		command := status(statusParams{
			Config: cfg,
		})

		// Execute command with invalid DSN
		err := testutil.RunCommand(t, command, []string{"--url", "invalid:9999"})
		assert.Error(t, err, "Should fail with invalid connection")
	})
}

func TestStatusCommand_CommandStructure(t *testing.T) {
	// Create test dependencies
	cfg := testutil.DefaultConfig()

	// Test that command has correct structure
	command := status(statusParams{
		Config: cfg,
	})

	assert.Equal(t, "status", command.Name)
	assert.Equal(t, "Show migration status", command.Usage)
	assert.NotEmpty(t, command.Description)
	assert.NotNil(t, command.Action)

	// Check that required flags exist
	urlFlag := false
	clusterFlag := false
	verboseFlag := false

	for _, flag := range command.Flags {
		switch flag.Names()[0] {
		case "url":
			urlFlag = true
		case "cluster":
			clusterFlag = true
		case "verbose":
			verboseFlag = true
		}
	}

	assert.True(t, urlFlag, "Should have url flag")
	assert.True(t, clusterFlag, "Should have cluster flag")
	assert.True(t, verboseFlag, "Should have verbose flag")
}
