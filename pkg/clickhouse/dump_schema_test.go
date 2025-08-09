package clickhouse_test

// This test validates the actual DumpSchema functionality against a real ClickHouse instance
// using our Docker package. It validates all schema extraction methods and ordering.

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/golden"
)

func TestDumpSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Docker tests in short mode")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	// Create temporary directory for test configuration
	tmpDir := t.TempDir()
	configDir := filepath.Join(tmpDir, "config.d")
	require.NoError(t, os.MkdirAll(configDir, consts.ModeDir))

	// Copy the clickhouse.xml config to config.d directory as main.xml
	configContent, err := os.ReadFile("testdata/clickhouse.xml")
	require.NoError(t, err)

	configFile := filepath.Join(configDir, "clickhouse.xml")
	require.NoError(t, os.WriteFile(configFile, configContent, consts.ModeFile))

	// Start ClickHouse container using our Docker package
	container := docker.NewWithOptions(docker.DockerOptions{
		Version:   project.DefaultClickHouseVersion,
		ConfigDir: configDir,
	})

	// Clean up at end
	defer func() { _ = container.Stop(ctx) }()

	// Start the container
	require.NoError(t, container.Start(ctx), "Failed to start ClickHouse container")

	// Get DSN and create client
	dsn, err := container.GetDSN()
	require.NoError(t, err)

	// Create client with cluster support
	client, err := clickhouse.NewClientWithOptions(ctx, dsn, clickhouse.ClientOptions{
		Cluster: "test_cluster",
	})
	require.NoError(t, err)
	defer func() { _ = client.Close() }()

	// Load and execute the sample schema statement by statement
	schemaContent, err := os.ReadFile("testdata/sample_schema.sql")
	require.NoError(t, err)

	err = executeSchemaSeparately(ctx, client, string(schemaContent))
	require.NoError(t, err, "Failed to execute sample schema")

	// Test the main DumpSchema function via client.GetSchema()
	// This tests the complete schema extraction functionality
	schema, err := client.GetSchema(ctx)
	require.NoError(t, err)
	require.NotNil(t, schema)

	// Format the extracted schema for golden file comparison
	var buf bytes.Buffer
	require.NoError(t, format.FormatSQL(&buf, format.Defaults, schema))

	// Compare with golden file using gotest.tools/v3/golden
	golden.Assert(t, buf.String(), "expected_schema.sql")

	// Test individual extraction methods work without errors
	_, err = client.GetDatabases(ctx)
	require.NoError(t, err)

	_, err = client.GetTables(ctx)
	require.NoError(t, err)

	_, err = client.GetViews(ctx)
	require.NoError(t, err)

	_, err = client.GetDictionaries(ctx)
	require.NoError(t, err)
}

// executeSchemaSeparately executes SQL statements individually to avoid multi-statement issues
func executeSchemaSeparately(ctx context.Context, client *clickhouse.Client, schemaSQL string) error {
	// Split by semicolons and execute each statement separately
	lines := strings.Split(schemaSQL, "\n")
	var currentStatement strings.Builder

	for _, line := range lines {
		line = strings.TrimSpace(line)

		// Skip comments and empty lines
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}

		currentStatement.WriteString(line)
		currentStatement.WriteString("\n")

		// If line ends with semicolon, execute the statement
		if strings.HasSuffix(line, ";") {
			stmt := strings.TrimSpace(currentStatement.String())
			if stmt != "" {
				if err := client.ExecuteMigration(ctx, stmt); err != nil {
					return err
				}
			}
			currentStatement.Reset()
		}
	}

	// Execute any remaining statement
	stmt := strings.TrimSpace(currentStatement.String())
	if stmt != "" {
		return client.ExecuteMigration(ctx, stmt)
	}

	return nil
}
