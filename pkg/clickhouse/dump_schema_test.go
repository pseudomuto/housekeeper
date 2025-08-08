package clickhouse_test

// This test validates the actual DumpSchema functionality against a real ClickHouse instance
// using the testcontainers ClickHouse module. It validates all schema extraction methods and ordering.

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	clickhousemodule "github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/wait"
	"gotest.tools/v3/golden"
)

func TestDumpSchema(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping testcontainer tests in short mode")
	}

	ctx := context.Background()

	// Start ClickHouse container with the ClickHouse module
	container, err := clickhousemodule.Run(
		ctx,
		fmt.Sprintf("clickhouse/clickhouse-server:%s-alpine", project.DefaultClickHouseVersion),
		clickhousemodule.WithUsername("default"),
		clickhousemodule.WithPassword(""),
		clickhousemodule.WithDatabase("default"),
		clickhousemodule.WithConfigFile("testdata/clickhouse.xml"),
		clickhousemodule.WithInitScripts("testdata/sample_schema.sql"),
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}),
		testcontainers.WithWaitStrategyAndDeadline(
			5*time.Minute,
			wait.
				NewHTTPStrategy("/").
				WithPort(nat.Port("8123/tcp")).
				WithStatusCodeMatcher(func(status int) bool {
					return status == 200
				}),
		),
	)
	if err != nil {
		// Dump the container logs on error (if available).
		// NB: This is nice when a config/init script issue causes the process to die.
		if container != nil {
			logs, _ := container.Logs(ctx)
			if logs != nil {
				io.Copy(os.Stderr, logs) // nolint: errcheck
			}
		}

		require.FailNow(t, "failed to standup clickhouse")
	}
	defer func() { _ = container.Terminate(ctx) }()

	// Get connection string
	connectionHost, err := container.ConnectionHost(ctx)
	require.NoError(t, err)

	// Create client using the connection host
	client, err := clickhouse.NewClientWithOptions(ctx, connectionHost, clickhouse.ClientOptions{
		Cluster: "test_cluster",
	})
	require.NoError(t, err)
	defer client.Close()

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
