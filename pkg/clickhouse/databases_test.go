package clickhouse_test

import (
	"testing"
)

func TestClient_GetDatabases(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration tests in short mode")
	}

	t.Skip("Integration test - requires actual ClickHouse connection")

	// Example of what integration tests would look like:
	/*
		ctx := context.Background()
		client, err := clickhouse.NewClient(ctx, "localhost:9000")
		require.NoError(t, err)
		defer client.Close()

		databases, err := client.GetDatabases(ctx)
		require.NoError(t, err)
		require.NotNil(t, databases)

		// Should have parsed statements
		require.Greater(t, len(databases.Statements), 0)

		// All statements should be database creates
		for _, stmt := range databases.Statements {
			require.NotNil(t, stmt.CreateDatabase)
			// Check that system databases are excluded
			require.NotEqual(t, "system", stmt.CreateDatabase.Name)
			require.NotEqual(t, "information_schema", stmt.CreateDatabase.Name)
			require.NotEqual(t, "INFORMATION_SCHEMA", stmt.CreateDatabase.Name)
		}
	*/
}

func BenchmarkClient_GetDatabases(b *testing.B) {
	b.Skip("Benchmark test - requires actual ClickHouse connection")
}
