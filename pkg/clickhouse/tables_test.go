package clickhouse_test

import (
	"testing"
)

func TestClient_GetTables(t *testing.T) {
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

		tables, err := client.GetTables(ctx)
		require.NoError(t, err)
		require.NotNil(t, tables)

		// Should have parsed statements
		require.Greater(t, len(tables.Statements), 0)

		// All statements should be table creates
		for _, stmt := range tables.Statements {
			require.NotNil(t, stmt.CreateTable)

			// Check that system tables are excluded by database name
			if stmt.CreateTable.Database != nil {
				require.NotEqual(t, "system", *stmt.CreateTable.Database)
				require.NotEqual(t, "information_schema", *stmt.CreateTable.Database)
				require.NotEqual(t, "INFORMATION_SCHEMA", *stmt.CreateTable.Database)
			}
		}
	*/
}

func BenchmarkClient_GetTables(b *testing.B) {
	b.Skip("Benchmark test - requires actual ClickHouse connection")
}
