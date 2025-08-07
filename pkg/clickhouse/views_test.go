package clickhouse_test

import (
	"testing"
)

func TestClient_GetViews(t *testing.T) {
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

		views, err := client.GetViews(ctx)
		require.NoError(t, err)
		require.NotNil(t, views)

		// Should have parsed statements
		require.Greater(t, len(views.Statements), 0)

		// All statements should be view creates
		for _, stmt := range views.Statements {
			require.NotNil(t, stmt.CreateView, "All statements should be CREATE VIEW")

			// Check that system views are excluded by database name
			if stmt.CreateView.Database != nil {
				require.NotEqual(t, "system", *stmt.CreateView.Database)
				require.NotEqual(t, "information_schema", *stmt.CreateView.Database)
				require.NotEqual(t, "INFORMATION_SCHEMA", *stmt.CreateView.Database)
			}
		}
	*/
}

func BenchmarkClient_GetViews(b *testing.B) {
	b.Skip("Benchmark test - requires actual ClickHouse connection")
}
