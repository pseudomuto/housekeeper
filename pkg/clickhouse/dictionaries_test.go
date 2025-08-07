package clickhouse_test

import (
	"testing"
)

func TestClient_GetDictionaries(t *testing.T) {
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

		dictionaries, err := client.GetDictionaries(ctx)
		require.NoError(t, err)
		require.NotNil(t, dictionaries)

		// Should have parsed statements
		require.Greater(t, len(dictionaries.Statements), 0)

		// All statements should be dictionary creates
		for _, stmt := range dictionaries.Statements {
			require.NotNil(t, stmt.CreateDictionary, "All statements should be CREATE DICTIONARY")

			// Check that system dictionaries are excluded by database name
			if stmt.CreateDictionary.Database != nil {
				require.NotEqual(t, "system", *stmt.CreateDictionary.Database)
				require.NotEqual(t, "information_schema", *stmt.CreateDictionary.Database)
				require.NotEqual(t, "INFORMATION_SCHEMA", *stmt.CreateDictionary.Database)
			}
		}
	*/
}

func BenchmarkClient_GetDictionaries(b *testing.B) {
	b.Skip("Benchmark test - requires actual ClickHouse connection")
}
