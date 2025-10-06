package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAddOnClusterToFunction(t *testing.T) {
	tests := []struct {
		name        string
		createQuery string
		cluster     string
		expected    string
	}{
		{
			name:        "function with backticks",
			createQuery: "CREATE FUNCTION `normalizedBrowser` AS (`br`) -> multiIf(lower(`br`) = 'firefox', 'Firefox', 'Other')",
			cluster:     "warehouse",
			expected:    "CREATE FUNCTION `normalizedBrowser` ON CLUSTER `warehouse` AS (`br`) -> multiIf(lower(`br`) = 'firefox', 'Firefox', 'Other')",
		},
		{
			name:        "function without backticks",
			createQuery: "CREATE FUNCTION normalizedBrowser AS (br) -> multiIf(lower(br) = 'firefox', 'Firefox', 'Other')",
			cluster:     "production",
			expected:    "CREATE FUNCTION normalizedBrowser ON CLUSTER `production` AS (br) -> multiIf(lower(br) = 'firefox', 'Firefox', 'Other')",
		},
		{
			name:        "function with no parameters",
			createQuery: "CREATE FUNCTION getCurrentTime AS () -> now()",
			cluster:     "test",
			expected:    "CREATE FUNCTION getCurrentTime ON CLUSTER `test` AS () -> now()",
		},
		{
			name:        "function already has ON CLUSTER",
			createQuery: "CREATE FUNCTION test ON CLUSTER existing AS () -> now()",
			cluster:     "new_cluster",
			expected:    "CREATE FUNCTION test ON CLUSTER existing AS () -> now()",
		},
		{
			name:        "empty cluster name",
			createQuery: "CREATE FUNCTION test AS () -> now()",
			cluster:     "",
			expected:    "CREATE FUNCTION test AS () -> now()",
		},
		{
			name: "complex multiline function",
			createQuery: `CREATE FUNCTION normalizedOS AS (os) -> multiIf(
				startsWith(lower(os), 'windows'), 'Windows',
				startsWith(lower(os), 'mac'), 'Mac',
				lower(os) IN ('ios', 'iphone'), 'iOS',
				lower(os) = 'android', 'Android',
				'Other'
			)`,
			cluster: "analytics",
			expected: `CREATE FUNCTION normalizedOS ON CLUSTER ` + "`analytics`" + ` AS (os) -> multiIf(
				startsWith(lower(os), 'windows'), 'Windows',
				startsWith(lower(os), 'mac'), 'Mac',
				lower(os) IN ('ios', 'iphone'), 'iOS',
				lower(os) = 'android', 'Android',
				'Other'
			)`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := &Client{
				options: ClientOptions{
					Cluster: tt.cluster,
				},
			}
			result := client.addOnClusterToFunction(tt.createQuery)
			require.Equal(t, tt.expected, result)
		})
	}
}
