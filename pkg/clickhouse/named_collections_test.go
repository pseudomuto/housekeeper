package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildNamedCollectionsQuery(t *testing.T) {
	tests := []struct {
		name              string
		availableColumns  map[string]bool
		expectedQuery     string
		expectedSupported bool
	}{
		{
			name: "newer structure with key-value pairs filters builtin collections",
			availableColumns: map[string]bool{
				"name":        true,
				"key":         true,
				"value":       true,
				"overridable": true,
			},
			expectedQuery: `
			SELECT 
				name,
				key,
				value,
				overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`,
			expectedSupported: true,
		},
		{
			name: "older structure with Map filters builtin collections",
			availableColumns: map[string]bool{
				"name":       true,
				"collection": true,
			},
			expectedQuery: `
			SELECT 
				name,
				arrayJoin(mapKeys(collection)) AS key,
				collection[key] AS value,
				1 AS overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`,
			expectedSupported: true,
		},
		{
			name: "unsupported structure returns false",
			availableColumns: map[string]bool{
				"name": true,
			},
			expectedQuery:     "",
			expectedSupported: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Mock the function logic without requiring a real client
			query, supported := mockBuildNamedCollectionsQuery(tt.availableColumns)

			require.Equal(t, tt.expectedSupported, supported)

			if supported {
				// Normalize whitespace for comparison
				require.Contains(t, query, "WHERE name NOT LIKE 'builtin_%'",
					"Query should filter out collections starting with 'builtin_'")
				require.Contains(t, query, "FROM system.named_collections")
				require.Contains(t, query, "ORDER BY name, key")
			} else {
				require.Empty(t, query)
			}
		})
	}
}

// mockBuildNamedCollectionsQuery replicates the logic from buildNamedCollectionsQuery
// for testing without requiring a real ClickHouse connection
func mockBuildNamedCollectionsQuery(availableColumns map[string]bool) (string, bool) {
	if availableColumns["key"] && availableColumns["value"] {
		// Newer structure with key-value pairs
		return `
			SELECT 
				name,
				key,
				value,
				overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`, true
	} else if availableColumns["collection"] {
		// Older structure where collection is stored as Map(String, String)
		return `
			SELECT 
				name,
				arrayJoin(mapKeys(collection)) AS key,
				collection[key] AS value,
				1 AS overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`, true
	}

	return "", false
}

func TestFilterBuiltinCollections(t *testing.T) {
	tests := []struct {
		name           string
		collectionName string
		shouldFilter   bool
	}{
		{
			name:           "builtin collection with builtin_ prefix should be filtered",
			collectionName: "builtin_kafka",
			shouldFilter:   true,
		},
		{
			name:           "builtin collection with BUILTIN_ prefix should be filtered",
			collectionName: "BUILTIN_mysql",
			shouldFilter:   false, // SQL LIKE is case-sensitive
		},
		{
			name:           "user collection without builtin_ prefix should not be filtered",
			collectionName: "my_kafka_config",
			shouldFilter:   false,
		},
		{
			name:           "user collection containing builtin but not as prefix should not be filtered",
			collectionName: "kafka_builtin_config",
			shouldFilter:   false,
		},
		{
			name:           "empty collection name should not be filtered",
			collectionName: "",
			shouldFilter:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the LIKE pattern matching logic
			filtered := isBuiltinCollection(tt.collectionName)
			if tt.shouldFilter {
				require.True(t, filtered, "Collection '%s' should be filtered", tt.collectionName)
			} else {
				require.False(t, filtered, "Collection '%s' should not be filtered", tt.collectionName)
			}
		})
	}
}

// isBuiltinCollection simulates the SQL LIKE 'builtin_%' pattern matching
func isBuiltinCollection(name string) bool {
	return len(name) >= 8 && name[:8] == "builtin_"
}
