package clickhouse

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestHousekeeperObjectsNotOnCluster(t *testing.T) {
	tests := []struct {
		name           string
		sql            string
		cluster        string
		expectedResult []ClusterExpectation
	}{
		{
			name:    "regular database gets ON CLUSTER",
			sql:     "CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "database", Name: "analytics", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "housekeeper database does NOT get ON CLUSTER",
			sql:     "CREATE DATABASE housekeeper ENGINE = Atomic COMMENT 'Migration tracking';",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "database", Name: "housekeeper", HasCluster: false},
			},
		},
		{
			name:    "regular table gets ON CLUSTER",
			sql:     "CREATE TABLE analytics.events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "table", Name: "events", Database: "analytics", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "housekeeper table does NOT get ON CLUSTER",
			sql:     "CREATE TABLE housekeeper.revisions (version String, executed_at DateTime) ENGINE = MergeTree() ORDER BY version;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "table", Name: "revisions", Database: "housekeeper", HasCluster: false},
			},
		},
		{
			name:    "regular dictionary gets ON CLUSTER",
			sql:     "CREATE DICTIONARY analytics.users (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/users')) LAYOUT(HASHED()) LIFETIME(3600);",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "dictionary", Name: "users", Database: "analytics", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "housekeeper dictionary does NOT get ON CLUSTER",
			sql:     "CREATE DICTIONARY housekeeper.lookup (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/lookup')) LAYOUT(HASHED()) LIFETIME(3600);",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "dictionary", Name: "lookup", Database: "housekeeper", HasCluster: false},
			},
		},
		{
			name:    "regular view gets ON CLUSTER",
			sql:     "CREATE VIEW analytics.summary AS SELECT count() as cnt FROM analytics.events;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "view", Name: "summary", Database: "analytics", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "housekeeper view does NOT get ON CLUSTER",
			sql:     "CREATE VIEW housekeeper.status AS SELECT version, executed_at FROM housekeeper.revisions;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "view", Name: "status", Database: "housekeeper", HasCluster: false},
			},
		},
		{
			name:    "regular materialized view gets ON CLUSTER",
			sql:     "CREATE MATERIALIZED VIEW analytics.mv_stats ENGINE = MergeTree() ORDER BY date AS SELECT toDate(timestamp) as date, count() as cnt FROM analytics.events GROUP BY date;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "view", Name: "mv_stats", Database: "analytics", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "housekeeper materialized view does NOT get ON CLUSTER",
			sql:     "CREATE MATERIALIZED VIEW housekeeper.mv_status ENGINE = MergeTree() ORDER BY date AS SELECT toDate(executed_at) as date, count() as cnt FROM housekeeper.revisions GROUP BY date;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "view", Name: "mv_status", Database: "housekeeper", HasCluster: false},
			},
		},
		{
			name: "mixed objects - only non-housekeeper get ON CLUSTER",
			sql: `CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';
CREATE DATABASE housekeeper ENGINE = Atomic COMMENT 'Migration tracking';
CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE housekeeper.revisions (version String) ENGINE = MergeTree() ORDER BY version;`,
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "database", Name: "analytics", HasCluster: true, ClusterName: "production"},
				{Type: "database", Name: "housekeeper", HasCluster: false},
				{Type: "table", Name: "events", Database: "analytics", HasCluster: true, ClusterName: "production"},
				{Type: "table", Name: "revisions", Database: "housekeeper", HasCluster: false},
			},
		},
		{
			name:    "default database tables get ON CLUSTER",
			sql:     "CREATE TABLE events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;",
			cluster: "production",
			expectedResult: []ClusterExpectation{
				{Type: "table", Name: "events", Database: "default", HasCluster: true, ClusterName: "production"},
			},
		},
		{
			name:    "no cluster specified - no injection",
			sql:     "CREATE DATABASE analytics ENGINE = Atomic; CREATE DATABASE housekeeper ENGINE = Atomic;",
			cluster: "",
			expectedResult: []ClusterExpectation{
				{Type: "database", Name: "analytics", HasCluster: false},
				{Type: "database", Name: "housekeeper", HasCluster: false},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			parsed, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, parsed.Statements, len(tt.expectedResult))

			// Apply InjectOnCluster (this is the function we're testing)
			result := parser.InjectOnCluster(parsed.Statements, tt.cluster)

			// Verify the results
			require.Len(t, result, len(tt.expectedResult))
			for i, stmt := range result {
				expected := tt.expectedResult[i]
				actual := extractClusterInfo(stmt)

				require.Equal(t, expected, actual, "Statement %d mismatch", i)
			}
		})
	}
}

// ClusterExpectation represents what we expect for cluster configuration
type ClusterExpectation struct {
	Type        string // "database", "table", "dictionary", "view"
	Name        string
	Database    string // For non-database objects
	HasCluster  bool
	ClusterName string
}

// extractClusterInfo extracts cluster information from a parsed statement for verification
func extractClusterInfo(stmt *parser.Statement) ClusterExpectation {
	switch {
	case stmt.CreateDatabase != nil:
		db := stmt.CreateDatabase
		expectation := ClusterExpectation{
			Type: "database",
			Name: db.Name,
		}
		if db.OnCluster != nil {
			expectation.HasCluster = true
			expectation.ClusterName = *db.OnCluster
		}
		return expectation

	case stmt.CreateTable != nil:
		table := stmt.CreateTable
		expectation := ClusterExpectation{
			Type:     "table",
			Name:     table.Name,
			Database: getDatabaseName(table.Database),
		}
		if table.OnCluster != nil {
			expectation.HasCluster = true
			expectation.ClusterName = *table.OnCluster
		}
		return expectation

	case stmt.CreateDictionary != nil:
		dict := stmt.CreateDictionary
		expectation := ClusterExpectation{
			Type:     "dictionary",
			Name:     dict.Name,
			Database: getDatabaseName(dict.Database),
		}
		if dict.OnCluster != nil {
			expectation.HasCluster = true
			expectation.ClusterName = *dict.OnCluster
		}
		return expectation

	case stmt.CreateView != nil:
		view := stmt.CreateView
		expectation := ClusterExpectation{
			Type:     "view",
			Name:     view.Name,
			Database: getDatabaseName(view.Database),
		}
		if view.OnCluster != nil {
			expectation.HasCluster = true
			expectation.ClusterName = *view.OnCluster
		}
		return expectation

	default:
		return ClusterExpectation{Type: "unknown"}
	}
}

// extracts the database name from a pointer, defaulting to "default" if nil.
func getDatabaseName(database *string) string {
	if database != nil && *database != "" {
		return *database
	}
	return "default"
}
