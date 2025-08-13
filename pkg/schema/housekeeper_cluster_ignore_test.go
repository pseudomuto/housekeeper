package schema_test

import (
	"bytes"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestHousekeeperObjectsIgnoreClusterDifferences(t *testing.T) {
	tests := []struct {
		name          string
		currentSQL    string
		targetSQL     string
		expectNoDiff  bool
		expectedDiffs int
	}{
		{
			name:          "regular database with cluster difference should create diff",
			currentSQL:    `CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';`,
			targetSQL:     `CREATE DATABASE analytics ON CLUSTER production ENGINE = Atomic COMMENT 'Analytics DB';`,
			expectNoDiff:  false,
			expectedDiffs: 0, // This should cause validation error due to cluster change
		},
		{
			name:          "housekeeper database with cluster difference should be ignored",
			currentSQL:    `CREATE DATABASE housekeeper ENGINE = Atomic COMMENT 'Migration tracking';`,
			targetSQL:     `CREATE DATABASE housekeeper ON CLUSTER production ENGINE = Atomic COMMENT 'Migration tracking';`,
			expectNoDiff:  true,
			expectedDiffs: 0,
		},
		{
			name: "regular table with cluster difference should create diff",
			currentSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE TABLE analytics.events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;`,
			targetSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE TABLE analytics.events ON CLUSTER production (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;`,
			expectNoDiff:  false,
			expectedDiffs: 1, // Table modification
		},
		{
			name: "housekeeper table with cluster difference should be ignored",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE TABLE housekeeper.revisions (version String, executed_at DateTime) ENGINE = MergeTree() ORDER BY version;`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE TABLE housekeeper.revisions ON CLUSTER production (version String, executed_at DateTime) ENGINE = MergeTree() ORDER BY version;`,
			expectNoDiff:  true,
			expectedDiffs: 0,
		},
		{
			name: "regular view with cluster difference should create diff",
			currentSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE VIEW analytics.summary AS SELECT count() as cnt FROM events;`,
			targetSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE VIEW analytics.summary ON CLUSTER production AS SELECT count() as cnt FROM events;`,
			expectNoDiff:  false,
			expectedDiffs: 1,
		},
		{
			name: "housekeeper view with cluster difference should be ignored",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE VIEW housekeeper.status AS SELECT version, executed_at FROM housekeeper.revisions;`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE VIEW housekeeper.status ON CLUSTER production AS SELECT version, executed_at FROM housekeeper.revisions;`,
			expectNoDiff:  true,
			expectedDiffs: 0,
		},
		{
			name: "regular dictionary with cluster difference should create diff",
			currentSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE DICTIONARY analytics.users (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/users')) LAYOUT(HASHED()) LIFETIME(3600);`,
			targetSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE DICTIONARY analytics.users ON CLUSTER production (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/users')) LAYOUT(HASHED()) LIFETIME(3600);`,
			expectNoDiff:  false,
			expectedDiffs: 1,
		},
		{
			name: "housekeeper dictionary with cluster difference should be ignored",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE DICTIONARY housekeeper.lookup (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/lookup')) LAYOUT(HASHED()) LIFETIME(3600);`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE DICTIONARY housekeeper.lookup ON CLUSTER production (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/lookup')) LAYOUT(HASHED()) LIFETIME(3600);`,
			expectNoDiff:  true,
			expectedDiffs: 0,
		},
		{
			name: "mixed objects - only housekeeper should ignore cluster differences",
			currentSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE housekeeper.revisions (version String) ENGINE = MergeTree() ORDER BY version;`,
			targetSQL: `
CREATE DATABASE analytics ENGINE = Atomic;
CREATE DATABASE housekeeper ON CLUSTER production ENGINE = Atomic;
CREATE TABLE analytics.events ON CLUSTER production (id UInt64) ENGINE = MergeTree() ORDER BY id;
CREATE TABLE housekeeper.revisions ON CLUSTER production (version String) ENGINE = MergeTree() ORDER BY version;`,
			expectNoDiff:  false,
			expectedDiffs: 1, // Only analytics.events should create diff
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			current, err := parser.ParseString(tt.currentSQL)
			require.NoError(t, err)

			target, err := parser.ParseString(tt.targetSQL)
			require.NoError(t, err)

			// Generate diff
			diff, err := schema.GenerateDiff(current, target)

			if tt.expectNoDiff {
				// Should have no error and no SQL changes
				if err != nil && err.Error() == "no differences found" {
					// This is expected for no differences
					return
				}
				require.NoError(t, err)

				// Format the diff SQL to check if it's empty
				var buf bytes.Buffer
				err = format.FormatSQL(&buf, format.Defaults, diff)
				require.NoError(t, err)
				formattedSQL := buf.String()

				require.Empty(t, formattedSQL, "Expected no SQL changes for housekeeper objects")
			} else {
				// May have validation errors for unsupported operations like cluster changes
				if err != nil {
					// Some operations like cluster changes are not supported
					require.Contains(t, err.Error(), "cluster configuration changes not supported")
				} else {
					// Should have SQL changes
					var buf bytes.Buffer
					err = format.FormatSQL(&buf, format.Defaults, diff)
					require.NoError(t, err)
					formattedSQL := buf.String()

					require.NotEmpty(t, formattedSQL, "Expected SQL changes for regular objects")
				}
			}
		})
	}
}

func TestHousekeeperObjectRenameDetection(t *testing.T) {
	tests := []struct {
		name                  string
		currentSQL            string
		targetSQL             string
		expectedOperationType string // "RENAME", "CREATE", "DROP+CREATE"
		expectedObjectType    string // "DATABASE", "TABLE", "VIEW", "DICTIONARY"
	}{
		{
			name:                  "housekeeper database rename without cluster difference should be detected",
			currentSQL:            `CREATE DATABASE old_housekeeper ENGINE = Atomic COMMENT 'Migration tracking';`,
			targetSQL:             `CREATE DATABASE housekeeper ENGINE = Atomic COMMENT 'Migration tracking';`,
			expectedOperationType: "RENAME",
			expectedObjectType:    "DATABASE",
		},
		{
			name: "housekeeper table rename without cluster difference should be detected",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE TABLE housekeeper.old_revisions (version String) ENGINE = MergeTree() ORDER BY version;`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE TABLE housekeeper.revisions (version String) ENGINE = MergeTree() ORDER BY version;`,
			expectedOperationType: "RENAME",
			expectedObjectType:    "TABLE",
		},
		{
			name: "housekeeper view change without cluster difference should generate CREATE+DROP",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE VIEW housekeeper.old_status AS SELECT version FROM revisions;`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE VIEW housekeeper.status AS SELECT version FROM revisions;`,
			expectedOperationType: "CREATE+DROP",
			expectedObjectType:    "VIEW", // Views currently use CREATE+DROP instead of rename detection
		},
		{
			name: "housekeeper dictionary rename without cluster difference should be detected",
			currentSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE DICTIONARY housekeeper.old_lookup (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/lookup')) LAYOUT(HASHED()) LIFETIME(3600);`,
			targetSQL: `
CREATE DATABASE housekeeper ENGINE = Atomic;
CREATE DICTIONARY housekeeper.lookup (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/lookup')) LAYOUT(HASHED()) LIFETIME(3600);`,
			expectedOperationType: "RENAME",
			expectedObjectType:    "DICTIONARY",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			current, err := parser.ParseString(tt.currentSQL)
			require.NoError(t, err)

			target, err := parser.ParseString(tt.targetSQL)
			require.NoError(t, err)

			// Generate diff
			diff, err := schema.GenerateDiff(current, target)
			require.NoError(t, err)

			// Format the diff SQL
			var buf bytes.Buffer
			err = format.FormatSQL(&buf, format.Defaults, diff)
			require.NoError(t, err)
			formattedSQL := buf.String()

			// Check that operation was detected
			require.NotEmpty(t, formattedSQL, "Expected SQL changes for operation")

			// Verify the operation type
			switch tt.expectedOperationType {
			case "RENAME":
				require.Contains(t, formattedSQL, "RENAME", "Expected RENAME operation in generated SQL")
				// Verify the rename is for the expected object type
				switch tt.expectedObjectType {
				case "DATABASE":
					require.Contains(t, formattedSQL, "RENAME DATABASE", "Expected database rename")
				case "TABLE":
					require.Contains(t, formattedSQL, "RENAME TABLE", "Expected table rename")
				case "VIEW":
					require.Contains(t, formattedSQL, "RENAME TABLE", "Expected view rename (uses RENAME TABLE)")
				case "DICTIONARY":
					require.Contains(t, formattedSQL, "RENAME DICTIONARY", "Expected dictionary rename")
				}
			case "CREATE+DROP":
				require.Contains(t, formattedSQL, "CREATE", "Expected CREATE operation in generated SQL")
				require.Contains(t, formattedSQL, "DROP", "Expected DROP operation in generated SQL")
				// Verify it's for the expected object type
				switch tt.expectedObjectType {
				case "VIEW":
					require.Contains(t, formattedSQL, "CREATE VIEW", "Expected CREATE VIEW")
					require.Contains(t, formattedSQL, "DROP VIEW", "Expected DROP VIEW")
				}
			}
		})
	}
}
