package schema

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestEnginesEqual(t *testing.T) {
	tests := []struct {
		name     string
		target   *parser.TableEngine
		current  *parser.TableEngine
		expected bool
	}{
		{
			name: "ReplicatedMergeTree() target should equal ReplicatedMergeTree with params",
			target: &parser.TableEngine{
				Name:       "ReplicatedMergeTree",
				Parameters: []parser.EngineParameter{}, // No parameters
			},
			current: &parser.TableEngine{
				Name: "ReplicatedMergeTree",
				Parameters: []parser.EngineParameter{
					{String: stringPtr("'/clickhouse/tables/{uuid}/{shard}'")},
					{String: stringPtr("'{replica}'")},
				},
			},
			expected: true,
		},
		{
			name: "ReplicatedMergeTree with different explicit params should not be equal",
			target: &parser.TableEngine{
				Name: "ReplicatedMergeTree",
				Parameters: []parser.EngineParameter{
					{String: stringPtr("'/clickhouse/tables/new_path/{shard}'")},
					{String: stringPtr("'{replica}'")},
				},
			},
			current: &parser.TableEngine{
				Name: "ReplicatedMergeTree",
				Parameters: []parser.EngineParameter{
					{String: stringPtr("'/clickhouse/tables/old_path/{shard}'")},
					{String: stringPtr("'{replica}'")},
				},
			},
			expected: false,
		},
		{
			name: "MergeTree engines should use normal comparison",
			target: &parser.TableEngine{
				Name:       "MergeTree",
				Parameters: []parser.EngineParameter{},
			},
			current: &parser.TableEngine{
				Name:       "MergeTree",
				Parameters: []parser.EngineParameter{},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := enginesEqual(tt.target, tt.current)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestTableInfoEqual(t *testing.T) {
	// Test ReplicatedMergeTree with different explicit parameters
	currentTable := &TableInfo{
		Name:     "events",
		Database: "",
		Engine: &parser.TableEngine{
			Name: "ReplicatedMergeTree",
			Parameters: []parser.EngineParameter{
				{String: stringPtr("'/clickhouse/tables/old_path/{shard}'")},
				{String: stringPtr("'{replica}'")},
			},
		},
		Columns: []ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{Name: "data", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
		},
	}

	targetTable := &TableInfo{
		Name:     "events",
		Database: "",
		Engine: &parser.TableEngine{
			Name: "ReplicatedMergeTree",
			Parameters: []parser.EngineParameter{
				{String: stringPtr("'/clickhouse/tables/new_path/{shard}'")},
				{String: stringPtr("'{replica}'")},
			},
		},
		Columns: []ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{Name: "data", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
		},
	}

	// These should NOT be equal due to different engine parameters
	result := currentTable.Equal(targetTable)
	require.False(t, result, "Tables with different ReplicatedMergeTree parameters should not be equal")
}

func TestInferSchemaCluster(t *testing.T) {
	cases := []struct {
		name     string
		tables   map[string]*TableInfo
		expected string
	}{
		{
			name: "macro cluster is authoritative",
			tables: map[string]*TableInfo{
				"events": {Cluster: "'{cluster}'"},
				"sales":  {Cluster: "'{cluster}'"},
			},
			expected: "'{cluster}'",
		},
		{
			name: "macro wins over plain names in the same map",
			tables: map[string]*TableInfo{
				"events": {Cluster: "default"},
				"sales":  {Cluster: "'{cluster}'"},
			},
			expected: "'{cluster}'",
		},
		{
			name: "unanimous plain name is returned",
			tables: map[string]*TableInfo{
				"events": {Cluster: "default"},
				"sales":  {Cluster: "default"},
			},
			expected: "default",
		},
		{
			name: "mixed plain names — no normalisation",
			tables: map[string]*TableInfo{
				"events": {Cluster: "cluster-a"},
				"sales":  {Cluster: "cluster-b"},
			},
			expected: "",
		},
		{
			name: "no clustered tables — no normalisation",
			tables: map[string]*TableInfo{
				"events": {Cluster: ""},
			},
			expected: "",
		},
		{
			name:     "empty map",
			tables:   map[string]*TableInfo{},
			expected: "",
		},
		{
			name: "shard macro is also accepted",
			tables: map[string]*TableInfo{
				"metrics": {Cluster: "'{shard}'"},
			},
			expected: "'{shard}'",
		},
		{
			name: "plain-quoted string without braces is NOT a macro",
			tables: map[string]*TableInfo{
				"events": {Cluster: "'plain-string'"},
			},
			expected: "'plain-string'", // treated as unanimous plain name
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := inferSchemaCluster(tc.tables)
			require.Equal(t, tc.expected, got)
		})
	}
}

// TestCompareTables_ClusterNormalisation verifies that ALTER/DROP/RENAME
// migrations use the target schema's cluster macro ('{cluster}') rather than
// the concrete cluster name that ClickHouse reports after macro expansion.
func TestCompareTables_ClusterNormalisation(t *testing.T) {
	// current = live DB state after applying migrations; ClickHouse has expanded
	// '{cluster}' → 'default' in SHOW CREATE TABLE output.
	current, err := parser.ParseString(`
		CREATE TABLE events ON CLUSTER default (
			id UInt64, name String
		) ENGINE = ReplicatedMergeTree() ORDER BY id;
	`)
	require.NoError(t, err)

	// target = desired schema with the portable macro syntax.
	target, err := parser.ParseString(`
		CREATE TABLE events ON CLUSTER '{cluster}' (
			id UInt64, name String, ts DateTime
		) ENGINE = ReplicatedMergeTree() ORDER BY id;
	`)
	require.NoError(t, err)

	diffs, err := compareTables(current, target)
	require.NoError(t, err)
	require.Len(t, diffs, 1)

	// The ALTER TABLE migration must use the macro, not the concrete name.
	require.Contains(t, diffs[0].UpSQL, "'{cluster}'",
		"ALTER TABLE UpSQL must use the macro cluster syntax")
	require.NotContains(t, diffs[0].UpSQL, "`default`",
		"ALTER TABLE UpSQL must not hard-code the concrete cluster name")
}

// TestCompareTables_DropClusterNormalisation verifies that DROP TABLE
// migrations generated for orphaned tables also use the target schema's cluster
// macro rather than the concrete cluster name from the live DB.
func TestCompareTables_DropClusterNormalisation(t *testing.T) {
	// current contains a table that no longer exists in the desired schema.
	current, err := parser.ParseString(`
		CREATE TABLE events ON CLUSTER default (
			id UInt64
		) ENGINE = ReplicatedMergeTree() ORDER BY id;
		CREATE TABLE orphan ON CLUSTER default (
			id UInt64
		) ENGINE = ReplicatedMergeTree() ORDER BY id;
	`)
	require.NoError(t, err)

	// target keeps only 'events', using the portable macro.
	target, err := parser.ParseString(`
		CREATE TABLE events ON CLUSTER '{cluster}' (
			id UInt64
		) ENGINE = ReplicatedMergeTree() ORDER BY id;
	`)
	require.NoError(t, err)

	diffs, err := compareTables(current, target)
	require.NoError(t, err)

	var dropDiff *TableDiff
	for _, d := range diffs {
		if d.Type == string(TableDiffDrop) {
			dropDiff = d
			break
		}
	}
	require.NotNil(t, dropDiff, "expected a DROP diff for orphan table")
	require.Contains(t, dropDiff.UpSQL, "'{cluster}'",
		"DROP TABLE UpSQL must use the macro cluster syntax")
	require.NotContains(t, dropDiff.UpSQL, "`default`",
		"DROP TABLE UpSQL must not hard-code the concrete cluster name")
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
