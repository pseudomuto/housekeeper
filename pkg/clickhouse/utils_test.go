package clickhouse

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSystemDatabaseExclusion(t *testing.T) {
	tests := []struct {
		name           string
		columnName     string
		expectedQuery  string
		expectedParams []any
	}{
		{
			name:           "database column",
			columnName:     "database",
			expectedQuery:  "database NOT IN (?, ?, ?, ?, ?)",
			expectedParams: []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "housekeeper"},
		},
		{
			name:           "name column",
			columnName:     "name",
			expectedQuery:  "name NOT IN (?, ?, ?, ?, ?)",
			expectedParams: []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "housekeeper"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, params := buildSystemDatabaseExclusion(tt.columnName)
			require.Equal(t, tt.expectedQuery, query)
			require.Equal(t, tt.expectedParams, params)
		})
	}
}

func TestNormalizeDataTypesInDDL(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Hidden password normalization",
			input:    "SOURCE(HTTP(url 'http://example.com', user 'user', password '[HIDDEN]'))",
			expected: "SOURCE(HTTP(url 'http://example.com', user 'user', password ''))",
		},
		{
			name:     "Case insensitive password normalization",
			input:    "SOURCE(HTTP(url 'http://example.com', PASSWORD '[HIDDEN]'))",
			expected: "SOURCE(HTTP(url 'http://example.com', password ''))",
		},
		{
			name:     "Float default normalization with space",
			input:    "CREATE TABLE test (score Float32 DEFAULT 0. ) ENGINE = MergeTree();",
			expected: "CREATE TABLE test (score Float32 DEFAULT 0.0 ) ENGINE = MergeTree();",
		},
		{
			name:     "Float default normalization at end",
			input:    "score Float32 DEFAULT 0.",
			expected: "score Float32 DEFAULT 0.0",
		},
		{
			name:     "LIFETIME normalization",
			input:    "LIFETIME(MIN 0 MAX 3600)",
			expected: "LIFETIME(3600)",
		},
		{
			name:     "No normalization needed",
			input:    "CREATE TABLE test (col String) ENGINE = MergeTree();",
			expected: "CREATE TABLE test (col String) ENGINE = MergeTree();",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := normalizeDataTypesInDDL(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCleanCreateStatement(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "Add missing semicolon and format via AST",
			input:    "CREATE TABLE test (id UInt64) ENGINE = MergeTree()",
			expected: "CREATE TABLE `test` (\n    `id` UInt64\n)\nENGINE = MergeTree();",
		},
		{
			name:     "Parse and reformat for consistency",
			input:    "CREATE TABLE test (id UInt64) ENGINE = MergeTree();",
			expected: "CREATE TABLE `test` (\n    `id` UInt64\n)\nENGINE = MergeTree();",
		},
		{
			name:     "Complex AggregateFunction should be preserved",
			input:    "CREATE TABLE test (col AggregateFunction(quantiles(0.5, 0.75, 0.9), Float64)) ENGINE = MergeTree();",
			expected: "CREATE TABLE `test` (\n    `col` AggregateFunction(quantiles(0.5, 0.75, 0.9), Float64)\n)\nENGINE = MergeTree();",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cleanCreateStatement(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestBuildDatabaseExclusion(t *testing.T) {
	tests := []struct {
		name            string
		columnName      string
		ignoreDatabases []string
		expectedQuery   string
		expectedParams  []any
	}{
		{
			name:            "only system databases",
			columnName:      "database",
			ignoreDatabases: []string{},
			expectedQuery:   "database NOT IN (?, ?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "housekeeper"},
		},
		{
			name:            "system and user databases",
			columnName:      "name",
			ignoreDatabases: []string{"testing_db", "temp_db"},
			expectedQuery:   "name NOT IN (?, ?, ?, ?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "housekeeper", "testing_db", "temp_db"},
		},
		{
			name:            "single ignored database",
			columnName:      "database",
			ignoreDatabases: []string{"staging"},
			expectedQuery:   "database NOT IN (?, ?, ?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "housekeeper", "staging"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, params := buildDatabaseExclusion(tt.columnName, tt.ignoreDatabases)
			require.Equal(t, tt.expectedQuery, query)
			require.Equal(t, tt.expectedParams, params)
		})
	}
}
