package clickhouse

import (
	"testing"

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
			expectedQuery:  "database NOT IN (?, ?, ?, ?)",
			expectedParams: []any{"default", "system", "information_schema", "INFORMATION_SCHEMA"},
		},
		{
			name:           "name column",
			columnName:     "name",
			expectedQuery:  "name NOT IN (?, ?, ?, ?)",
			expectedParams: []any{"default", "system", "information_schema", "INFORMATION_SCHEMA"},
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
			expectedQuery:   "database NOT IN (?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA"},
		},
		{
			name:            "system and user databases",
			columnName:      "name",
			ignoreDatabases: []string{"testing_db", "temp_db"},
			expectedQuery:   "name NOT IN (?, ?, ?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "testing_db", "temp_db"},
		},
		{
			name:            "single ignored database",
			columnName:      "database",
			ignoreDatabases: []string{"staging"},
			expectedQuery:   "database NOT IN (?, ?, ?, ?, ?)",
			expectedParams:  []any{"default", "system", "information_schema", "INFORMATION_SCHEMA", "staging"},
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
