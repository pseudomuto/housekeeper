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
