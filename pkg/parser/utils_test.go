package parser

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIsHousekeeperDatabaseUnit(t *testing.T) {
	tests := []struct {
		name     string
		database string
		expected bool
	}{
		{name: "housekeeper database", database: "housekeeper", expected: true},
		{name: "regular database", database: "analytics", expected: false},
		{name: "default database", database: "default", expected: false},
		{name: "system database", database: "system", expected: false},
		{name: "empty database", database: "", expected: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isHousekeeperDatabase(tt.database)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestGetDatabaseNameUnit(t *testing.T) {
	tests := []struct {
		name     string
		database *string
		expected string
	}{
		{name: "nil database", database: nil, expected: "default"},
		{name: "empty database", database: stringPtr(""), expected: "default"},
		{name: "explicit database", database: stringPtr("analytics"), expected: "analytics"},
		{name: "housekeeper database", database: stringPtr("housekeeper"), expected: "housekeeper"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDatabaseName(tt.database)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
