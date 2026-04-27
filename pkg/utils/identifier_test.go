package utils_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestBacktickIdentifier(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "simple identifier",
			input:    "table",
			expected: "`table`",
		},
		{
			name:     "qualified identifier with two parts",
			input:    "database.table",
			expected: "`database`.`table`",
		},
		{
			name:     "qualified identifier with three parts",
			input:    "database.schema.table",
			expected: "`database`.`schema`.`table`",
		},
		{
			name:     "already backticked simple identifier",
			input:    "`table`",
			expected: "`table`",
		},
		{
			name:     "partially backticked qualified identifier",
			input:    "`database`.table",
			expected: "`database`.`table`",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "identifier with spaces",
			input:    "my table",
			expected: "`my table`",
		},
		{
			name:     "identifier with special characters",
			input:    "table-name",
			expected: "`table-name`",
		},
		{
			name:     "qualified identifier with spaces",
			input:    "my database.my table",
			expected: "`my database`.`my table`",
		},
		{
			name:     "already fully backticked qualified identifier",
			input:    "`database`.`table`",
			expected: "`database`.`table`",
		},
		{
			name:     "mixed backticks in qualified identifier",
			input:    "database.`table`",
			expected: "`database`.`table`",
		},
		{
			name:     "single character identifier",
			input:    "a",
			expected: "`a`",
		},
		{
			name:     "numeric identifier",
			input:    "123",
			expected: "`123`",
		},
		{
			name:     "identifier with dots in backticks",
			input:    "`db.table`",
			expected: "`db.table`", // Treat as already backticked single identifier
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.BacktickIdentifier(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestBacktickQualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		database *string
		table    string
		expected string
	}{
		{
			name:     "with database",
			database: stringPtr("analytics"),
			table:    "events",
			expected: "`analytics`.`events`",
		},
		{
			name:     "without database (nil)",
			database: nil,
			table:    "events",
			expected: "`events`",
		},
		{
			name:     "without database (empty string)",
			database: stringPtr(""),
			table:    "events",
			expected: "`events`",
		},
		{
			name:     "already backticked database",
			database: stringPtr("`analytics`"),
			table:    "events",
			expected: "`analytics`.`events`",
		},
		{
			name:     "already backticked table",
			database: stringPtr("analytics"),
			table:    "`events`",
			expected: "`analytics`.`events`",
		},
		{
			name:     "both already backticked",
			database: stringPtr("`analytics`"),
			table:    "`events`",
			expected: "`analytics`.`events`",
		},
		{
			name:     "database with special characters",
			database: stringPtr("my-database"),
			table:    "my_table",
			expected: "`my-database`.`my_table`",
		},
		{
			name:     "empty table name",
			database: stringPtr("analytics"),
			table:    "",
			expected: "`analytics`.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.BacktickQualifiedName(tt.database, tt.table)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsBackticked(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		{
			name:     "backticked identifier",
			input:    "`table`",
			expected: true,
		},
		{
			name:     "not backticked",
			input:    "table",
			expected: false,
		},
		{
			name:     "qualified backticked identifier",
			input:    "`database`.`table`",
			expected: false, // This is a qualified name, not a single backticked identifier
		},
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "single backtick",
			input:    "`",
			expected: false,
		},
		{
			name:     "mismatched backticks",
			input:    "`table",
			expected: false,
		},
		{
			name:     "backticks with content containing backticks",
			input:    "`ta`ble`",
			expected: false, // Contains backticks in the middle
		},
		{
			name:     "just two backticks",
			input:    "``",
			expected: true,
		},
		{
			name:     "backticked identifier with spaces",
			input:    "`my table`",
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.IsBackticked(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestStripBackticks(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{
			name:     "backticked identifier",
			input:    "`table`",
			expected: "table",
		},
		{
			name:     "not backticked",
			input:    "table",
			expected: "table",
		},
		{
			name:     "qualified backticked identifier",
			input:    "`database`.`table`",
			expected: "database.table",
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
		},
		{
			name:     "multiple backticks",
			input:    "``table``",
			expected: "table",
		},
		{
			name:     "backticks in the middle",
			input:    "ta`ble",
			expected: "table",
		},
		{
			name:     "mixed backticks",
			input:    "`database`.table.`column`",
			expected: "database.table.column",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.StripBackticks(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}

func TestIsClusterMacro(t *testing.T) {
	cases := []struct {
		input    string
		expected bool
	}{
		// ClickHouse server macro references — single-quoted braces
		{"'{cluster}'", true},
		{"'{shard}'", true},
		{"'{replica}'", true},
		// Any single-quoted value is treated as a macro by the parser convention
		{"'any-quoted-string'", true},
		// Plain identifiers must NOT be treated as macros
		{"default", false},
		{"my-cluster", false},
		{"`default`", false},
		// Edge cases
		{"", false},
		{"'", false},  // not a valid single-quoted string (no closing quote)
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.expected, utils.IsClusterMacro(tc.input))
		})
	}
}

func TestIsClickHouseMacro(t *testing.T) {
	cases := []struct {
		input    string
		expected bool
	}{
		// Proper ClickHouse server macro syntax: '{identifier}'
		{"'{cluster}'", true},
		{"'{shard}'", true},
		{"'{replica}'", true},
		{"'{my_macro}'", true},
		// Single-quoted strings without braces are NOT server macros
		{"'any-quoted-string'", false},
		{"'default'", false},
		// Plain identifiers are not macros
		{"default", false},
		{"my-cluster", false},
		{"`default`", false},
		// Malformed macro patterns
		{"'{}'", false},           // empty identifier
		{"'{123bad}'", false},     // identifier starts with digit
		{"'{cluster'", false},     // missing closing brace
		{"'{cluster}x'", false},   // trailing content after brace
		// Edge cases
		{"", false},
		{"'", false},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.expected, utils.IsClickHouseMacro(tc.input))
		})
	}
}

func TestFormatClusterName(t *testing.T) {
	cases := []struct {
		input    string
		expected string
	}{
		// Macro references must be returned verbatim (no extra quoting)
		{"'{cluster}'", "'{cluster}'"},
		{"'{shard}'", "'{shard}'"},
		// Plain identifiers must be backtick-quoted
		{"default", "`default`"},
		{"my-cluster", "`my-cluster`"},
		{"prod", "`prod`"},
		// Empty string — nothing to format
		{"", ""},
	}

	for _, tc := range cases {
		t.Run(tc.input, func(t *testing.T) {
			require.Equal(t, tc.expected, utils.FormatClusterName(tc.input))
		})
	}
}
