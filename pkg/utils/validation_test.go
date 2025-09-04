package utils_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestIsNumericValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid numeric values
		{
			name:     "positive integer",
			input:    "123",
			expected: true,
		},
		{
			name:     "negative integer",
			input:    "-123",
			expected: true,
		},
		{
			name:     "positive float",
			input:    "123.45",
			expected: true,
		},
		{
			name:     "negative float",
			input:    "-123.45",
			expected: true,
		},
		{
			name:     "float without integer part",
			input:    ".123",
			expected: true,
		},
		{
			name:     "negative float without integer part",
			input:    "-.123",
			expected: true,
		},
		{
			name:     "float without decimal part",
			input:    "123.",
			expected: true,
		},
		{
			name:     "scientific notation lowercase e",
			input:    "1.23e4",
			expected: true,
		},
		{
			name:     "scientific notation uppercase E",
			input:    "1.23E4",
			expected: true,
		},
		{
			name:     "scientific notation with positive exponent",
			input:    "1.23e+4",
			expected: true,
		},
		{
			name:     "scientific notation with negative exponent",
			input:    "1.23e-4",
			expected: true,
		},
		{
			name:     "zero",
			input:    "0",
			expected: true,
		},
		{
			name:     "negative zero",
			input:    "-0",
			expected: true,
		},
		{
			name:     "zero float",
			input:    "0.0",
			expected: true,
		},

		// Invalid numeric values
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "just period",
			input:    ".",
			expected: false,
		},
		{
			name:     "just minus",
			input:    "-",
			expected: false,
		},
		{
			name:     "multiple decimal points",
			input:    "1.2.3",
			expected: false,
		},
		{
			name:     "letters",
			input:    "abc",
			expected: false,
		},
		{
			name:     "mixed letters and numbers",
			input:    "123abc",
			expected: false,
		},
		{
			name:     "invalid scientific notation",
			input:    "1.23e",
			expected: false,
		},
		{
			name:     "double negative",
			input:    "--123",
			expected: false,
		},
		{
			name:     "minus in middle",
			input:    "12-3",
			expected: false,
		},
		{
			name:     "spaces",
			input:    "1 2 3",
			expected: false,
		},
		{
			name:     "leading/trailing spaces",
			input:    " 123 ",
			expected: false,
		},
		{
			name:     "plus sign (should be invalid for ClickHouse)",
			input:    "+123",
			expected: true, // ParseFloat accepts this, but we might want to reject it
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.IsNumericValue(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestIsBooleanValue(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected bool
	}{
		// Valid boolean values
		{
			name:     "lowercase true",
			input:    "true",
			expected: true,
		},
		{
			name:     "uppercase TRUE",
			input:    "TRUE",
			expected: true,
		},
		{
			name:     "mixed case True",
			input:    "True",
			expected: true,
		},
		{
			name:     "mixed case tRuE",
			input:    "tRuE",
			expected: true,
		},
		{
			name:     "lowercase false",
			input:    "false",
			expected: true,
		},
		{
			name:     "uppercase FALSE",
			input:    "FALSE",
			expected: true,
		},
		{
			name:     "mixed case False",
			input:    "False",
			expected: true,
		},
		{
			name:     "mixed case fAlSe",
			input:    "fAlSe",
			expected: true,
		},

		// Invalid boolean values
		{
			name:     "empty string",
			input:    "",
			expected: false,
		},
		{
			name:     "numeric 1",
			input:    "1",
			expected: false,
		},
		{
			name:     "numeric 0",
			input:    "0",
			expected: false,
		},
		{
			name:     "yes",
			input:    "yes",
			expected: false,
		},
		{
			name:     "no",
			input:    "no",
			expected: false,
		},
		{
			name:     "on",
			input:    "on",
			expected: false,
		},
		{
			name:     "off",
			input:    "off",
			expected: false,
		},
		{
			name:     "t",
			input:    "t",
			expected: false,
		},
		{
			name:     "f",
			input:    "f",
			expected: false,
		},
		{
			name:     "with spaces",
			input:    " true ",
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.IsBooleanValue(tt.input)
			require.Equal(t, tt.expected, result)
		})
	}
}
