package utils

import (
	"strconv"
	"strings"
)

// IsNumericValue checks if a string represents a valid numeric value.
// This uses strconv.ParseFloat to properly validate numeric formats,
// including integers, floats, and scientific notation.
//
// Examples:
//   - "123" -> true
//   - "123.45" -> true
//   - "-123.45" -> true
//   - "1.23e-4" -> true (scientific notation)
//   - "1.23E+5" -> true (scientific notation)
//   - "abc" -> false
//   - "1.2.3" -> false (multiple decimal points)
//   - "" -> false
//   - "." -> false
//   - "-" -> false
func IsNumericValue(value string) bool {
	if value == "" {
		return false
	}

	_, err := strconv.ParseFloat(value, 64)
	return err == nil
}

// IsBooleanValue checks if a string represents a boolean value.
// This is case-insensitive and supports various boolean representations.
//
// Examples:
//   - "true" -> true
//   - "TRUE" -> true
//   - "True" -> true
//   - "false" -> true
//   - "FALSE" -> true
//   - "1" -> false (use IsNumericValue for numeric booleans)
//   - "yes" -> false
//   - "" -> false
func IsBooleanValue(value string) bool {
	lowered := strings.ToLower(value)
	return lowered == "true" || lowered == "false"
}
