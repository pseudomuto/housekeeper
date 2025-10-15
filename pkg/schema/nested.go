package schema

import (
	"maps"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// FlattenNestedColumns creates a copy of TableInfo with Nested columns converted to dotted Array columns.
// This is used for accurate schema comparison since ClickHouse internally represents Nested columns
// as separate Array columns with dotted names.
//
// Example transformation:
//
//	profile Nested(name String, age UInt8)
//	becomes:
//	profile.name Array(String), profile.age Array(UInt8)
//
// The original TableInfo is not modified; a new copy is returned.
// Properties like DEFAULT, TTL, CODEC, and COMMENT from the original Nested column
// are preserved on each flattened Array column.
func FlattenNestedColumns(table *TableInfo) *TableInfo {
	if table == nil {
		return nil
	}

	// Create a copy of the table
	flattened := &TableInfo{
		Name:          table.Name,
		Database:      table.Database,
		Engine:        table.Engine,
		Cluster:       table.Cluster,
		Comment:       table.Comment,
		OrderBy:       table.OrderBy,
		PartitionBy:   table.PartitionBy,
		PrimaryKey:    table.PrimaryKey,
		SampleBy:      table.SampleBy,
		TTL:           table.TTL,
		Settings:      copyStringMap(table.Settings),
		OrReplace:     table.OrReplace,
		IfNotExists:   table.IfNotExists,
		AsSourceTable: table.AsSourceTable,
		AsDependents:  copyBoolMap(table.AsDependents),
		Columns:       make([]ColumnInfo, 0, len(table.Columns)),
	}

	// Process each column
	for _, col := range table.Columns {
		if col.DataType != nil && col.DataType.Nested != nil {
			// Convert Nested column to multiple dotted Array columns
			flattenedCols := ConvertNestedToFlattened(col.DataType.Nested, col.Name, col)
			flattened.Columns = append(flattened.Columns, flattenedCols...)
		} else {
			// Regular column - copy as-is
			flattened.Columns = append(flattened.Columns, col)
		}
	}

	return flattened
}

// ConvertNestedToFlattened converts a Nested data type to multiple Array columns with dotted names.
// This function takes a ClickHouse Nested type definition and expands it into the equivalent
// flattened representation that ClickHouse uses internally.
//
// Parameters:
//   - nested: The parsed Nested type from the DDL
//   - prefix: The column name that will be used as the prefix for dotted names
//   - originalCol: The original column info to preserve properties like DEFAULT, TTL, etc.
//
// Example transformation:
//
//	Nested(name String, age UInt8) with prefix "profile"
//	becomes:
//	profile.name Array(String), profile.age Array(UInt8)
//
// Properties from the original column (DEFAULT, TTL, CODEC, COMMENT) are preserved
// on each flattened column, maintaining consistency with the original definition.
//
// Returns a slice of ColumnInfo representing the flattened Array columns.
func ConvertNestedToFlattened(nested *parser.NestedType, prefix string, originalCol ColumnInfo) []ColumnInfo {
	if nested == nil {
		return nil
	}

	columns := make([]ColumnInfo, 0, len(nested.Columns))

	for _, nestedCol := range nested.Columns {
		// Create dotted column name
		dottedName := prefix + "." + nestedCol.Name

		// Wrap the nested column type in Array()
		arrayType := &parser.DataType{
			Array: &parser.ArrayType{
				Array: "Array",
				Type:  nestedCol.Type,
				Close: ")",
			},
		}

		column := ColumnInfo{
			Name:        dottedName,
			DataType:    arrayType,
			DefaultType: originalCol.DefaultType,
			Default:     originalCol.Default,
			Codec:       originalCol.Codec,
			TTL:         originalCol.TTL,
			Comment:     originalCol.Comment,
		}

		columns = append(columns, column)
	}

	return columns
}

// DetectNestedGroups identifies groups of dotted columns that could potentially represent
// flattened Nested structures. This function analyzes a set of columns and groups together
// those that appear to be flattened representations of Nested types.
//
// The function looks for columns with names containing dots where:
//   - Multiple columns share the same prefix (before the first dot)
//   - All columns in the group have Array data types
//   - Groups with only one column are filtered out (unlikely to be Nested)
//
// This is useful for:
//   - Validation and debugging of flattened schemas
//   - Reverse engineering original Nested structure from ClickHouse output
//   - Schema analysis and documentation
//
// Parameters:
//   - columns: A slice of ColumnInfo to analyze for potential Nested groupings
//
// Returns a map where:
//   - Key: The prefix name (e.g., "profile" for "profile.name", "profile.age")
//   - Value: A slice of ColumnInfo representing the potential Nested fields
//
// Example:
//
//	Input: [`profile.name` Array(String), `profile.age` Array(UInt8), `metadata.key` Array(String)]
//	Output: {"profile": [profile.name, profile.age]} (metadata.key ignored - only one column)
func DetectNestedGroups(columns []ColumnInfo) map[string][]ColumnInfo {
	groups := make(map[string][]ColumnInfo)

	for _, col := range columns {
		// Check if column name contains a dot
		if dotIndex := findFirstDot(col.Name); dotIndex != -1 {
			prefix := col.Name[:dotIndex]

			// Only consider Array columns as potential Nested fields
			if col.DataType != nil && col.DataType.Array != nil {
				groups[prefix] = append(groups[prefix], col)
			}
		}
	}

	// Filter out groups with only one column (unlikely to be Nested)
	filtered := make(map[string][]ColumnInfo)
	for prefix, cols := range groups {
		if len(cols) > 1 {
			filtered[prefix] = cols
		}
	}

	return filtered
}

// findFirstDot returns the index of the first dot in the string, or -1 if not found.
func findFirstDot(s string) int {
	for i, r := range s {
		if r == '.' {
			return i
		}
	}
	return -1
}

// copyStringMap creates a deep copy of a string map
func copyStringMap(m map[string]string) map[string]string {
	if m == nil {
		return nil
	}
	copy := make(map[string]string, len(m))
	maps.Copy(copy, m)
	return copy
}

// copyBoolMap creates a deep copy of a bool map
func copyBoolMap(m map[string]bool) map[string]bool {
	if m == nil {
		return nil
	}
	copy := make(map[string]bool, len(m))
	maps.Copy(copy, m)
	return copy
}
