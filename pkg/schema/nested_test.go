package schema_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestFlattenNestedColumns(t *testing.T) {
	tests := []struct {
		name     string
		input    *schema.TableInfo
		expected *schema.TableInfo
	}{
		{
			name:     "nil input",
			input:    nil,
			expected: nil,
		},
		{
			name: "no nested columns",
			input: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "name",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "String"},
						},
					},
				},
			},
			expected: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "name",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "String"},
						},
					},
				},
			},
		},
		{
			name: "single nested column",
			input: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "profile",
						DataType: &parser.DataType{
							Nested: &parser.NestedType{
								Nested: "Nested",
								Columns: []parser.NestedColumn{
									{
										Name: "name",
										Type: &parser.DataType{
											Simple: &parser.SimpleType{Name: "String"},
										},
									},
									{
										Name: "age",
										Type: &parser.DataType{
											Simple: &parser.SimpleType{Name: "UInt8"},
										},
									},
								},
								Close: ")",
							},
						},
					},
				},
			},
			expected: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "profile.name",
						DataType: &parser.DataType{
							Array: &parser.ArrayType{
								Array: "Array",
								Type: &parser.DataType{
									Simple: &parser.SimpleType{Name: "String"},
								},
								Close: ")",
							},
						},
					},
					{
						Name: "profile.age",
						DataType: &parser.DataType{
							Array: &parser.ArrayType{
								Array: "Array",
								Type: &parser.DataType{
									Simple: &parser.SimpleType{Name: "UInt8"},
								},
								Close: ")",
							},
						},
					},
				},
			},
		},
		{
			name: "multiple nested columns",
			input: &schema.TableInfo{
				Name: "events",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "profile",
						DataType: &parser.DataType{
							Nested: &parser.NestedType{
								Nested: "Nested",
								Columns: []parser.NestedColumn{
									{
										Name: "name",
										Type: &parser.DataType{
											Simple: &parser.SimpleType{Name: "String"},
										},
									},
								},
								Close: ")",
							},
						},
					},
					{
						Name: "metadata",
						DataType: &parser.DataType{
							Nested: &parser.NestedType{
								Nested: "Nested",
								Columns: []parser.NestedColumn{
									{
										Name: "key",
										Type: &parser.DataType{
											Simple: &parser.SimpleType{Name: "String"},
										},
									},
									{
										Name: "value",
										Type: &parser.DataType{
											Simple: &parser.SimpleType{Name: "String"},
										},
									},
								},
								Close: ")",
							},
						},
					},
				},
			},
			expected: &schema.TableInfo{
				Name: "events",
				Columns: []schema.ColumnInfo{
					{
						Name: "id",
						DataType: &parser.DataType{
							Simple: &parser.SimpleType{Name: "UInt64"},
						},
					},
					{
						Name: "profile.name",
						DataType: &parser.DataType{
							Array: &parser.ArrayType{
								Array: "Array",
								Type: &parser.DataType{
									Simple: &parser.SimpleType{Name: "String"},
								},
								Close: ")",
							},
						},
					},
					{
						Name: "metadata.key",
						DataType: &parser.DataType{
							Array: &parser.ArrayType{
								Array: "Array",
								Type: &parser.DataType{
									Simple: &parser.SimpleType{Name: "String"},
								},
								Close: ")",
							},
						},
					},
					{
						Name: "metadata.value",
						DataType: &parser.DataType{
							Array: &parser.ArrayType{
								Array: "Array",
								Type: &parser.DataType{
									Simple: &parser.SimpleType{Name: "String"},
								},
								Close: ")",
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test the exported function directly
			result := schema.FlattenNestedColumns(tt.input)

			if tt.expected == nil {
				require.Nil(t, result)
				return
			}

			require.NotNil(t, result)
			require.Equal(t, tt.expected.Name, result.Name)
			require.Len(t, result.Columns, len(tt.expected.Columns))

			for i, expectedCol := range tt.expected.Columns {
				actualCol := result.Columns[i]
				require.Equal(t, expectedCol.Name, actualCol.Name, "column %d name mismatch", i)
				require.Equal(t, formatDataType(expectedCol.DataType), formatDataType(actualCol.DataType), "column %d type mismatch", i)
			}
		})
	}
}

func TestDetectNestedGroups(t *testing.T) {
	tests := []struct {
		name     string
		columns  []schema.ColumnInfo
		expected map[string][]schema.ColumnInfo
	}{
		{
			name: "no dotted columns",
			columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{Name: "name", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
			},
			expected: map[string][]schema.ColumnInfo{},
		},
		{
			name: "single column with dot (not a group)",
			columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
			},
			expected: map[string][]schema.ColumnInfo{},
		},
		{
			name: "multiple columns with same prefix",
			columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
				{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
			},
			expected: map[string][]schema.ColumnInfo{
				"profile": {
					{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
					{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
				},
			},
		},
		{
			name: "multiple prefixes with multiple columns each",
			columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
				{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
				{Name: "metadata.key", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
				{Name: "metadata.value", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
			},
			expected: map[string][]schema.ColumnInfo{
				"profile": {
					{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
					{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
				},
				"metadata": {
					{Name: "metadata.key", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
					{Name: "metadata.value", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
				},
			},
		},
		{
			name: "dotted columns that are not arrays (should be ignored)",
			columns: []schema.ColumnInfo{
				{Name: "profile.name", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}, // Not an array
				{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
			},
			expected: map[string][]schema.ColumnInfo{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := schema.DetectNestedGroups(tt.columns)

			require.Len(t, result, len(tt.expected))

			for prefix, expectedCols := range tt.expected {
				actualCols, exists := result[prefix]
				require.True(t, exists, "expected prefix %s not found", prefix)
				require.Len(t, actualCols, len(expectedCols), "wrong number of columns for prefix %s", prefix)

				for i, expectedCol := range expectedCols {
					require.Equal(t, expectedCol.Name, actualCols[i].Name, "column name mismatch for prefix %s", prefix)
				}
			}
		})
	}
}

func TestHasNestedColumns(t *testing.T) {
	tests := []struct {
		name     string
		table    *schema.TableInfo
		expected bool
	}{
		{
			name:     "nil table",
			table:    nil,
			expected: false,
		},
		{
			name: "table with no columns",
			table: &schema.TableInfo{
				Name:    "empty",
				Columns: []schema.ColumnInfo{},
			},
			expected: false,
		},
		{
			name: "table with only simple columns",
			table: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
					{Name: "name", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
				},
			},
			expected: false,
		},
		{
			name: "table with flattened columns (dotted arrays)",
			table: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
					{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
					{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}}}},
				},
			},
			expected: false,
		},
		{
			name: "table with nested column",
			table: &schema.TableInfo{
				Name: "users",
				Columns: []schema.ColumnInfo{
					{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
					{
						Name: "profile",
						DataType: &parser.DataType{
							Nested: &parser.NestedType{
								Nested: "Nested",
								Columns: []parser.NestedColumn{
									{Name: "name", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
									{Name: "age", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}},
								},
								Close: ")",
							},
						},
					},
				},
			},
			expected: true,
		},
		{
			name: "table with mixed columns including nested",
			table: &schema.TableInfo{
				Name: "events",
				Columns: []schema.ColumnInfo{
					{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
					{Name: "name", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
					{Name: "tags", DataType: &parser.DataType{Array: &parser.ArrayType{Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}}}},
					{
						Name: "metadata",
						DataType: &parser.DataType{
							Nested: &parser.NestedType{
								Nested: "Nested",
								Columns: []parser.NestedColumn{
									{Name: "key", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
									{Name: "value", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
								},
								Close: ")",
							},
						},
					},
				},
			},
			expected: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := schema.HasNestedColumns(tt.table)
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestMaybeFlattenNestedColumns(t *testing.T) {
	// Helper to create a simple table
	simpleTable := func(name string) *schema.TableInfo {
		return &schema.TableInfo{
			Name: name,
			Columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			},
		}
	}

	// Helper to create a table with Nested column
	nestedTable := func(name string) *schema.TableInfo {
		return &schema.TableInfo{
			Name: name,
			Columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{
					Name: "profile",
					DataType: &parser.DataType{
						Nested: &parser.NestedType{
							Nested: "Nested",
							Columns: []parser.NestedColumn{
								{Name: "name", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
								{Name: "age", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}},
							},
							Close: ")",
						},
					},
				},
			},
		}
	}

	// Helper to create a table with flattened columns (what ClickHouse returns with flatten_nested=1)
	flattenedTable := func(name string) *schema.TableInfo {
		return &schema.TableInfo{
			Name: name,
			Columns: []schema.ColumnInfo{
				{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
				{Name: "profile.name", DataType: &parser.DataType{Array: &parser.ArrayType{Array: "Array", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}, Close: ")"}}},
				{Name: "profile.age", DataType: &parser.DataType{Array: &parser.ArrayType{Array: "Array", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt8"}}, Close: ")"}}},
			},
		}
	}

	tests := []struct {
		name            string
		currentTable    *schema.TableInfo
		targetTable     *schema.TableInfo
		expectFlattened bool // true if result should have flattened columns
	}{
		{
			name:            "current has Nested - don't flatten target",
			currentTable:    nestedTable("current"),
			targetTable:     nestedTable("target"),
			expectFlattened: false,
		},
		{
			name:            "current has flattened - flatten target",
			currentTable:    flattenedTable("current"),
			targetTable:     nestedTable("target"),
			expectFlattened: true,
		},
		{
			name:            "current is simple - flatten target with nested",
			currentTable:    simpleTable("current"),
			targetTable:     nestedTable("target"),
			expectFlattened: true,
		},
		{
			name:            "both simple tables - no change needed",
			currentTable:    simpleTable("current"),
			targetTable:     simpleTable("target"),
			expectFlattened: false, // No nested columns to flatten
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := schema.MaybeFlattenNestedColumns(tt.currentTable, tt.targetTable)

			require.NotNil(t, result)

			// Check if result has flattened columns (dotted names with Array type)
			hasFlattened := false
			hasNested := false
			for _, col := range result.Columns {
				if col.DataType != nil && col.DataType.Nested != nil {
					hasNested = true
				}
				// Check for dotted column names which indicate flattening
				for _, c := range col.Name {
					if c == '.' {
						hasFlattened = true
						break
					}
				}
			}

			if tt.expectFlattened {
				// Should have flattened columns (dotted names) and no Nested
				require.True(t, hasFlattened || !schema.HasNestedColumns(tt.targetTable),
					"expected flattened columns")
				require.False(t, hasNested, "should not have Nested columns after flattening")
			} else {
				// Should preserve original structure
				if schema.HasNestedColumns(tt.targetTable) {
					require.True(t, hasNested, "should preserve Nested columns")
				}
			}
		})
	}
}

func TestMaybeFlattenNestedColumns_PreservesIdentityWhenCurrentHasNested(t *testing.T) {
	// When current table has Nested columns (flatten_nested=0),
	// the target should be returned as-is without flattening

	targetTable := &schema.TableInfo{
		Name: "target",
		Columns: []schema.ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{
				Name: "visual_matches",
				DataType: &parser.DataType{
					Nested: &parser.NestedType{
						Nested: "Nested",
						Columns: []parser.NestedColumn{
							{Name: "start", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
							{Name: "end", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
							{Name: "word", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
							{Name: "snippet", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
						},
						Close: ")",
					},
				},
			},
		},
	}

	currentTable := &schema.TableInfo{
		Name: "current",
		Columns: []schema.ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{
				Name: "visual_matches",
				DataType: &parser.DataType{
					Nested: &parser.NestedType{
						Nested: "Nested",
						Columns: []parser.NestedColumn{
							{Name: "start", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
							{Name: "end", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
							{Name: "word", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
							{Name: "snippet", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "String"}}},
						},
						Close: ")",
					},
				},
			},
		},
	}

	result := schema.MaybeFlattenNestedColumns(currentTable, targetTable)

	// Result should be the same as targetTable (not flattened)
	require.Equal(t, targetTable, result, "should return target as-is when current has Nested columns")
	require.Len(t, result.Columns, 2, "should have 2 columns (not flattened to 5)")
	require.Equal(t, "visual_matches", result.Columns[1].Name, "should keep Nested column name")
	require.NotNil(t, result.Columns[1].DataType.Nested, "should keep Nested type")
}

func TestMaybeFlattenNestedColumns_FlattensWhenCurrentHasFlattened(t *testing.T) {
	// When current table has flattened columns (flatten_nested=1 default),
	// the target should be flattened to match

	targetTable := &schema.TableInfo{
		Name: "target",
		Columns: []schema.ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{
				Name: "visual_matches",
				DataType: &parser.DataType{
					Nested: &parser.NestedType{
						Nested: "Nested",
						Columns: []parser.NestedColumn{
							{Name: "start", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
							{Name: "end", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}},
						},
						Close: ")",
					},
				},
			},
		},
	}

	// Current table as returned by ClickHouse with flatten_nested=1
	currentTable := &schema.TableInfo{
		Name: "current",
		Columns: []schema.ColumnInfo{
			{Name: "id", DataType: &parser.DataType{Simple: &parser.SimpleType{Name: "UInt64"}}},
			{Name: "visual_matches.start", DataType: &parser.DataType{Array: &parser.ArrayType{Array: "Array", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}, Close: ")"}}},
			{Name: "visual_matches.end", DataType: &parser.DataType{Array: &parser.ArrayType{Array: "Array", Type: &parser.DataType{Simple: &parser.SimpleType{Name: "Int32"}}, Close: ")"}}},
		},
	}

	result := schema.MaybeFlattenNestedColumns(currentTable, targetTable)

	// Result should be flattened
	require.NotEqual(t, targetTable, result, "should return flattened copy")
	require.Len(t, result.Columns, 3, "should have 3 columns (flattened from 2)")
	require.Equal(t, "id", result.Columns[0].Name)
	require.Equal(t, "visual_matches.start", result.Columns[1].Name)
	require.Equal(t, "visual_matches.end", result.Columns[2].Name)
	require.NotNil(t, result.Columns[1].DataType.Array, "should be Array type")
	require.NotNil(t, result.Columns[2].DataType.Array, "should be Array type")
}

// Helper function to format data types for comparison
func formatDataType(dt *parser.DataType) string {
	if dt == nil {
		return "nil"
	}
	if dt.Simple != nil {
		return dt.Simple.Name
	}
	if dt.Array != nil {
		return "Array(" + formatDataType(dt.Array.Type) + ")"
	}
	if dt.Nested != nil {
		return "Nested(...)"
	}
	return "unknown"
}
