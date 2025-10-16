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
