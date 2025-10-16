package schema_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/stretchr/testify/require"
)

// TestNestedColumnPropertiesPreservation verifies that column properties
// like DEFAULT, TTL, CODEC, and COMMENT are correctly preserved when
// flattening Nested columns to dotted Array columns
func TestNestedColumnPropertiesPreservation(t *testing.T) {
	// Create a Nested column with properties (theoretical case)
	originalTable := &schema.TableInfo{
		Name: "test_table",
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
				DefaultType: "DEFAULT",
				Default:     nil, // Simplified for testing
				Comment:     "User profile data",
			},
		},
	}

	// Flatten the table
	flattened := schema.FlattenNestedColumns(originalTable)

	require.NotNil(t, flattened)
	require.Len(t, flattened.Columns, 3, "should have id + 2 flattened columns")

	// Verify regular column is unchanged
	require.Equal(t, "id", flattened.Columns[0].Name)
	require.Empty(t, flattened.Columns[0].Comment)

	// Verify flattened columns preserve properties
	profileNameCol := flattened.Columns[1]
	profileAgeCol := flattened.Columns[2]

	// Check names
	require.Equal(t, "profile.name", profileNameCol.Name)
	require.Equal(t, "profile.age", profileAgeCol.Name)

	// Check that properties from original Nested column are preserved
	require.Equal(t, "DEFAULT", profileNameCol.DefaultType)
	require.Equal(t, "DEFAULT", profileAgeCol.DefaultType)
	require.Equal(t, "User profile data", profileNameCol.Comment)
	require.Equal(t, "User profile data", profileAgeCol.Comment)
	require.Nil(t, profileNameCol.Default)
	require.Nil(t, profileAgeCol.Default)

	// Check that data types are correctly wrapped in Array()
	require.NotNil(t, profileNameCol.DataType.Array)
	require.NotNil(t, profileAgeCol.DataType.Array)
	require.Equal(t, "String", profileNameCol.DataType.Array.Type.Simple.Name)
	require.Equal(t, "UInt8", profileAgeCol.DataType.Array.Type.Simple.Name)
}

// TestNestedColumnWithoutProperties verifies that flattening works correctly
// when the original Nested column has no additional properties (the common case)
func TestNestedColumnWithoutProperties(t *testing.T) {
	originalTable := &schema.TableInfo{
		Name: "simple_table",
		Columns: []schema.ColumnInfo{
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
						},
						Close: ")",
					},
				},
				// No additional properties set (common case)
			},
		},
	}

	// Flatten the table
	flattened := schema.FlattenNestedColumns(originalTable)

	require.NotNil(t, flattened)
	require.Len(t, flattened.Columns, 1, "should have 1 flattened column")

	// Verify flattened column
	metadataKeyCol := flattened.Columns[0]
	require.Equal(t, "metadata.key", metadataKeyCol.Name)

	// Properties should be empty (zero values)
	require.Empty(t, metadataKeyCol.DefaultType)
	require.Nil(t, metadataKeyCol.Default)
	require.Nil(t, metadataKeyCol.Codec)
	require.Nil(t, metadataKeyCol.TTL)
	require.Empty(t, metadataKeyCol.Comment)

	// Data type should be correctly wrapped
	require.NotNil(t, metadataKeyCol.DataType.Array)
	require.Equal(t, "String", metadataKeyCol.DataType.Array.Type.Simple.Name)
}
