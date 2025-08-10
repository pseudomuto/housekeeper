package parser_test

import (
	"fmt"
	"testing"

	"github.com/pkg/errors"
	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

//nolint:maintidx // Comprehensive test function covers all column parsing scenarios
func TestColumnParsing(t *testing.T) {
	// Helper function to parse a column definition via ParseString
	parseColumn := func(colDef string) (*Column, error) {
		// Wrap column definition in a CREATE TABLE statement
		sql := fmt.Sprintf("CREATE TABLE test (%s) ENGINE = Memory();", colDef)
		sqlResult, err := ParseString(sql)
		if err != nil {
			return nil, err
		}

		// Extract the column from the parsed statement
		if len(sqlResult.Statements) > 0 &&
			sqlResult.Statements[0].CreateTable != nil &&
			len(sqlResult.Statements[0].CreateTable.Elements) > 0 &&
			sqlResult.Statements[0].CreateTable.Elements[0].Column != nil {
			return sqlResult.Statements[0].CreateTable.Elements[0].Column, nil
		}

		return nil, errors.New("no column found in parsed SQL")
	}

	tests := []struct {
		name     string
		input    string
		validate func(*testing.T, *Column)
	}{
		{
			name:  "simple column",
			input: "user_id UInt64",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "user_id", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "UInt64", col.DataType.Simple.Name)
			},
		},
		{
			name:  "column with default",
			input: "name String DEFAULT 'Anonymous'",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "name", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "String", col.DataType.Simple.Name)
				defaultClause := col.GetDefault()
				require.NotNil(t, defaultClause)
				require.Equal(t, "DEFAULT", defaultClause.Type)
				require.NotNil(t, defaultClause.Expression.Or)
			},
		},
		{
			name:  "column with codec",
			input: "data String CODEC(ZSTD)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				codecClause := col.GetCodec()
				require.NotNil(t, codecClause)
				require.Len(t, codecClause.Codecs, 1)
				require.Equal(t, "ZSTD", codecClause.Codecs[0].Name)
				require.Empty(t, codecClause.Codecs[0].Parameters)
			},
		},
		{
			name:  "column with multiple codecs",
			input: "data String CODEC(Delta, ZSTD)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				codecClause := col.GetCodec()
				require.NotNil(t, codecClause)
				require.Len(t, codecClause.Codecs, 2)
				require.Equal(t, "Delta", codecClause.Codecs[0].Name)
				require.Equal(t, "ZSTD", codecClause.Codecs[1].Name)
				require.Empty(t, codecClause.Codecs[0].Parameters)
				require.Empty(t, codecClause.Codecs[1].Parameters)
			},
		},
		{
			name:  "column with comment",
			input: "age UInt8 COMMENT 'User age'",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "age", col.Name)
				comment := col.GetComment()
				require.NotNil(t, comment)
				require.Equal(t, "'User age'", *comment)
			},
		},
		{
			name:  "nullable column",
			input: "email Nullable(String)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "email", col.Name)
				require.NotNil(t, col.DataType.Nullable)
				require.NotNil(t, col.DataType.Nullable.Type.Simple)
				require.Equal(t, "String", col.DataType.Nullable.Type.Simple.Name)
			},
		},
		{
			name:  "array column",
			input: "tags Array(String)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "tags", col.Name)
				require.NotNil(t, col.DataType.Array)
				require.NotNil(t, col.DataType.Array.Type.Simple)
				require.Equal(t, "String", col.DataType.Array.Type.Simple.Name)
			},
		},
		{
			name:  "nested array",
			input: "matrix Array(Array(Float64))",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "matrix", col.Name)
				require.NotNil(t, col.DataType.Array)
				require.NotNil(t, col.DataType.Array.Type.Array)
				require.NotNil(t, col.DataType.Array.Type.Array.Type.Simple)
				require.Equal(t, "Float64", col.DataType.Array.Type.Array.Type.Simple.Name)
			},
		},
		{
			name:  "tuple column",
			input: "point Tuple(Float64, Float64)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "point", col.Name)
				require.NotNil(t, col.DataType.Tuple)
				require.Len(t, col.DataType.Tuple.Elements, 2)
				// For unnamed tuples, check UnnamedType instead of Type
				require.Nil(t, col.DataType.Tuple.Elements[0].Name)
				require.Nil(t, col.DataType.Tuple.Elements[0].Type)
				require.NotNil(t, col.DataType.Tuple.Elements[0].UnnamedType)
				require.Equal(t, "Float64", col.DataType.Tuple.Elements[0].UnnamedType.Simple.Name)
			},
		},
		{
			name:  "named tuple column",
			input: "coordinates Tuple(lat Float64, lon Float64)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "coordinates", col.Name)
				require.NotNil(t, col.DataType.Tuple)
				require.Len(t, col.DataType.Tuple.Elements, 2)
				require.NotNil(t, col.DataType.Tuple.Elements[0].Name)
				require.Equal(t, "lat", *col.DataType.Tuple.Elements[0].Name)
				require.Equal(t, "Float64", col.DataType.Tuple.Elements[0].Type.Simple.Name)
			},
		},
		{
			name:  "nested column",
			input: "metadata Nested(key String, value String)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "metadata", col.Name)
				require.NotNil(t, col.DataType.Nested)
				require.Len(t, col.DataType.Nested.Columns, 2)
				require.Equal(t, "key", col.DataType.Nested.Columns[0].Name)
				require.Equal(t, "String", col.DataType.Nested.Columns[0].Type.Simple.Name)
			},
		},
		{
			name:  "map column",
			input: "settings Map(String, String)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "settings", col.Name)
				require.NotNil(t, col.DataType.Map)
				require.Equal(t, "String", col.DataType.Map.KeyType.Simple.Name)
				require.Equal(t, "String", col.DataType.Map.ValueType.Simple.Name)
			},
		},
		{
			name:  "low cardinality column",
			input: "status LowCardinality(String)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "status", col.Name)
				require.NotNil(t, col.DataType.LowCardinality)
				require.Equal(t, "String", col.DataType.LowCardinality.Type.Simple.Name)
			},
		},
		{
			name:  "parametric type",
			input: "name FixedString(50)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "name", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "FixedString", col.DataType.Simple.Name)
				require.Len(t, col.DataType.Simple.Parameters, 1)
				require.NotNil(t, col.DataType.Simple.Parameters[0].Number)
				require.Equal(t, "50", *col.DataType.Simple.Parameters[0].Number)
			},
		},
		{
			name:  "decimal type",
			input: "amount Decimal(10, 2)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "amount", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "Decimal", col.DataType.Simple.Name)
				require.Len(t, col.DataType.Simple.Parameters, 2)
				require.Equal(t, "10", *col.DataType.Simple.Parameters[0].Number)
				require.Equal(t, "2", *col.DataType.Simple.Parameters[1].Number)
			},
		},
		{
			name:  "datetime with timezone",
			input: "created_at DateTime('UTC')",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "created_at", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "DateTime", col.DataType.Simple.Name)
				require.Len(t, col.DataType.Simple.Parameters, 1)
				require.NotNil(t, col.DataType.Simple.Parameters[0].String)
				require.Equal(t, "'UTC'", *col.DataType.Simple.Parameters[0].String)
			},
		},
		{
			name:  "materialized column",
			input: "full_name String MATERIALIZED concat(first_name, ' ', last_name)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "full_name", col.Name)
				defaultClause := col.GetDefault()
				require.NotNil(t, defaultClause)
				require.Equal(t, "MATERIALIZED", defaultClause.Type)
				require.NotNil(t, defaultClause.Expression.Or)
			},
		},
		{
			name:  "alias column",
			input: "age_in_days UInt32 ALIAS dateDiff('day', birth_date, today())",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "age_in_days", col.Name)
				defaultClause := col.GetDefault()
				require.NotNil(t, defaultClause)
				require.Equal(t, "ALIAS", defaultClause.Type)
			},
		},
		{
			name:  "column with TTL",
			input: "temp_data String TTL created_at + days(1)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "temp_data", col.Name)
				ttlClause := col.GetTTL()
				require.NotNil(t, ttlClause)
				require.NotNil(t, ttlClause.Expression.Or)
			},
		},
		{
			name:  "complex column with all options",
			input: "data Nullable(String) DEFAULT '' CODEC(ZSTD) TTL created_at + days(30) COMMENT 'User data'",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				require.NotNil(t, col.DataType.Nullable)
				require.NotNil(t, col.GetDefault())
				require.NotNil(t, col.GetCodec())
				require.NotNil(t, col.GetTTL())
				require.NotNil(t, col.GetComment())
			},
		},
		{
			name:  "backtick identifier",
			input: "`order` UInt32",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "`order`", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "UInt32", col.DataType.Simple.Name)
			},
		},
		{
			name:  "backtick identifier with special chars",
			input: "`user-name` String",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "`user-name`", col.Name)
				require.NotNil(t, col.DataType.Simple)
				require.Equal(t, "String", col.DataType.Simple.Name)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := parseColumn(tt.input)
			require.NoError(t, err)
			tt.validate(t, col)
		})
	}
}

func TestComplexNestedTypes(t *testing.T) {
	// Helper function to parse a column definition via ParseString
	parseColumn := func(colDef string) (*Column, error) {
		// Wrap column definition in a CREATE TABLE statement
		sql := fmt.Sprintf("CREATE TABLE test (%s) ENGINE = Memory();", colDef)
		sqlResult, err := ParseString(sql)
		if err != nil {
			return nil, err
		}

		// Extract the column from the parsed statement
		if len(sqlResult.Statements) > 0 &&
			sqlResult.Statements[0].CreateTable != nil &&
			len(sqlResult.Statements[0].CreateTable.Elements) > 0 &&
			sqlResult.Statements[0].CreateTable.Elements[0].Column != nil {
			return sqlResult.Statements[0].CreateTable.Elements[0].Column, nil
		}

		return nil, errors.New("no column found in parsed SQL")
	}

	tests := []struct {
		name  string
		input string
	}{
		{
			name:  "array of nullable",
			input: "values Array(Nullable(Float64))",
		},
		{
			name:  "nullable array",
			input: "values Nullable(Array(String))",
		},
		{
			name:  "map with complex value",
			input: "user_settings Map(String, Array(String))",
		},
		{
			name:  "nested tuple",
			input: "location Tuple(city String, coords Tuple(lat Float64, lon Float64))",
		},
		{
			name:  "array of tuples",
			input: "points Array(Tuple(x Float64, y Float64))",
		},
		{
			name:  "low cardinality nullable",
			input: "category LowCardinality(Nullable(String))",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := parseColumn(tt.input)
			require.NoError(t, err)
			require.NotNil(t, col)
		})
	}
}
