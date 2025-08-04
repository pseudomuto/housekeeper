package parser_test

import (
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/stretchr/testify/require"

	. "github.com/pseudomuto/housekeeper/pkg/parser"
)

// Test lexer and parser for column definitions
var (
	columnLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Comment", Pattern: `--[^\r\n]*`},
		{Name: "String", Pattern: `'([^'\\]|\\.)*'`},
		{Name: "Number", Pattern: `\d+(\.\d+)?`},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
		{Name: "Punct", Pattern: `[(),.;=+\-*/]`},
		{Name: "Whitespace", Pattern: `\s+`},
	})

	columnParser = participle.MustBuild[Column](
		participle.Lexer(columnLexer),
		participle.Elide("Comment", "Whitespace"),
		participle.CaseInsensitive("DEFAULT", "MATERIALIZED", "EPHEMERAL", "ALIAS", "CODEC", "TTL",
			"COMMENT", "NULLABLE", "ARRAY", "TUPLE", "NESTED", "MAP", "LOWCARDINALITY",
			"PRIMARY", "ORDER", "PARTITION", "SAMPLE", "SETTINGS", "ENGINE", "BY", "INTERVAL"),
	)
)

func TestColumnParsing(t *testing.T) {
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
				require.NotNil(t, col.Default)
				require.Equal(t, "DEFAULT", col.Default.Type)
				require.NotNil(t, col.Default.Expression.Or)
			},
		},
		{
			name:  "column with codec",
			input: "data String CODEC(ZSTD)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				require.NotNil(t, col.Codec)
				require.Len(t, col.Codec.Codecs, 1)
				require.Equal(t, "ZSTD", col.Codec.Codecs[0].Name)
				require.Empty(t, col.Codec.Codecs[0].Parameters)
			},
		},
		{
			name:  "column with multiple codecs",
			input: "data String CODEC(Delta, ZSTD)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				require.NotNil(t, col.Codec)
				require.Len(t, col.Codec.Codecs, 2)
				require.Equal(t, "Delta", col.Codec.Codecs[0].Name)
				require.Equal(t, "ZSTD", col.Codec.Codecs[1].Name)
				require.Empty(t, col.Codec.Codecs[0].Parameters)
				require.Empty(t, col.Codec.Codecs[1].Parameters)
			},
		},
		{
			name:  "column with comment",
			input: "age UInt8 COMMENT 'User age'",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "age", col.Name)
				require.NotNil(t, col.Comment)
				require.Equal(t, "'User age'", *col.Comment)
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
				require.NotNil(t, col.Default)
				require.Equal(t, "MATERIALIZED", col.Default.Type)
				require.NotNil(t, col.Default.Expression.Or)
			},
		},
		{
			name:  "alias column",
			input: "age_in_days UInt32 ALIAS dateDiff('day', birth_date, today())",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "age_in_days", col.Name)
				require.NotNil(t, col.Default)
				require.Equal(t, "ALIAS", col.Default.Type)
			},
		},
		{
			name:  "column with TTL",
			input: "temp_data String TTL created_at + days(1)",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "temp_data", col.Name)
				require.NotNil(t, col.TTL)
				require.NotNil(t, col.TTL.Expression.Or)
			},
		},
		{
			name:  "complex column with all options",
			input: "data Nullable(String) DEFAULT '' CODEC(ZSTD) TTL created_at + days(30) COMMENT 'User data'",
			validate: func(t *testing.T, col *Column) {
				require.Equal(t, "data", col.Name)
				require.NotNil(t, col.DataType.Nullable)
				require.NotNil(t, col.Default)
				require.NotNil(t, col.Codec)
				require.NotNil(t, col.TTL)
				require.NotNil(t, col.Comment)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			col, err := columnParser.ParseString("", tt.input)
			require.NoError(t, err)
			tt.validate(t, col)
		})
	}
}

func TestComplexNestedTypes(t *testing.T) {
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
			col, err := columnParser.ParseString("", tt.input)
			require.NoError(t, err, "Failed to parse: %s", tt.input)
			require.NotNil(t, col)
		})
	}
}
