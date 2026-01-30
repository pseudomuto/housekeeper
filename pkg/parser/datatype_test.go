package parser_test

import (
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestTypeParameter(t *testing.T) {
	t.Parallel()

	t.Run("formatTypeParameter", func(t *testing.T) {
		t.Parallel()

		// Note: formatTypeParameter is unexported, so we test via SimpleType.String()
		tests := []struct {
			name     string
			simple   SimpleType
			expected string
		}{
			{
				name:     "number parameter",
				simple:   SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("10")}}},
				expected: "Decimal(10)",
			},
			{
				name:     "string parameter",
				simple:   SimpleType{Name: "DateTime", Parameters: []TypeParameter{{String: utils.Ptr("'UTC'")}}},
				expected: "DateTime('UTC')",
			},
			{
				name:     "ident parameter",
				simple:   SimpleType{Name: "Enum8", Parameters: []TypeParameter{{Ident: utils.Ptr("active")}}},
				expected: "Enum8(active)",
			},
			{
				name: "enum value parameter",
				simple: SimpleType{
					Name: "Enum8",
					Parameters: []TypeParameter{
						{EnumValue: &EnumValue{Name: "'active'", Value: "1"}},
						{EnumValue: &EnumValue{Name: "'inactive'", Value: "0"}},
					},
				},
				expected: "Enum8('active' = 1, 'inactive' = 0)",
			},
			{
				name: "function parameter",
				simple: SimpleType{
					Name: "AggregateFunction",
					Parameters: []TypeParameter{
						{Function: &ParametricFunction{Name: "quantiles", Parameters: []TypeParameter{{Number: utils.Ptr("0.5")}}}},
					},
				},
				expected: "AggregateFunction(quantiles(0.5))",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.simple.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			a, b     TypeParameter
			expected bool
		}{
			{
				name:     "same number",
				a:        TypeParameter{Number: utils.Ptr("10")},
				b:        TypeParameter{Number: utils.Ptr("10")},
				expected: true,
			},
			{
				name:     "different number",
				a:        TypeParameter{Number: utils.Ptr("10")},
				b:        TypeParameter{Number: utils.Ptr("20")},
				expected: false,
			},
			{
				name:     "number vs string",
				a:        TypeParameter{Number: utils.Ptr("10")},
				b:        TypeParameter{String: utils.Ptr("10")},
				expected: false,
			},
			{
				name:     "quoted string vs unquoted ident - same value",
				a:        TypeParameter{String: utils.Ptr("'UTC'")},
				b:        TypeParameter{Ident: utils.Ptr("UTC")},
				expected: true, // ClickHouse outputs 'UTC' but user may write UTC
			},
			{
				name:     "unquoted ident vs quoted string - same value",
				a:        TypeParameter{Ident: utils.Ptr("UTC")},
				b:        TypeParameter{String: utils.Ptr("'UTC'")},
				expected: true, // Symmetric with above
			},
			{
				name:     "quoted string vs unquoted ident - different value",
				a:        TypeParameter{String: utils.Ptr("'UTC'")},
				b:        TypeParameter{Ident: utils.Ptr("America/New_York")},
				expected: false,
			},
			{
				name:     "same ident",
				a:        TypeParameter{Ident: utils.Ptr("UTC")},
				b:        TypeParameter{Ident: utils.Ptr("UTC")},
				expected: true,
			},
			{
				name:     "same quoted string",
				a:        TypeParameter{String: utils.Ptr("'UTC'")},
				b:        TypeParameter{String: utils.Ptr("'UTC'")},
				expected: true,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.a.Equal(&tt.b))
			})
		}
	})
}

func TestSimpleType(t *testing.T) {
	t.Parallel()

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			simple   SimpleType
			expected string
		}{
			{
				name:     "no parameters",
				simple:   SimpleType{Name: "String"},
				expected: "String",
			},
			{
				name:     "single parameter",
				simple:   SimpleType{Name: "FixedString", Parameters: []TypeParameter{{Number: utils.Ptr("50")}}},
				expected: "FixedString(50)",
			},
			{
				name:     "multiple parameters",
				simple:   SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("10")}, {Number: utils.Ptr("2")}}},
				expected: "Decimal(10, 2)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.simple.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			a, b     SimpleType
			expected bool
		}{
			{
				name:     "same name no params",
				a:        SimpleType{Name: "String"},
				b:        SimpleType{Name: "String"},
				expected: true,
			},
			{
				name:     "different names",
				a:        SimpleType{Name: "String"},
				b:        SimpleType{Name: "UInt64"},
				expected: false,
			},
			{
				name:     "same params",
				a:        SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("10")}, {Number: utils.Ptr("2")}}},
				b:        SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("10")}, {Number: utils.Ptr("2")}}},
				expected: true,
			},
			{
				name:     "different params",
				a:        SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("10")}, {Number: utils.Ptr("2")}}},
				b:        SimpleType{Name: "Decimal", Parameters: []TypeParameter{{Number: utils.Ptr("18")}, {Number: utils.Ptr("4")}}},
				expected: false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.a.Equal(&tt.b))
			})
		}
	})

	t.Run("DateTime64Compatibility", func(t *testing.T) {
		t.Parallel()

		// DateTime64 has special handling for timezone normalization
		tests := []struct {
			name     string
			a, b     SimpleType
			expected bool
		}{
			{
				name:     "same precision and timezone",
				a:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {String: utils.Ptr("'UTC'")}}},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {String: utils.Ptr("'UTC'")}}},
				expected: true,
			},
			{
				name:     "precision only vs precision with timezone",
				a:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {String: utils.Ptr("'UTC'")}}},
				expected: true, // timezone normalization
			},
			{
				name:     "different precisions",
				a:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("6")}}},
				expected: false,
			},
			{
				name:     "DateTime with precision vs DateTime64 - ClickHouse normalization",
				a:        SimpleType{Name: "DateTime", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {Ident: utils.Ptr("UTC")}}},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}},
				expected: true, // ClickHouse normalizes DateTime(3, UTC) to DateTime64(3)
			},
			{
				name:     "DateTime64 vs DateTime with precision - symmetric",
				a:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}},
				b:        SimpleType{Name: "DateTime", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {Ident: utils.Ptr("UTC")}}},
				expected: true, // symmetric with above
			},
			{
				name:     "DateTime vs DateTime64 different precision",
				a:        SimpleType{Name: "DateTime", Parameters: []TypeParameter{{Number: utils.Ptr("3")}, {Ident: utils.Ptr("UTC")}}},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("6")}}},
				expected: false, // different precision
			},
			{
				name:     "plain DateTime without precision - not compatible",
				a:        SimpleType{Name: "DateTime"},
				b:        SimpleType{Name: "DateTime64", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}},
				expected: false, // plain DateTime is not the same as DateTime64
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.a.Equal(&tt.b))
			})
		}
	})
}

func TestWrapperTypes(t *testing.T) {
	t.Parallel()

	simpleString := &DataType{Simple: &SimpleType{Name: "String"}}
	simpleUInt64 := &DataType{Simple: &SimpleType{Name: "UInt64"}}

	t.Run("NullableType", func(t *testing.T) {
		t.Parallel()

		t.Run("String", func(t *testing.T) {
			t.Parallel()
			n := NullableType{Type: simpleString}
			require.Equal(t, "Nullable(String)", n.String())
		})

		t.Run("Equal", func(t *testing.T) {
			t.Parallel()
			a := NullableType{Type: simpleString}
			b := NullableType{Type: simpleString}
			c := NullableType{Type: simpleUInt64}
			require.True(t, a.Equal(&b))
			require.False(t, a.Equal(&c))
		})
	})

	t.Run("ArrayType", func(t *testing.T) {
		t.Parallel()

		t.Run("String", func(t *testing.T) {
			t.Parallel()
			a := ArrayType{Type: simpleString}
			require.Equal(t, "Array(String)", a.String())
		})

		t.Run("Equal", func(t *testing.T) {
			t.Parallel()
			a := ArrayType{Type: simpleString}
			b := ArrayType{Type: simpleString}
			c := ArrayType{Type: simpleUInt64}
			require.True(t, a.Equal(&b))
			require.False(t, a.Equal(&c))
		})
	})

	t.Run("LowCardinalityType", func(t *testing.T) {
		t.Parallel()

		t.Run("String", func(t *testing.T) {
			t.Parallel()
			l := LowCardinalityType{Type: simpleString}
			require.Equal(t, "LowCardinality(String)", l.String())
		})

		t.Run("Equal", func(t *testing.T) {
			t.Parallel()
			a := LowCardinalityType{Type: simpleString}
			b := LowCardinalityType{Type: simpleString}
			c := LowCardinalityType{Type: simpleUInt64}
			require.True(t, a.Equal(&b))
			require.False(t, a.Equal(&c))
		})
	})

	t.Run("MapType", func(t *testing.T) {
		t.Parallel()

		t.Run("String", func(t *testing.T) {
			t.Parallel()
			m := MapType{KeyType: simpleString, ValueType: simpleUInt64}
			require.Equal(t, "Map(String, UInt64)", m.String())
		})

		t.Run("Equal", func(t *testing.T) {
			t.Parallel()
			a := MapType{KeyType: simpleString, ValueType: simpleUInt64}
			b := MapType{KeyType: simpleString, ValueType: simpleUInt64}
			c := MapType{KeyType: simpleUInt64, ValueType: simpleString}
			require.True(t, a.Equal(&b))
			require.False(t, a.Equal(&c))
		})
	})
}

func TestTupleType(t *testing.T) {
	t.Parallel()

	simpleFloat64 := &DataType{Simple: &SimpleType{Name: "Float64"}}
	simpleString := &DataType{Simple: &SimpleType{Name: "String"}}

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			tuple    TupleType
			expected string
		}{
			{
				name:     "empty tuple",
				tuple:    TupleType{},
				expected: "Tuple()",
			},
			{
				name: "unnamed tuple",
				tuple: TupleType{
					Elements: []TupleElement{
						{UnnamedType: simpleFloat64},
						{UnnamedType: simpleFloat64},
					},
				},
				expected: "Tuple(Float64, Float64)",
			},
			{
				name: "named tuple",
				tuple: TupleType{
					Elements: []TupleElement{
						{Name: utils.Ptr("lat"), Type: simpleFloat64},
						{Name: utils.Ptr("lon"), Type: simpleFloat64},
					},
				},
				expected: "Tuple(lat Float64, lon Float64)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.tuple.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		a := TupleType{Elements: []TupleElement{{Name: utils.Ptr("x"), Type: simpleFloat64}}}
		b := TupleType{Elements: []TupleElement{{Name: utils.Ptr("x"), Type: simpleFloat64}}}
		c := TupleType{Elements: []TupleElement{{Name: utils.Ptr("y"), Type: simpleFloat64}}}
		d := TupleType{Elements: []TupleElement{{Name: utils.Ptr("x"), Type: simpleString}}}

		require.True(t, a.Equal(&b))
		require.False(t, a.Equal(&c)) // different name
		require.False(t, a.Equal(&d)) // different type
	})
}

func TestNestedType(t *testing.T) {
	t.Parallel()

	simpleString := &DataType{Simple: &SimpleType{Name: "String"}}

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		n := NestedType{
			Columns: []NestedColumn{
				{Name: "key", Type: simpleString},
				{Name: "value", Type: simpleString},
			},
		}
		require.Equal(t, "Nested(key String, value String)", n.String())
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		a := NestedType{Columns: []NestedColumn{{Name: "key", Type: simpleString}}}
		b := NestedType{Columns: []NestedColumn{{Name: "key", Type: simpleString}}}
		c := NestedType{Columns: []NestedColumn{{Name: "val", Type: simpleString}}}

		require.True(t, a.Equal(&b))
		require.False(t, a.Equal(&c))
	})
}

func TestDataType(t *testing.T) {
	t.Parallel()

	t.Run("String", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name     string
			dt       *DataType
			expected string
		}{
			{
				name:     "nil",
				dt:       nil,
				expected: "",
			},
			{
				name:     "simple type",
				dt:       &DataType{Simple: &SimpleType{Name: "String"}},
				expected: "String",
			},
			{
				name:     "nullable",
				dt:       &DataType{Nullable: &NullableType{Type: &DataType{Simple: &SimpleType{Name: "String"}}}},
				expected: "Nullable(String)",
			},
			{
				name:     "array",
				dt:       &DataType{Array: &ArrayType{Type: &DataType{Simple: &SimpleType{Name: "UInt64"}}}},
				expected: "Array(UInt64)",
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()
				require.Equal(t, tt.expected, tt.dt.String())
			})
		}
	})

	t.Run("Equal", func(t *testing.T) {
		t.Parallel()

		simpleA := &DataType{Simple: &SimpleType{Name: "String"}}
		simpleB := &DataType{Simple: &SimpleType{Name: "String"}}
		simpleC := &DataType{Simple: &SimpleType{Name: "UInt64"}}
		arrayA := &DataType{Array: &ArrayType{Type: simpleA}}

		require.True(t, simpleA.Equal(simpleB))
		require.False(t, simpleA.Equal(simpleC))
		require.False(t, simpleA.Equal(arrayA)) // different type kinds
		require.True(t, (*DataType)(nil).Equal(nil))
		require.False(t, simpleA.Equal(nil))
	})
}

func TestNormalizeDataType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          *DataType
		expectedName   string
		expectedParams []string
	}{
		{
			name:           "Decimal32",
			input:          &DataType{Simple: &SimpleType{Name: "Decimal32", Parameters: []TypeParameter{{Number: utils.Ptr("3")}}}},
			expectedName:   "Decimal",
			expectedParams: []string{"9", "3"},
		},
		{
			name:           "Decimal64",
			input:          &DataType{Simple: &SimpleType{Name: "Decimal64", Parameters: []TypeParameter{{Number: utils.Ptr("5")}}}},
			expectedName:   "Decimal",
			expectedParams: []string{"18", "5"},
		},
		{
			name:           "Decimal128",
			input:          &DataType{Simple: &SimpleType{Name: "Decimal128", Parameters: []TypeParameter{{Number: utils.Ptr("10")}}}},
			expectedName:   "Decimal",
			expectedParams: []string{"38", "10"},
		},
		{
			name:           "Decimal256",
			input:          &DataType{Simple: &SimpleType{Name: "Decimal256", Parameters: []TypeParameter{{Number: utils.Ptr("20")}}}},
			expectedName:   "Decimal",
			expectedParams: []string{"76", "20"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			NormalizeDataType(tt.input)

			require.Equal(t, tt.expectedName, tt.input.Simple.Name)
			require.Len(t, tt.input.Simple.Parameters, 2)
			require.Equal(t, tt.expectedParams[0], *tt.input.Simple.Parameters[0].Number)
			require.Equal(t, tt.expectedParams[1], *tt.input.Simple.Parameters[1].Number)
		})
	}
}
