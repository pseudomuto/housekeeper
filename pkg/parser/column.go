package parser

import "github.com/pseudomuto/housekeeper/pkg/compare"

type (
	// Column represents a complete column definition in ClickHouse DDL.
	// It includes the column name, data type, and all possible modifiers
	// such as DEFAULT values, MATERIALIZED expressions, ALIAS definitions,
	// compression CODECs, TTL settings, and comments.
	Column struct {
		LeadingComments  []string          `parser:"@(Comment | MultilineComment)*"`
		Name             string            `parser:"@(Ident | BacktickIdent)"`
		DataType         *DataType         `parser:"@@"`
		Attributes       []ColumnAttribute `parser:"@@*"`
		TrailingComments []string          `parser:"@(Comment | MultilineComment)*"`
	}

	// ColumnAttribute represents any attribute that can appear after the data type
	// This allows attributes to be specified in any order
	ColumnAttribute struct {
		Default *DefaultClause `parser:"@@"`
		Codec   *CodecClause   `parser:"| @@"`
		TTL     *TTLClause     `parser:"| @@"`
		Comment *string        `parser:"| ('COMMENT' @String)"`
	}

	// DataType represents any ClickHouse data type including primitives,
	// parametric types, and complex types like arrays, tuples, and nested structures.
	DataType struct {
		// Nullable wrapper (e.g., Nullable(String))
		Nullable *NullableType `parser:"@@"`
		// Array types (e.g., Array(String))
		Array *ArrayType `parser:"| @@"`
		// Tuple types (e.g., Tuple(name String, age UInt8))
		Tuple *TupleType `parser:"| @@"`
		// Nested types (e.g., Nested(id UInt32, name String))
		Nested *NestedType `parser:"| @@"`
		// Map types (e.g., Map(String, UInt32))
		Map *MapType `parser:"| @@"`
		// LowCardinality wrapper (e.g., LowCardinality(String))
		LowCardinality *LowCardinalityType `parser:"| @@"`
		// Simple or parametric types (e.g., String, FixedString(10), Decimal(10,2))
		Simple *SimpleType `parser:"| @@"`
	}

	// NullableType represents Nullable(T) where T is any data type
	NullableType struct {
		Nullable string    `parser:"'Nullable' '('"`
		Type     *DataType `parser:"@@"`
		Close    string    `parser:"')'"`
	}

	// ArrayType represents Array(T) where T is any data type
	ArrayType struct {
		Array string    `parser:"'Array' '('"`
		Type  *DataType `parser:"@@"`
		Close string    `parser:"')'"`
	}

	// TupleType represents Tuple(T1, T2, ...) or named Tuple(name1 T1, name2 T2, ...)
	TupleType struct {
		Tuple    string         `parser:"'Tuple' '('"`
		Elements []TupleElement `parser:"@@ (',' @@)*"`
		Close    string         `parser:"')'"`
	}

	// TupleElement represents a single element in a tuple, which can be named or unnamed
	TupleElement struct {
		// Try to parse name + type first, then fall back to just type
		Name *string   `parser:"(@(Ident | BacktickIdent)"`
		Type *DataType `parser:"@@)"`
		// For unnamed tuples, we just have the type
		UnnamedType *DataType `parser:"| @@"`
	}

	// NestedType represents Nested(col1 Type1, col2 Type2, ...)
	NestedType struct {
		Nested  string         `parser:"'Nested' '('"`
		Columns []NestedColumn `parser:"@@ (',' @@)*"`
		Close   string         `parser:"')'"`
	}

	// NestedColumn represents a column within a Nested type
	NestedColumn struct {
		Name string    `parser:"@(Ident | BacktickIdent)"`
		Type *DataType `parser:"@@"`
	}

	// MapType represents Map(K, V) where K and V are data types
	MapType struct {
		Map       string    `parser:"'Map' '('"`
		KeyType   *DataType `parser:"@@"`
		Comma     string    `parser:"','"`
		ValueType *DataType `parser:"@@"`
		Close     string    `parser:"')'"`
	}

	// LowCardinalityType represents LowCardinality(T) where T is a data type
	LowCardinalityType struct {
		LowCardinality string    `parser:"'LowCardinality' '('"`
		Type           *DataType `parser:"@@"`
		Close          string    `parser:"')'"`
	}

	// SimpleType represents basic data types and parametric types
	SimpleType struct {
		Name       string          `parser:"@(Ident | BacktickIdent)"`
		Parameters []TypeParameter `parser:"('(' @@ (',' @@)* ')')?"`
	}

	// TypeParameter represents a parameter in a parametric type (can be number, identifier, string, or nested function call)
	TypeParameter struct {
		Function *ParametricFunction `parser:"@@"`
		Number   *string             `parser:"| @Number"`
		String   *string             `parser:"| @String"`
		Ident    *string             `parser:"| @(Ident | BacktickIdent)"`
	}

	// ParametricFunction represents a function call within type parameters (e.g., quantiles(0.5, 0.75))
	ParametricFunction struct {
		Name       string          `parser:"@(Ident | BacktickIdent)"`
		Parameters []TypeParameter `parser:"'(' (@@ (',' @@)*)? ')'"`
	}

	// DefaultClause represents DEFAULT, MATERIALIZED, EPHEMERAL, or ALIAS expressions
	DefaultClause struct {
		Type       string     `parser:"@('DEFAULT' | 'MATERIALIZED' | 'EPHEMERAL' | 'ALIAS')"`
		Expression Expression `parser:"@@"`
	}

	// CodecClause represents compression codec specification
	CodecClause struct {
		Codec  string      `parser:"'CODEC' '('"`
		Codecs []CodecSpec `parser:"@@ (',' @@)*"`
		Close  string      `parser:"')'"`
	}

	// CodecSpec represents a single codec specification (e.g., ZSTD, LZ4HC(9))
	CodecSpec struct {
		Name       string          `parser:"@(Ident | BacktickIdent)"`
		Parameters []TypeParameter `parser:"('(' @@ (',' @@)* ')')?"`
	}

	// TTLClause represents column-level TTL specification
	TTLClause struct {
		TTL        string     `parser:"'TTL'"`
		Expression Expression `parser:"@@"`
	}
)

// NormalizeDataType converts ClickHouse shorthand types to their canonical forms.
// ClickHouse internally represents certain types differently than their shorthand:
//   - Decimal32(S) → Decimal(9, S)
//   - Decimal64(S) → Decimal(18, S)
//   - Decimal128(S) → Decimal(38, S)
//   - Decimal256(S) → Decimal(76, S)
func NormalizeDataType(dt *DataType) {
	if dt == nil {
		return
	}

	// Recursively normalize nested types
	if dt.Nullable != nil {
		NormalizeDataType(dt.Nullable.Type)
	}
	if dt.Array != nil {
		NormalizeDataType(dt.Array.Type)
	}
	if dt.LowCardinality != nil {
		NormalizeDataType(dt.LowCardinality.Type)
	}
	if dt.Map != nil {
		NormalizeDataType(dt.Map.KeyType)
		NormalizeDataType(dt.Map.ValueType)
	}
	if dt.Tuple != nil {
		for i := range dt.Tuple.Elements {
			if dt.Tuple.Elements[i].Type != nil {
				NormalizeDataType(dt.Tuple.Elements[i].Type)
			}
			if dt.Tuple.Elements[i].UnnamedType != nil {
				NormalizeDataType(dt.Tuple.Elements[i].UnnamedType)
			}
		}
	}
	if dt.Nested != nil {
		for i := range dt.Nested.Columns {
			NormalizeDataType(dt.Nested.Columns[i].Type)
		}
	}

	// Normalize SimpleType Decimal variants
	if dt.Simple != nil {
		switch dt.Simple.Name {
		case "Decimal32":
			normalizeDecimalType(dt, "9")
		case "Decimal64":
			normalizeDecimalType(dt, "18")
		case "Decimal128":
			normalizeDecimalType(dt, "38")
		case "Decimal256":
			normalizeDecimalType(dt, "76")
		}
	}
}

// normalizeDecimalType normalizes Decimal types to their canonical form.
func normalizeDecimalType(dt *DataType, precision string) {
	dt.Simple.Name = "Decimal"
	scale := "0"
	if len(dt.Simple.Parameters) > 0 && dt.Simple.Parameters[0].Number != nil {
		scale = *dt.Simple.Parameters[0].Number
	}
	dt.Simple.Parameters = []TypeParameter{
		{Number: &precision},
		{Number: &scale},
	}
}

// Equal compares two CodecClause instances for equality
func (c *CodecClause) Equal(other *CodecClause) bool {
	if eq, done := compare.NilCheck(c, other); !done {
		return eq
	}
	return compare.Slices(c.Codecs, other.Codecs, func(a, b CodecSpec) bool {
		return a.Equal(&b)
	})
}

// Equal compares two CodecSpec instances for equality
func (c *CodecSpec) Equal(other *CodecSpec) bool {
	return c.Name == other.Name &&
		compare.Slices(c.Parameters, other.Parameters, func(a, b TypeParameter) bool {
			return a.Equal(&b)
		})
}

// Equal compares two TTLClause instances for equality
func (t *TTLClause) Equal(other *TTLClause) bool {
	if eq, done := compare.NilCheck(t, other); !done {
		return eq
	}
	return t.Expression.Equal(&other.Expression)
}

// Equal compares two DefaultClause instances for equality
func (d *DefaultClause) Equal(other *DefaultClause) bool {
	if eq, done := compare.NilCheck(d, other); !done {
		return eq
	}
	return d.Type == other.Type && d.Expression.Equal(&other.Expression)
}

// Equal compares two TypeParameter instances for equality
func (t *TypeParameter) Equal(other *TypeParameter) bool {
	return compare.PointersWithEqual(t.Function, other.Function, (*ParametricFunction).Equal) &&
		compare.Pointers(t.Number, other.Number) &&
		compare.Pointers(t.String, other.String) &&
		compare.Pointers(t.Ident, other.Ident)
}

// Equal compares two ParametricFunction instances for equality
func (p *ParametricFunction) Equal(other *ParametricFunction) bool {
	if eq, done := compare.NilCheck(p, other); !done {
		return eq
	}
	return p.Name == other.Name &&
		compare.Slices(p.Parameters, other.Parameters, func(a, b TypeParameter) bool {
			return a.Equal(&b)
		})
}

// GetDefault returns the default clause for the column, if present
func (c *Column) GetDefault() *DefaultClause {
	for _, attr := range c.Attributes {
		if attr.Default != nil {
			return attr.Default
		}
	}
	return nil
}

// GetCodec returns the codec clause for the column, if present
func (c *Column) GetCodec() *CodecClause {
	for _, attr := range c.Attributes {
		if attr.Codec != nil {
			return attr.Codec
		}
	}
	return nil
}

// GetTTL returns the TTL clause for the column, if present
func (c *Column) GetTTL() *TTLClause {
	for _, attr := range c.Attributes {
		if attr.TTL != nil {
			return attr.TTL
		}
	}
	return nil
}

// GetComment returns the comment for the column, if present
func (c *Column) GetComment() *string {
	for _, attr := range c.Attributes {
		if attr.Comment != nil {
			return attr.Comment
		}
	}
	return nil
}

// Equal compares two DataType instances for equality
func (d *DataType) Equal(other *DataType) bool {
	if d == nil && other == nil {
		return true
	}
	if d == nil || other == nil {
		return false
	}

	// Compare Nullable
	if (d.Nullable != nil) != (other.Nullable != nil) {
		return false
	}
	if d.Nullable != nil {
		return d.Nullable.Type.Equal(other.Nullable.Type)
	}

	// Compare Array
	if (d.Array != nil) != (other.Array != nil) {
		return false
	}
	if d.Array != nil {
		return d.Array.Type.Equal(other.Array.Type)
	}

	// Compare Tuple
	if (d.Tuple != nil) != (other.Tuple != nil) {
		return false
	}
	if d.Tuple != nil {
		if len(d.Tuple.Elements) != len(other.Tuple.Elements) {
			return false
		}
		for i := range d.Tuple.Elements {
			if !tupleElementsEqual(&d.Tuple.Elements[i], &other.Tuple.Elements[i]) {
				return false
			}
		}
		return true
	}

	// Compare Nested
	if (d.Nested != nil) != (other.Nested != nil) {
		return false
	}
	if d.Nested != nil {
		if len(d.Nested.Columns) != len(other.Nested.Columns) {
			return false
		}
		for i := range d.Nested.Columns {
			if !nestedColumnsEqual(&d.Nested.Columns[i], &other.Nested.Columns[i]) {
				return false
			}
		}
		return true
	}

	// Compare Map
	if (d.Map != nil) != (other.Map != nil) {
		return false
	}
	if d.Map != nil {
		return d.Map.KeyType.Equal(other.Map.KeyType) && d.Map.ValueType.Equal(other.Map.ValueType)
	}

	// Compare LowCardinality
	if (d.LowCardinality != nil) != (other.LowCardinality != nil) {
		return false
	}
	if d.LowCardinality != nil {
		return d.LowCardinality.Type.Equal(other.LowCardinality.Type)
	}

	// Compare Simple
	if (d.Simple != nil) != (other.Simple != nil) {
		return false
	}
	if d.Simple != nil {
		if d.Simple.Name != other.Simple.Name {
			return false
		}
		if len(d.Simple.Parameters) != len(other.Simple.Parameters) {
			return false
		}
		for i := range d.Simple.Parameters {
			if !d.Simple.Parameters[i].Equal(&other.Simple.Parameters[i]) {
				return false
			}
		}
		return true
	}

	return true
}

func tupleElementsEqual(a, b *TupleElement) bool {
	// Compare name
	if (a.Name != nil) != (b.Name != nil) {
		return false
	}
	if a.Name != nil && *a.Name != *b.Name {
		return false
	}

	// Compare Type
	if (a.Type != nil) != (b.Type != nil) {
		return false
	}
	if a.Type != nil {
		return a.Type.Equal(b.Type)
	}

	// Compare UnnamedType
	if (a.UnnamedType != nil) != (b.UnnamedType != nil) {
		return false
	}
	if a.UnnamedType != nil {
		return a.UnnamedType.Equal(b.UnnamedType)
	}

	return true
}

func nestedColumnsEqual(a, b *NestedColumn) bool {
	return a.Name == b.Name && a.Type.Equal(b.Type)
}
