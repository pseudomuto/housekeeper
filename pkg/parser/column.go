package parser

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
)

type (
	// DataTypeComparable represents a data type that can be compared for equality.
	//
	// This interface allows each concrete data type to implement its own comparison logic,
	// including special handling for ClickHouse normalization patterns.
	DataTypeComparable interface {
		Equal(other DataTypeComparable) bool
		TypeName() string
	}

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
		Name *string   `parser:"@(Ident | BacktickIdent)"`
		Type *DataType `parser:"@@"`
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

// Interface implementations for DataTypeComparable

// Equal compares two SimpleType instances with special handling for ClickHouse normalization patterns
func (s *SimpleType) Equal(other DataTypeComparable) bool {
	otherSimple, ok := other.(*SimpleType)
	if !ok {
		return false
	}

	if s.Name != otherSimple.Name {
		return false
	}

	// Special handling for DateTime64 timezone normalization
	// ClickHouse may normalize DateTime64(precision, timezone) to DateTime64(precision) in system.tables
	if s.Name == "DateTime64" && otherSimple.Name == "DateTime64" {
		return s.isDateTime64CompatibleWith(otherSimple)
	}

	// Standard parameter comparison for all other types
	if len(s.Parameters) != len(otherSimple.Parameters) {
		return false
	}
	for i := range s.Parameters {
		if !s.Parameters[i].Equal(&otherSimple.Parameters[i]) {
			return false
		}
	}
	return true
}

// TypeName returns the type name for SimpleType
func (s *SimpleType) TypeName() string {
	return "SimpleType"
}

// isDateTime64CompatibleWith checks if two DateTime64 types are semantically compatible
// despite potential timezone normalization differences from ClickHouse system.tables
func (s *SimpleType) isDateTime64CompatibleWith(other *SimpleType) bool {
	// Both must have at least precision parameter
	if len(s.Parameters) == 0 || len(other.Parameters) == 0 {
		return false
	}

	// First parameter (precision) must match
	if !s.Parameters[0].Equal(&other.Parameters[0]) {
		return false
	}

	// Case 1: Both have same number of parameters - use normal comparison
	if len(s.Parameters) == len(other.Parameters) {
		for i := range s.Parameters {
			if !s.Parameters[i].Equal(&other.Parameters[i]) {
				return false
			}
		}
		return true
	}

	// Case 2: Different parameter counts - check for timezone normalization
	// One should have 1 param (precision), other should have 2 params (precision + timezone)
	if (len(s.Parameters) == 1 && len(other.Parameters) == 2) ||
		(len(s.Parameters) == 2 && len(other.Parameters) == 1) {
		// Precision already matches (checked above), so this is compatible
		return true
	}

	// Case 3: Both have more than 2 params or some other mismatch - not compatible
	return false
}

// Equal compares two NullableType instances
func (n *NullableType) Equal(other DataTypeComparable) bool {
	otherNullable, ok := other.(*NullableType)
	if !ok {
		return false
	}
	return n.Type.Equal(otherNullable.Type)
}

// TypeName returns the type name for NullableType
func (n *NullableType) TypeName() string {
	return "NullableType"
}

// Equal compares two ArrayType instances
func (a *ArrayType) Equal(other DataTypeComparable) bool {
	otherArray, ok := other.(*ArrayType)
	if !ok {
		return false
	}
	return a.Type.Equal(otherArray.Type)
}

// TypeName returns the type name for ArrayType
func (a *ArrayType) TypeName() string {
	return "ArrayType"
}

// Equal compares two TupleType instances
func (t *TupleType) Equal(other DataTypeComparable) bool {
	otherTuple, ok := other.(*TupleType)
	if !ok {
		return false
	}
	if len(t.Elements) != len(otherTuple.Elements) {
		return false
	}
	for i := range t.Elements {
		if !tupleElementsEqual(&t.Elements[i], &otherTuple.Elements[i]) {
			return false
		}
	}
	return true
}

// TypeName returns the type name for TupleType
func (t *TupleType) TypeName() string {
	return "TupleType"
}

// Equal compares two NestedType instances
func (n *NestedType) Equal(other DataTypeComparable) bool {
	otherNested, ok := other.(*NestedType)
	if !ok {
		return false
	}
	if len(n.Columns) != len(otherNested.Columns) {
		return false
	}
	for i := range n.Columns {
		if !nestedColumnsEqual(&n.Columns[i], &otherNested.Columns[i]) {
			return false
		}
	}
	return true
}

// TypeName returns the type name for NestedType
func (n *NestedType) TypeName() string {
	return "NestedType"
}

// Equal compares two MapType instances
func (m *MapType) Equal(other DataTypeComparable) bool {
	otherMap, ok := other.(*MapType)
	if !ok {
		return false
	}
	return m.KeyType.Equal(otherMap.KeyType) && m.ValueType.Equal(otherMap.ValueType)
}

// TypeName returns the type name for MapType
func (m *MapType) TypeName() string {
	return "MapType"
}

// Equal compares two LowCardinalityType instances
func (l *LowCardinalityType) Equal(other DataTypeComparable) bool {
	otherLowCard, ok := other.(*LowCardinalityType)
	if !ok {
		return false
	}
	return l.Type.Equal(otherLowCard.Type)
}

// TypeName returns the type name for LowCardinalityType
func (l *LowCardinalityType) TypeName() string {
	return "LowCardinalityType"
}

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

// getConcreteType extracts the concrete DataTypeComparable implementation from the DataType union
func (d *DataType) getConcreteType() DataTypeComparable {
	if d.Nullable != nil {
		return d.Nullable
	}
	if d.Array != nil {
		return d.Array
	}
	if d.Tuple != nil {
		return d.Tuple
	}
	if d.Nested != nil {
		return d.Nested
	}
	if d.Map != nil {
		return d.Map
	}
	if d.LowCardinality != nil {
		return d.LowCardinality
	}
	if d.Simple != nil {
		return d.Simple
	}
	return nil
}

// Equal compares two DataType instances for equality using interface delegation
func (d *DataType) Equal(other *DataType) bool {
	if d == nil && other == nil {
		return true
	}
	if d == nil || other == nil {
		return false
	}

	// Extract concrete type implementations
	dType := d.getConcreteType()
	otherType := other.getConcreteType()

	if dType == nil && otherType == nil {
		return true
	}
	if dType == nil || otherType == nil {
		return false
	}

	// Delegate to interface-based comparison
	return dType.Equal(otherType)
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

// String returns the SQL representation of the data type.
// This is the canonical implementation used by all packages.
func (d *DataType) String() string {
	if d == nil {
		return ""
	}

	if d.Nullable != nil {
		return d.Nullable.String()
	}
	if d.Array != nil {
		return d.Array.String()
	}
	if d.Tuple != nil {
		return d.Tuple.String()
	}
	if d.Nested != nil {
		return d.Nested.String()
	}
	if d.Map != nil {
		return d.Map.String()
	}
	if d.LowCardinality != nil {
		return d.LowCardinality.String()
	}
	if d.Simple != nil {
		return d.Simple.String()
	}
	return ""
}

// String returns the SQL representation of a Nullable type.
func (n *NullableType) String() string {
	if n == nil {
		return ""
	}
	return "Nullable(" + n.Type.String() + ")"
}

// String returns the SQL representation of an Array type.
func (a *ArrayType) String() string {
	if a == nil {
		return ""
	}
	return "Array(" + a.Type.String() + ")"
}

// String returns the SQL representation of a Tuple type.
func (t *TupleType) String() string {
	if t == nil || len(t.Elements) == 0 {
		return "Tuple()"
	}

	elements := make([]string, 0, len(t.Elements))
	for _, element := range t.Elements {
		elements = append(elements, element.String())
	}

	return "Tuple(" + strings.Join(elements, ", ") + ")"
}

// String returns the SQL representation of a tuple element.
func (e *TupleElement) String() string {
	if e.Name != nil {
		// Named tuple element
		return *e.Name + " " + e.Type.String()
	}
	// Unnamed tuple element
	return e.UnnamedType.String()
}

// String returns the SQL representation of a Nested type.
func (n *NestedType) String() string {
	if n == nil || len(n.Columns) == 0 {
		return "Nested()"
	}

	columns := make([]string, 0, len(n.Columns))
	for _, col := range n.Columns {
		columns = append(columns, col.String())
	}

	return "Nested(" + strings.Join(columns, ", ") + ")"
}

// String returns the SQL representation of a nested column.
func (c *NestedColumn) String() string {
	return c.Name + " " + c.Type.String()
}

// String returns the SQL representation of a Map type.
func (m *MapType) String() string {
	if m == nil {
		return ""
	}
	return "Map(" + m.KeyType.String() + ", " + m.ValueType.String() + ")"
}

// String returns the SQL representation of a LowCardinality type.
func (l *LowCardinalityType) String() string {
	if l == nil {
		return ""
	}
	return "LowCardinality(" + l.Type.String() + ")"
}

// String returns the SQL representation of a simple or parametric type.
func (s *SimpleType) String() string {
	if s == nil {
		return ""
	}

	if len(s.Parameters) == 0 {
		return s.Name
	}

	params := make([]string, 0, len(s.Parameters))
	for _, param := range s.Parameters {
		params = append(params, formatTypeParameter(&param))
	}

	return s.Name + "(" + strings.Join(params, ", ") + ")"
}

// formatTypeParameter formats a type parameter to its SQL representation.
// This is a helper function rather than a method because TypeParameter
// has a field named String which would conflict with a String() method.
func formatTypeParameter(t *TypeParameter) string {
	if t.Function != nil {
		return t.Function.String()
	}
	if t.String != nil {
		return *t.String
	}
	if t.Number != nil {
		return *t.Number
	}
	if t.Ident != nil {
		return *t.Ident
	}
	return ""
}

// String returns the SQL representation of a parametric function.
func (p *ParametricFunction) String() string {
	if p == nil {
		return ""
	}

	if len(p.Parameters) == 0 {
		return p.Name + "()"
	}

	params := make([]string, 0, len(p.Parameters))
	for _, param := range p.Parameters {
		params = append(params, formatTypeParameter(&param))
	}

	return p.Name + "(" + strings.Join(params, ", ") + ")"
}
