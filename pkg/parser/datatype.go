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

	// TypeParameter represents a parameter in a parametric type (can be number, identifier, string, enum value, or nested function call)
	TypeParameter struct {
		Function  *ParametricFunction `parser:"@@"`
		EnumValue *EnumValue          `parser:"| @@"`
		Number    *string             `parser:"| @Number"`
		String    *string             `parser:"| @String"`
		Ident     *string             `parser:"| @(Ident | BacktickIdent)"`
	}

	// EnumValue represents an enum value definition in Enum8/Enum16: 'name' = number
	EnumValue struct {
		Name  string `parser:"@String '='"`
		Value string `parser:"@Number"`
	}

	// ParametricFunction represents a function call within type parameters (e.g., quantiles(0.5, 0.75))
	ParametricFunction struct {
		Name       string          `parser:"@(Ident | BacktickIdent)"`
		Parameters []TypeParameter `parser:"'(' (@@ (',' @@)*)? ')'"`
	}
)

// Interface implementations for DataTypeComparable

// Equal compares two SimpleType instances with special handling for ClickHouse normalization patterns
func (s *SimpleType) Equal(other DataTypeComparable) bool {
	otherSimple, ok := other.(*SimpleType)
	if !ok {
		return false
	}

	// Special handling for DateTime/DateTime64 normalization
	// ClickHouse normalizes DateTime(precision, timezone) to DateTime64(precision) in system.tables
	// So DateTime(3, UTC) and DateTime64(3) should be considered equal
	if s.isDateTimeType() && otherSimple.isDateTimeType() {
		return s.isDateTimeCompatibleWith(otherSimple)
	}

	// Special handling for Decimal type normalization
	// ClickHouse normalizes Decimal64(N) to Decimal(18, N) in system.tables
	// Similarly Decimal32(N) -> Decimal(9, N) and Decimal128(N) -> Decimal(38, N)
	if s.isDecimalType() && otherSimple.isDecimalType() {
		return s.isDecimalCompatibleWith(otherSimple)
	}

	if s.Name != otherSimple.Name {
		return false
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

// isDateTimeType returns true if this is a DateTime or DateTime64 type
func (s *SimpleType) isDateTimeType() bool {
	return s.Name == "DateTime" || s.Name == "DateTime64"
}

// isDateTimeCompatibleWith checks if two DateTime/DateTime64 types are semantically compatible
// despite ClickHouse's normalization patterns:
// - DateTime(precision, timezone) is normalized to DateTime64(precision) in system.tables
// - DateTime64(precision, timezone) may be normalized to DateTime64(precision)
func (s *SimpleType) isDateTimeCompatibleWith(other *SimpleType) bool {
	// Get precision from both types
	sPrecision := s.getDateTimePrecision()
	otherPrecision := other.getDateTimePrecision()

	// If either has no precision (plain DateTime), they must be the same type with same params
	if sPrecision == "" || otherPrecision == "" {
		// Plain DateTime without precision - use strict comparison
		if s.Name != other.Name {
			return false
		}
		if len(s.Parameters) != len(other.Parameters) {
			return false
		}
		for i := range s.Parameters {
			if !s.Parameters[i].Equal(&other.Parameters[i]) {
				return false
			}
		}
		return true
	}

	// Both have precision - compare precision values
	if sPrecision != otherPrecision {
		return false
	}

	// Precision matches, so DateTime(3, UTC) == DateTime64(3) == DateTime64(3, 'UTC')
	return true
}

// getDateTimePrecision returns the precision parameter for DateTime/DateTime64 types
// Returns "" if no precision is specified (plain DateTime)
func (s *SimpleType) getDateTimePrecision() string {
	if len(s.Parameters) == 0 {
		return ""
	}
	// First parameter should be precision (a number)
	if s.Parameters[0].Number != nil {
		return *s.Parameters[0].Number
	}
	return ""
}

// isDecimalType returns true if this is a Decimal, Decimal32, Decimal64, or Decimal128 type
func (s *SimpleType) isDecimalType() bool {
	return s.Name == "Decimal" || s.Name == "Decimal32" || s.Name == "Decimal64" || s.Name == "Decimal128"
}

// isDecimalCompatibleWith checks if two Decimal types are semantically compatible
// despite ClickHouse's normalization patterns:
// - Decimal32(N) is normalized to Decimal(9, N)
// - Decimal64(N) is normalized to Decimal(18, N)
// - Decimal128(N) is normalized to Decimal(38, N)
func (s *SimpleType) isDecimalCompatibleWith(other *SimpleType) bool {
	sPrecision, sScale := s.getDecimalPrecisionScale()
	otherPrecision, otherScale := other.getDecimalPrecisionScale()

	// Compare normalized precision and scale
	return sPrecision == otherPrecision && sScale == otherScale
}

// getDecimalPrecisionScale returns the normalized (precision, scale) for a Decimal type
// For DecimalN(S), it returns the equivalent Decimal(P, S) values
func (s *SimpleType) getDecimalPrecisionScale() (string, string) {
	switch s.Name {
	case "Decimal32":
		// Decimal32(S) -> Decimal(9, S)
		if len(s.Parameters) >= 1 && s.Parameters[0].Number != nil {
			return "9", *s.Parameters[0].Number
		}
		return "9", ""
	case "Decimal64":
		// Decimal64(S) -> Decimal(18, S)
		if len(s.Parameters) >= 1 && s.Parameters[0].Number != nil {
			return "18", *s.Parameters[0].Number
		}
		return "18", ""
	case "Decimal128":
		// Decimal128(S) -> Decimal(38, S)
		if len(s.Parameters) >= 1 && s.Parameters[0].Number != nil {
			return "38", *s.Parameters[0].Number
		}
		return "38", ""
	case "Decimal":
		// Decimal(P, S) - already normalized form
		if len(s.Parameters) >= 2 {
			var precision, scale string
			if s.Parameters[0].Number != nil {
				precision = *s.Parameters[0].Number
			}
			if s.Parameters[1].Number != nil {
				scale = *s.Parameters[1].Number
			}
			return precision, scale
		}
		return "", ""
	default:
		return "", ""
	}
}

// TypeName returns the type name for SimpleType
func (s *SimpleType) TypeName() string {
	return "SimpleType"
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

// Equal compares two TypeParameter instances for equality.
// It includes special handling for the common case where ClickHouse system tables output
// quoted strings (e.g., 'UTC') while user DDL may use unquoted identifiers (e.g., UTC).
// These are semantically equivalent and should compare as equal.
func (t *TypeParameter) Equal(other *TypeParameter) bool {
	// If both have functions, compare functions
	if t.Function != nil || other.Function != nil {
		return compare.PointersWithEqual(t.Function, other.Function, (*ParametricFunction).Equal)
	}

	// If both have enum values, compare enum values
	if t.EnumValue != nil || other.EnumValue != nil {
		return compare.PointersWithEqual(t.EnumValue, other.EnumValue, (*EnumValue).Equal)
	}

	// If both have numbers, compare numbers
	if t.Number != nil && other.Number != nil {
		return *t.Number == *other.Number
	}

	// If both have strings, compare strings
	if t.String != nil && other.String != nil {
		return *t.String == *other.String
	}

	// If both have idents, compare idents
	if t.Ident != nil && other.Ident != nil {
		return *t.Ident == *other.Ident
	}

	// Handle cross-type comparison: String vs Ident
	// ClickHouse may output 'UTC' (quoted string) while user DDL has UTC (unquoted ident)
	// Strip quotes from string and compare with ident
	if t.String != nil && other.Ident != nil {
		return stripQuotes(*t.String) == *other.Ident
	}
	if t.Ident != nil && other.String != nil {
		return *t.Ident == stripQuotes(*other.String)
	}

	// All other combinations (e.g., Number vs String) are not equal
	// Check if both are nil/empty
	return t.Function == nil && other.Function == nil &&
		t.EnumValue == nil && other.EnumValue == nil &&
		t.Number == nil && other.Number == nil &&
		t.String == nil && other.String == nil &&
		t.Ident == nil && other.Ident == nil
}

// Equal compares two EnumValue instances for equality
func (e *EnumValue) Equal(other *EnumValue) bool {
	if e == nil && other == nil {
		return true
	}
	if e == nil || other == nil {
		return false
	}
	return e.Name == other.Name && e.Value == other.Value
}

// String returns the SQL representation of an enum value: 'name' = value
func (e *EnumValue) String() string {
	if e == nil {
		return ""
	}
	return e.Name + " = " + e.Value
}

// stripQuotes removes surrounding single quotes from a string.
// For example, "'UTC'" becomes "UTC".
func stripQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

// Equal compares two ParametricFunction instances for equality
// Includes special handling for ClickHouse normalization patterns when function names
// represent type names (like DateTime, DateTime64, Decimal, Decimal64, etc.)
func (p *ParametricFunction) Equal(other *ParametricFunction) bool {
	if eq, done := compare.NilCheck(p, other); !done {
		return eq
	}

	// Check if this is a DateTime/DateTime64 type comparison
	if isDateTimeFunctionName(p.Name) && isDateTimeFunctionName(other.Name) {
		return parametricFunctionsDateTimeEqual(p, other)
	}

	// Check if this is a Decimal type comparison
	if isDecimalFunctionName(p.Name) && isDecimalFunctionName(other.Name) {
		return parametricFunctionsDecimalEqual(p, other)
	}

	return p.Name == other.Name &&
		compare.Slices(p.Parameters, other.Parameters, func(a, b TypeParameter) bool {
			return a.Equal(&b)
		})
}

// isDateTimeFunctionName returns true if the name is DateTime or DateTime64
func isDateTimeFunctionName(name string) bool {
	return name == "DateTime" || name == "DateTime64"
}

// isDecimalFunctionName returns true if the name is a Decimal type
func isDecimalFunctionName(name string) bool {
	return name == "Decimal" || name == "Decimal32" || name == "Decimal64" || name == "Decimal128"
}

// parametricFunctionsDateTimeEqual compares DateTime/DateTime64 parametric functions
// with special handling for ClickHouse normalization
func parametricFunctionsDateTimeEqual(p, other *ParametricFunction) bool {
	// Get precision from both
	pPrecision := getParametricFunctionPrecision(p)
	otherPrecision := getParametricFunctionPrecision(other)

	// If either has no precision (plain DateTime), use strict comparison
	if pPrecision == "" || otherPrecision == "" {
		if p.Name != other.Name {
			return false
		}
		return compare.Slices(p.Parameters, other.Parameters, func(a, b TypeParameter) bool {
			return a.Equal(&b)
		})
	}

	// Both have precision - compare precision values
	// DateTime(3, UTC) == DateTime64(3) when precisions match
	return pPrecision == otherPrecision
}

// parametricFunctionsDecimalEqual compares Decimal type parametric functions
// with special handling for ClickHouse normalization
func parametricFunctionsDecimalEqual(p, other *ParametricFunction) bool {
	pPrecision, pScale := getDecimalFunctionPrecisionScale(p)
	otherPrecision, otherScale := getDecimalFunctionPrecisionScale(other)

	return pPrecision == otherPrecision && pScale == otherScale
}

// getParametricFunctionPrecision extracts precision from DateTime/DateTime64 type parameters
func getParametricFunctionPrecision(p *ParametricFunction) string {
	if len(p.Parameters) == 0 {
		return ""
	}
	if p.Parameters[0].Number != nil {
		return *p.Parameters[0].Number
	}
	return ""
}

// getDecimalFunctionPrecisionScale returns normalized (precision, scale) for Decimal type functions
func getDecimalFunctionPrecisionScale(p *ParametricFunction) (string, string) {
	switch p.Name {
	case "Decimal32":
		// Decimal32(S) -> Decimal(9, S)
		if len(p.Parameters) >= 1 && p.Parameters[0].Number != nil {
			return "9", *p.Parameters[0].Number
		}
		return "9", ""
	case "Decimal64":
		// Decimal64(S) -> Decimal(18, S)
		if len(p.Parameters) >= 1 && p.Parameters[0].Number != nil {
			return "18", *p.Parameters[0].Number
		}
		return "18", ""
	case "Decimal128":
		// Decimal128(S) -> Decimal(38, S)
		if len(p.Parameters) >= 1 && p.Parameters[0].Number != nil {
			return "38", *p.Parameters[0].Number
		}
		return "38", ""
	case "Decimal":
		// Decimal(P, S) - already normalized form
		if len(p.Parameters) >= 2 {
			var precision, scale string
			if p.Parameters[0].Number != nil {
				precision = *p.Parameters[0].Number
			}
			if p.Parameters[1].Number != nil {
				scale = *p.Parameters[1].Number
			}
			return precision, scale
		}
		return "", ""
	default:
		return "", ""
	}
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
	if t.EnumValue != nil {
		return t.EnumValue.String()
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
