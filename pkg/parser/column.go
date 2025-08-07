package parser

type (
	// Column represents a complete column definition in ClickHouse DDL.
	// It includes the column name, data type, and all possible modifiers
	// such as DEFAULT values, MATERIALIZED expressions, ALIAS definitions,
	// compression CODECs, TTL settings, and comments.
	Column struct {
		Name       string            `parser:"@(Ident | BacktickIdent)"`
		DataType   *DataType         `parser:"@@"`
		Attributes []ColumnAttribute `parser:"@@*"`
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

	// TypeParameter represents a parameter in a parametric type (can be number or identifier)
	TypeParameter struct {
		Number *string `parser:"@Number"`
		Ident  *string `parser:"| @(Ident | BacktickIdent)"`
		String *string `parser:"| @String"`
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
