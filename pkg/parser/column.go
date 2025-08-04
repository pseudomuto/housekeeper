package parser

type (
	// Column represents a complete column definition in ClickHouse DDL.
	// It includes the column name, data type, and all possible modifiers
	// such as DEFAULT values, MATERIALIZED expressions, ALIAS definitions,
	// compression CODECs, TTL settings, and comments.
	Column struct {
		Name       string            `parser:"@Ident"`
		DataType   *DataType         `parser:"@@"`
		Default    *DefaultClause    `parser:"@@?"`
		Codec      *CodecClause      `parser:"@@?"`
		TTL        *TTLClause        `parser:"@@?"`
		Comment    *string           `parser:"('COMMENT' @String)?"`
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
		Name *string   `parser:"(@Ident"`
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
		Name string    `parser:"@Ident"`
		Type *DataType `parser:"@@"`
	}

	// MapType represents Map(K, V) where K and V are data types
	MapType struct {
		Map      string    `parser:"'Map' '('"`
		KeyType  *DataType `parser:"@@"`
		Comma    string    `parser:"','"`
		ValueType *DataType `parser:"@@"`
		Close    string    `parser:"')'"`
	}

	// LowCardinalityType represents LowCardinality(T) where T is a data type
	LowCardinalityType struct {
		LowCardinality string    `parser:"'LowCardinality' '('"`
		Type           *DataType `parser:"@@"`
		Close          string    `parser:"')'"`
	}

	// SimpleType represents basic data types and parametric types
	SimpleType struct {
		Name       string            `parser:"@Ident"`
		Parameters []TypeParameter   `parser:"('(' @@ (',' @@)* ')')?"`
	}

	// TypeParameter represents a parameter in a parametric type (can be number or identifier)
	TypeParameter struct {
		Number *string `parser:"@Number"`
		Ident  *string `parser:"| @Ident"`
		String *string `parser:"| @String"`
	}

	// DefaultClause represents DEFAULT, MATERIALIZED, EPHEMERAL, or ALIAS expressions
	DefaultClause struct {
		Type       string     `parser:"@('DEFAULT' | 'MATERIALIZED' | 'EPHEMERAL' | 'ALIAS')"`
		Expression Expression `parser:"@@"`
	}

	// Expression represents any ClickHouse expression (simplified for now)
	// In a full implementation, this would parse complex expressions
	Expression struct {
		// For now, we capture everything until we hit specific keywords
		// Note: This will collapse whitespace due to lexer configuration
		Raw string `parser:"@(~('CODEC' | 'TTL' | 'COMMENT' | 'PRIMARY' | 'ORDER' | 'PARTITION' | 'SAMPLE' | 'SETTINGS' | 'ENGINE' | ';'))+"`
	}

	// CodecClause represents compression codec specification
	CodecClause struct {
		Codec      string       `parser:"'CODEC' '('"`
		Codecs     []CodecSpec  `parser:"@@ (',' @@)*"`
		Close      string       `parser:"')'"`
	}

	// CodecSpec represents a single codec specification (e.g., ZSTD, LZ4HC(9))
	CodecSpec struct {
		Name       string           `parser:"@Ident"`
		Parameters []TypeParameter  `parser:"('(' @@ (',' @@)* ')')?"`
	}

	// TTLClause represents column-level TTL specification
	TTLClause struct {
		TTL        string     `parser:"'TTL'"`
		Expression Expression `parser:"@@"`
	}
)

// Common ClickHouse data types for reference:
// Numeric: UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256
// Floating: Float32, Float64
// Decimal: Decimal(P, S), Decimal32(S), Decimal64(S), Decimal128(S), Decimal256(S)
// Boolean: Bool
// String: String, FixedString(N)
// UUID: UUID
// Date/Time: Date, Date32, DateTime, DateTime([timezone]), DateTime64(precision, [timezone])
// Enum: Enum8('value1' = 1, 'value2' = 2, ...), Enum16(...)
// Array: Array(T)
// Tuple: Tuple(T1, T2, ...) or Tuple(name1 T1, name2 T2, ...)
// Map: Map(key, value)
// Nested: Nested(name1 Type1, name2 Type2, ...)
// Nullable: Nullable(T)
// LowCardinality: LowCardinality(T)
// AggregateFunction: AggregateFunction(name, types...)
// SimpleAggregateFunction: SimpleAggregateFunction(name, types...)
// Nothing, Interval
// IPv4, IPv6
// Geo types: Point, Ring, Polygon, MultiPolygon
// JSON (experimental)

// Codecs: NONE, LZ4, LZ4HC(level), ZSTD(level), ZSTD_QAT(level), DEFLATE_QPL, Delta(delta_bytes), DoubleDelta, Gorilla, FPC, T64, AES_128_GCM_SIV, AES_256_GCM_SIV

// Column examples:
// user_id UInt64
// name String DEFAULT 'Anonymous'
// email LowCardinality(String) CODEC(ZSTD(3))
// age UInt8 DEFAULT 18 COMMENT 'User age in years'
// tags Array(String) DEFAULT []
// metadata Nested(key String, value String)
// coordinates Tuple(lat Float64, lon Float64)
// settings Map(String, String)
// created_at DateTime DEFAULT now() TTL created_at + INTERVAL 1 YEAR
// data Nullable(String) CODEC(LZ4HC(9))
// status Enum8('active' = 1, 'inactive' = 2) DEFAULT 'active'
// full_name String MATERIALIZED concat(first_name, ' ', last_name)
// temp_data String TTL created_at + INTERVAL 30 DAY