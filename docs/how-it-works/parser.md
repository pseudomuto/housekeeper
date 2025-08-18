# Parser Architecture

Deep dive into Housekeeper's participle-based parser and how it handles ClickHouse DDL parsing.

## Parser Foundation

Housekeeper's parser is built on [participle v2](https://github.com/alecthomas/participle/v2), a Go library for building parsers from grammar rules instead of hand-written parsing code.

### Why Participle?

**Traditional SQL Parsing Approaches:**

1. **Regex-based** (fragile, unmaintainable)
2. **Hand-written parsers** (complex, error-prone)  
3. **Parser generators** (external dependencies, complex tooling)

**Participle Benefits:**

- **Grammar as Code**: Parser rules are Go struct tags
- **Type Safety**: Direct mapping to Go structs
- **Maintainability**: Clear, readable grammar definitions
- **Performance**: Optimized parsing with minimal overhead
- **Error Handling**: Detailed error messages with context

## Grammar Architecture

### Top-Level Structure

```go
// Main SQL structure - entry point for all parsing
type SQL struct {
    Statements []Statement `@@*`
}

// All possible statement types
type Statement struct {
    CreateDatabase    *CreateDatabase    `@@`
    CreateTable       *CreateTable      `| @@`
    CreateDictionary  *CreateDictionary `| @@`
    CreateView        *CreateView       `| @@`
    AlterTable        *AlterTable       `| @@`
    SelectStatement   *SelectStatement  `| @@`
    RenameTable       *RenameTable      `| @@`
    DropTable         *DropTable        `| @@`
    // ... more statement types
    Semicolon         string            `| @";"`
}
```

### Grammar Rule Syntax

Participle uses struct tags to define grammar rules:

| Tag | Meaning | Example |
|-----|---------|---------|
| `@` | Capture this token | `@"CREATE"` |
| `@@` | Capture sub-rule | `@@Identifier` |
| `\|` | Alternation (OR) | `"CREATE" \| "ALTER"` |
| `*` | Zero or more | `@@*` |
| `+` | One or more | `@@+` |
| `?` | Optional | `@"IF" "NOT" "EXISTS"?` |
| `()` | Grouping | `(@"," @@)*` |

## Lexer Configuration

### Token Definition

```go
var lexer = lexer.MustSimple([]lexer.SimpleRule{
    // Keywords (case-insensitive)
    {Name: "CREATE", Pattern: `(?i)CREATE`},
    {Name: "TABLE", Pattern: `(?i)TABLE`},
    {Name: "DATABASE", Pattern: `(?i)DATABASE`},
    
    // Identifiers and literals
    {Name: "IDENT", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
    {Name: "BACKTICK_IDENT", Pattern: "`[^`]+`"},
    {Name: "NUMBER", Pattern: `\d+(\.\d+)?`},
    {Name: "STRING", Pattern: `'[^']*'|"[^"]*"`},
    
    // Operators and punctuation
    {Name: "EQ", Pattern: `=`},
    {Name: "NE", Pattern: `!=|<>`},
    {Name: "LE", Pattern: `<=`},
    {Name: "GE", Pattern: `>=`},
    {Name: "LT", Pattern: `<`},
    {Name: "GT", Pattern: `>`},
    
    // Whitespace and comments
    {Name: "WHITESPACE", Pattern: `\s+`},
    {Name: "COMMENT", Pattern: `--[^\n]*`},
    {Name: "MULTILINE_COMMENT", Pattern: `/\*.*?\*/`},
})
```

### Special Token Handling

```go
// Handle ClickHouse-specific tokens
func (p *Parser) parseIdentifier() (string, error) {
    // Handle backtick-quoted identifiers: `my_table`
    if p.current.Type == "BACKTICK_IDENT" {
        return strings.Trim(p.current.Value, "`"), nil
    }
    
    // Handle regular identifiers
    if p.current.Type == "IDENT" {
        return p.current.Value, nil
    }
    
    return "", fmt.Errorf("expected identifier, got %s", p.current.Type)
}
```

## DDL Statement Parsing

### Database Statements

```go
type CreateDatabase struct {
    IfNotExists bool     `@"IF" "NOT" "EXISTS"`
    Name        string   `@@Identifier`
    OnCluster   *string  `("ON" "CLUSTER" @@Identifier)?`
    Engine      *Engine  `("ENGINE" "=" @@)?`
    Comment     *string  `("COMMENT" @@String)?`
}

type AlterDatabase struct {
    Name      string  `@@Identifier`
    OnCluster *string `("ON" "CLUSTER" @@Identifier)?`
    Modify    struct {
        Comment string `"MODIFY" "COMMENT" @@String`
    } `@@`
}
```

### Table Statements

```go
type CreateTable struct {
    OrReplace   bool        `@"OR" "REPLACE"`
    IfNotExists bool        `@"IF" "NOT" "EXISTS"`
    Database    *string     `@@Identifier ("." | @".")?`
    Name        string      `@@Identifier`
    OnCluster   *string     `("ON" "CLUSTER" @@Identifier)?`
    Columns     []ColumnDef `"(" @@ ("," @@)* ")"`
    Engine      *Engine     `"ENGINE" "=" @@`
    OrderBy     *OrderBy    `("ORDER" "BY" @@)?`
    PartitionBy *Expression `("PARTITION" "BY" @@)?`
    PrimaryKey  *PrimaryKey `("PRIMARY" "KEY" @@)?`
    SampleBy    *Expression `("SAMPLE" "BY" @@)?`
    TTL         *Expression `("TTL" @@)?`
    Settings    *Settings   `("SETTINGS" @@)?`
    Comment     *string     `("COMMENT" @@String)?`
}
```

### Complex Column Definitions

```go
type ColumnDef struct {
    Name         string       `@@Identifier`
    Type         DataType     `@@`
    Null         *string      `@("NULL" | "NOT" "NULL")?`
    DefaultExpr  *DefaultExpr `@@?`
    Codec        *Codec       `("CODEC" "(" @@ ")")?`
    TTL          *Expression  `("TTL" @@)?`
    Comment      *string      `("COMMENT" @@String)?`
}

type DefaultExpr struct {
    Type  string     `@("DEFAULT" | "MATERIALIZED" | "EPHEMERAL" | "ALIAS")`
    Value Expression `@@`
}
```

## Expression Parsing Engine

### Operator Precedence

Housekeeper implements proper operator precedence for ClickHouse expressions:

```go
// Expression with proper precedence
type Expression struct {
    Or []AndExpression `@@ ("OR" @@)*`
}

type AndExpression struct {
    And []NotExpression `@@ ("AND" @@)*`
}

type NotExpression struct {
    Not        bool                 `@"NOT"?`
    Comparison ComparisonExpression `@@`
}

type ComparisonExpression struct {
    Left  ArithmeticExpression `@@`
    Right []ComparisonRight    `@@*`
}

type ComparisonRight struct {
    Operator string                `@("=" | "!=" | "<>" | "<=" | ">=" | "<" | ">" | "LIKE" | "NOT" "LIKE" | "IN" | "NOT" "IN" | "BETWEEN")`
    Value    ArithmeticExpression  `@@`
}
```

### Function Calls and Complex Expressions

```go
type PrimaryExpression struct {
    // Literals
    Number       *float64           `@Number`
    String       *string            `| @String`
    Boolean      *bool              `| @("TRUE" | "FALSE")`
    Null         bool               `| @"NULL"`
    
    // Identifiers and qualified names
    Identifier   *QualifiedName     `| @@`
    
    // Function calls
    FunctionCall *FunctionCall      `| @@`
    
    // CAST expressions
    Cast         *CastExpression    `| @@`
    
    // Subqueries
    Subquery     *SelectStatement   `| "(" @@ ")"`
    
    // Parenthesized expressions
    Parentheses  *Expression        `| "(" @@ ")"`
    
    // Arrays and tuples
    Array        []Expression       `| "[" (@@ ("," @@)*)? "]"`
    Tuple        []Expression       `| "(" @@ ("," @@)* ")"`
}

type FunctionCall struct {
    Name      string       `@@Identifier`
    Arguments []Expression `"(" (@@ ("," @@)*)? ")"`
    Over      *OverClause  `("OVER" @@)?`  // Window functions
}
```

## Query Parsing Engine

### SELECT Statement Structure

```go
type SelectStatement struct {
    With     *WithClause    `@@?`
    Distinct bool           `@"DISTINCT"?`
    Columns  []SelectColumn `@@ ("," @@)*`
    From     *FromClause    `("FROM" @@)?`
    Where    *Expression    `("WHERE" @@)?`
    GroupBy  *GroupByClause `("GROUP" "BY" @@)?`
    Having   *Expression    `("HAVING" @@)?`
    OrderBy  *OrderByClause `("ORDER" "BY" @@)?`
    Limit    *LimitClause   `("LIMIT" @@)?`
    Settings *Settings      `("SETTINGS" @@)?`
}
```

### Common Table Expressions (CTEs)

```go
type WithClause struct {
    CTEs []CTE `@@ ("," @@)*`
}

type CTE struct {
    Name    string           `@@Identifier`
    Columns []string         `("(" @@Identifier ("," @@Identifier)* ")")?`
    As      SelectStatement  `"AS" "(" @@ ")"`
}
```

### JOIN Operations

```go
type FromClause struct {
    Table *TableExpression `@@`
    Joins []JoinClause     `@@*`
}

type JoinClause struct {
    Type     string          `@("INNER" | "LEFT" | "RIGHT" | "FULL" | "CROSS" | "ARRAY" | "GLOBAL" | "ASOF")?`
    Join     string          `@"JOIN"`
    Table    TableExpression `@@`
    On       *Expression     `("ON" @@)?`
    Using    []string        `("USING" "(" @@Identifier ("," @@Identifier)* ")")?`
}
```

### Window Functions

```go
type OverClause struct {
    PartitionBy *PartitionByClause `("PARTITION" "BY" @@)?`
    OrderBy     *OrderByClause     `("ORDER" "BY" @@)?`
    Frame       *FrameClause       `@@?`
}

type FrameClause struct {
    Type  string      `@("ROWS" | "RANGE")`
    Start FrameBound  `"BETWEEN" @@`
    End   FrameBound  `"AND" @@`
}

type FrameBound struct {
    Type   string     `@("UNBOUNDED" "PRECEDING" | "CURRENT" "ROW" | "UNBOUNDED" "FOLLOWING")`
    Offset *Expression `| @@ ("PRECEDING" | "FOLLOWING")`
}
```

## Data Type System

### Basic Types

```go
type DataType struct {
    Name       string     `@@Identifier`
    Parameters []DataType `("(" @@ ("," @@)* ")")?`
    
    // For complex types like Nullable(String)
    Nullable      bool      `| @"Nullable" "(" @@DataType ")"`
    Array         *DataType `| @"Array" "(" @@ ")"`
    LowCardinality *DataType `| @"LowCardinality" "(" @@ ")"`
    
    // Map type: Map(String, String)
    MapKey        *DataType `| @"Map" "(" @@`
    MapValue      *DataType `"," @@ ")"`
    
    // Tuple type: Tuple(String, UInt64)
    TupleElements []DataType `| @"Tuple" "(" @@ ("," @@)* ")"`
}
```

### Type Validation

```go
func (dt DataType) Validate() error {
    // Validate illegal combinations
    if dt.Name == "Nullable" && dt.Parameters != nil {
        if len(dt.Parameters) == 1 && dt.Parameters[0].Name == "LowCardinality" {
            return fmt.Errorf("Nullable(LowCardinality(...)) is not supported")
        }
    }
    
    // Validate numeric precision
    if dt.Name == "Decimal64" && len(dt.Parameters) > 2 {
        return fmt.Errorf("Decimal64 accepts at most 2 parameters")
    }
    
    return nil
}
```

## Error Handling

### Detailed Error Messages

```go
type ParseError struct {
    Position lexer.Position
    Expected []string
    Found    string
    Context  string
}

func (e ParseError) Error() string {
    return fmt.Sprintf(
        "parse error at line %d, column %d: expected %s, found %s\nContext: %s",
        e.Position.Line,
        e.Position.Column,
        strings.Join(e.Expected, " or "),
        e.Found,
        e.Context,
    )
}
```

### Recovery Strategies

```go
func (p *Parser) recoverFromError() error {
    // Skip to next statement boundary
    for p.current.Type != ";" && p.current.Type != "EOF" {
        if err := p.next(); err != nil {
            return err
        }
    }
    
    // Log warning about skipped content
    p.logger.Warn("Skipped malformed statement, continuing parsing")
    
    return nil
}
```

## Parser API

### Main Parsing Functions

```go
// Parse SQL from string (most common)
func ParseString(sql string) (*SQL, error) {
    return parser.ParseString("", sql)
}

// Parse SQL from io.Reader
func Parse(reader io.Reader) (*SQL, error) {
    return parser.Parse("", reader)
}

// Parse with custom options
func ParseWithOptions(reader io.Reader, options ...Option) (*SQL, error) {
    p := &Parser{
        lexer: lexer.MustSimple(lexerRules),
    }
    
    for _, opt := range options {
        opt(p)
    }
    
    return p.Parse("", reader)
}
```

### Parser Options

```go
type Option func(*Parser)

// Enable debug mode
func WithDebug() Option {
    return func(p *Parser) {
        p.debug = true
    }
}

// Custom error handler
func WithErrorHandler(handler func(error)) Option {
    return func(p *Parser) {
        p.errorHandler = handler
    }
}

// Strict mode (fail on any error)
func WithStrictMode() Option {
    return func(p *Parser) {
        p.strictMode = true
    }
}
```

## Testing Framework

### Testdata Structure

```
pkg/parser/testdata/
├── database_create.sql       # Input SQL
├── database_create.yaml      # Expected parse result
├── table_complex.sql
├── table_complex.yaml
├── query_with_cte.sql
├── query_with_cte.yaml
└── ...
```

### Automatic Test Generation

```bash
# Update test expectations from actual parsing results
go test -v ./pkg/parser -update
```

This regenerates YAML files from parsing results, making it easy to:
1. Add new test cases (just add .sql file)
2. Update expectations when grammar changes
3. Verify parsing correctness visually

### Example Test Case

**Input (`table_complex.sql`):**
```sql
CREATE TABLE analytics.events ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(),
    timestamp DateTime,
    properties Map(String, String) DEFAULT map(),
    metadata Nullable(String) CODEC(ZSTD(3))
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, id)
TTL timestamp + INTERVAL 90 DAY;
```

**Expected Output (`table_complex.yaml`):**
```yaml
statements:
- create_table:
    database: analytics
    name: events
    on_cluster: my_cluster
    columns:
    - name: id
      type:
        name: UUID
      default_expr:
        type: DEFAULT
        value:
          function_call:
            name: generateUUIDv4
            arguments: []
    # ... more columns
    engine:
      name: MergeTree
    partition_by:
      function_call:
        name: toYYYYMM
        arguments:
        - identifier: timestamp
    order_by:
      expressions:
      - identifier: timestamp
      - identifier: id
    ttl:
      binary_op:
        left:
          identifier: timestamp
        operator: "+"
        right:
          interval:
            value: 90
            unit: DAY
```

## Performance Optimization

### Parsing Benchmarks

```go
func BenchmarkParseSimpleTable(b *testing.B) {
    sql := `CREATE TABLE test (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;`
    
    b.ResetTimer()
    for i := 0; i < b.N; i++ {
        _, err := ParseString(sql)
        if err != nil {
            b.Fatal(err)
        }
    }
}

// Results:
// BenchmarkParseSimpleTable-8     100000    10.5 µs/op    2.1 KB/op    21 allocs/op
// BenchmarkParseComplexSchema-8    1000    1.2 ms/op     45 KB/op    450 allocs/op
```

### Memory Optimization

```go
// Reuse parser instances for better performance
type ParserPool struct {
    pool sync.Pool
}

func NewParserPool() *ParserPool {
    return &ParserPool{
        pool: sync.Pool{
            New: func() interface{} {
                return &Parser{
                    lexer: lexer.MustSimple(lexerRules),
                }
            },
        },
    }
}

func (pp *ParserPool) Parse(sql string) (*SQL, error) {
    parser := pp.pool.Get().(*Parser)
    defer pp.pool.Put(parser)
    
    return parser.ParseString(sql)
}
```

## Next Steps

- **[Migration Generation](migration-generation.md)** - How migrations are created from parsed schemas
- **[Docker Integration](docker.md)** - Container management for testing
- **[Overview](overview.md)** - High-level architecture understanding
- **[Best Practices](../advanced/best-practices.md)** - Production deployment patterns