# How Housekeeper Works

Understand the architecture and algorithms that power Housekeeper's ClickHouse schema management capabilities.

## High-Level Architecture

Housekeeper is built with a modular, extensible architecture designed for reliability and maintainability:

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Schema Files  │    │  ClickHouse DB  │    │   Migration     │
│   (SQL + YAML)  │    │   (Current)     │    │     Files       │
└─────────┬───────┘    └─────────┬───────┘    └─────────────────┘
          │                      │                      ▲
          │                      │                      │
          ▼                      ▼                      │
┌─────────────────┐    ┌─────────────────┐              │
│     Parser      │    │    Extractor    │              │
│   (Participle)  │    │  (ClickHouse)   │              │
└─────────┬───────┘    └─────────┬───────┘              │
          │                      │                      │
          │                      │                      │
          ▼                      ▼                      │
┌─────────────────────────────────────────┐              │
│           Schema Comparison             │              │
│        (Intelligent Diff Engine)       │              │
└─────────────────┬───────────────────────┘              │
                  │                                      │
                  ▼                                      │
┌─────────────────────────────────────────┐              │
│         Migration Generator             │──────────────┘
│      (Strategy-Based DDL Creation)      │
└─────────────────────────────────────────┘
```

## Core Components

### 1. Participle-Based Parser

The foundation of Housekeeper is a robust SQL parser built with the [participle](https://github.com/alecthomas/participle/v2) parsing library.

#### Why Participle over Regex?

**Traditional Approach (Regex):**
```go
// Fragile and hard to maintain
var createTableRegex = regexp.MustCompile(`CREATE\s+TABLE\s+(\w+)\s*\(([^)]+)\)`)
```

**Housekeeper Approach (Grammar):**
```go
// Structured, maintainable parsing rules
type CreateTable struct {
    OrReplace  bool              `@"OR" "REPLACE"`
    IfNotExists bool             `@"IF" "NOT" "EXISTS"`
    Database   *string           `@@Identifier ( "." )?`
    Name       string            `@@Identifier`
    Columns    []ColumnDef       `"(" @@ ( "," @@ )* ")"`
    Engine     *Engine           `"ENGINE" "=" @@`
    // ... more fields
}
```

#### Benefits of Structured Parsing

1. **Maintainability**: Clear, readable grammar rules
2. **Error Handling**: Detailed, actionable error messages  
3. **Extensibility**: Easy to add new ClickHouse features
4. **Type Safety**: Structured Go types instead of string manipulation
5. **Robustness**: Handles complex nested structures naturally
6. **Testing**: Comprehensive test coverage with automatic validation

### 2. Schema Compilation System

Housekeeper processes schema files through a sophisticated compilation pipeline:

#### Import Resolution
```sql
-- db/main.sql
CREATE DATABASE analytics ENGINE = Atomic;
-- housekeeper:import schemas/tables/events.sql
-- housekeeper:import schemas/views/daily_stats.sql
```

The compilation process:
1. **Recursive Processing**: Follows import directives recursively
2. **Path Resolution**: Resolves relative paths from each file's location
3. **Dependency Ordering**: Maintains proper object creation order
4. **Single Output**: Combines all SQL into unified stream
5. **Syntax Validation**: Validates all DDL through the parser

#### Example Compilation Flow
```
db/main.sql
├── schemas/analytics/database.sql
├── schemas/analytics/tables/
│   ├── users.sql
│   ├── events.sql
│   └── products.sql
└── schemas/analytics/views/
    ├── daily_stats.sql (depends on events.sql)
    └── user_summary.sql (depends on users.sql)

Result: Single SQL stream with proper ordering:
1. Database creation
2. Table creation (users, events, products)  
3. View creation (daily_stats, user_summary)
```

### 3. ClickHouse Integration

#### Schema Extraction
```go
// Extract current schema from ClickHouse
client, err := clickhouse.NewClient(ctx, dsn)
schema, err := client.GetSchema(ctx)
```

The extraction process:
1. **Connection Management**: Handles various DSN formats and connection options
2. **System Object Filtering**: Excludes system databases and tables
3. **Complete Object Support**: Extracts databases, tables, dictionaries, views
4. **Cluster Awareness**: Injects ON CLUSTER clauses when configured
5. **DDL Generation**: Produces valid ClickHouse DDL statements

#### Supported Connection Types
```bash
# Simple host:port
localhost:9000

# ClickHouse protocol with auth
clickhouse://user:password@host:9000/database

# TCP protocol with parameters  
tcp://host:9000?username=user&password=pass&database=db
```

### 4. Intelligent Comparison Engine

The heart of Housekeeper is its sophisticated schema comparison algorithm:

#### Object Detection
```go
type SchemaComparison struct {
    Added    []DatabaseObject    // Objects in target but not current
    Modified []ObjectPair        // Objects that exist in both but differ
    Removed  []DatabaseObject    // Objects in current but not target
    Renamed  []RenamePair        // Objects that appear renamed
}
```

#### Rename Detection Algorithm
```go
func detectRenames(current, target []Object) []RenamePair {
    var renames []RenamePair
    
    for _, currentObj := range current {
        for _, targetObj := range target {
            // Compare all properties except names
            if propertiesMatch(currentObj, targetObj) && 
               currentObj.Name != targetObj.Name {
                renames = append(renames, RenamePair{
                    From: currentObj,
                    To:   targetObj,
                })
            }
        }
    }
    
    return renames
}
```

The comparison process:
1. **Property Analysis**: Compares all object properties except names
2. **Exact Matching**: Properties must match exactly for rename detection
3. **Dependency Understanding**: Considers relationships between objects
4. **Change Classification**: Categorizes changes by impact and complexity

### 5. Migration Strategy Engine

Housekeeper employs intelligent migration strategies based on ClickHouse capabilities and limitations:

#### Strategy Selection Matrix

| Object Type | Operation | Strategy | Reason |
|-------------|-----------|----------|---------|
| Database | Create/Drop | Direct DDL | Straightforward operations |
| Database | Comment Change | ALTER DATABASE | Supported by ClickHouse |
| Database | Engine Change | **Error** | Not supported by ClickHouse |
| Table (Standard) | Column Changes | ALTER TABLE | Full ALTER support |
| Table (Integration) | Any Change | DROP+CREATE | Read-only from CH perspective |
| Dictionary | Any Change | CREATE OR REPLACE | No ALTER DICTIONARY support |
| View (Regular) | Query Change | CREATE OR REPLACE | Supported by ClickHouse |
| View (Materialized) | Query Change | DROP+CREATE | More reliable than ALTER |

#### Example Strategy Application
```sql
-- Current: Kafka table with 2 columns
CREATE TABLE events (id UInt64, data String) ENGINE = Kafka(...);

-- Target: Kafka table with 3 columns  
CREATE TABLE events (id UInt64, data String, timestamp DateTime) ENGINE = Kafka(...);

-- Generated Migration (DROP+CREATE strategy for integration engines):
DROP TABLE events;
CREATE TABLE events (id UInt64, data String, timestamp DateTime) ENGINE = Kafka(...);
```

## Parser Architecture Deep Dive

### Grammar Definition

Housekeeper uses structured grammar rules to parse ClickHouse DDL:

```go
// Top-level SQL structure
type SQL struct {
    Statements []Statement `@@*`
}

type Statement struct {
    CreateDatabase    *CreateDatabase    `@@`
    CreateTable       *CreateTable      `| @@`
    CreateDictionary  *CreateDictionary `| @@`
    CreateView        *CreateView       `| @@`
    AlterTable        *AlterTable       `| @@`
    SelectStatement   *SelectStatement  `| @@`
    // ... more statement types
}
```

### Expression Engine

The parser includes a comprehensive expression engine with proper operator precedence:

```
Precedence (lowest to highest):
OR
├── AND
    ├── NOT
        ├── Comparison (=, !=, <, >, <=, >=, LIKE, IN, BETWEEN)
            ├── Arithmetic (+, -, *, /, %)
                ├── Unary (+, -, NOT)
                    └── Primary (literals, identifiers, function calls, parentheses)
```

### Example Parse Tree

For the SQL:
```sql
CREATE TABLE events (
    id UInt64,
    timestamp DateTime DEFAULT now(),
    data Map(String, String)
) ENGINE = MergeTree() ORDER BY timestamp;
```

The parser generates:
```go
Statement{
    CreateTable: &CreateTable{
        Name: "events",
        Columns: []ColumnDef{
            {Name: "id", Type: DataType{Name: "UInt64"}},
            {Name: "timestamp", Type: DataType{Name: "DateTime"}, 
             Default: &DefaultExpr{Type: "DEFAULT", Value: FunctionCall{Name: "now"}}},
            {Name: "data", Type: DataType{Name: "Map", 
             Params: []DataType{{Name: "String"}, {Name: "String"}}}},
        },
        Engine: &Engine{Name: "MergeTree"},
        OrderBy: &OrderBy{Expressions: []Expression{{Identifier: "timestamp"}}},
    },
}
```

## Migration Generation Process

### 1. Schema Compilation
```bash
Input: db/main.sql + imports
↓
Parser: Converts to structured representation
↓  
Output: Complete schema object tree
```

### 2. Current State Extraction
```bash
Input: ClickHouse connection
↓
Extractor: Queries system tables
↓
Parser: Converts DDL to structured representation  
↓
Output: Current schema object tree
```

### 3. Intelligent Comparison
```bash
Input: Target schema + Current schema
↓
Comparison Engine: Analyzes differences
- Object detection (added/removed/modified)
- Rename detection (property matching)
- Dependency analysis (creation order)
↓
Output: Categorized change set
```

### 4. Strategy Application
```bash
Input: Categorized changes
↓
Strategy Engine: Selects optimal approach
- Standard operations: Direct DDL
- Unsupported operations: Error with explanation
- Complex operations: DROP+CREATE or CREATE OR REPLACE
↓
Output: Executable migration DDL
```

### 5. Migration File Generation
```bash
Input: Migration DDL
↓
File Generator: Creates timestamped files
- UTC timestamp naming (yyyyMMddHHmmss.sql)
- Header comments with metadata
- Integrity checksums (housekeeper.sum)
↓
Output: Ready-to-apply migration files
```

### 6. Migration Execution & Progress Tracking

Housekeeper includes a sophisticated migration execution engine with automatic partial progress tracking:

```bash
Input: Migration files
↓
Executor: Statement-by-statement execution
- Bootstrap housekeeper.revisions table
- Load existing revisions (detect partial progress)
- Execute statements with progress tracking
- Record revision entries with hash validation
- Automatic resume from failure points
↓
Output: Execution results with detailed progress information
```

#### Execution Features

**Statement-Level Progress:**
- Each statement execution is tracked individually
- Progress recorded in `housekeeper.revisions` table
- Cryptographic hashes stored for integrity validation
- Automatic detection and resume of partial failures

**Example Execution Flow:**
```go
// Migration: 5 statements total
for i, statement := range migration.Statements {
    // Skip already-applied statements in partial recovery
    if i < partialRevision.Applied {
        continue // Statement already applied successfully
    }
    
    // Execute statement
    if err := executor.Execute(statement); err != nil {
        // Record partial progress: i statements applied, error at statement i+1
        revision := &Revision{
            Applied: i,           // Successfully applied statements
            Total:   len(statements), // Total statements in migration
            Error:   err.Error(),     // Failure reason
            PartialHashes: hashes[:i], // Hashes of applied statements
        }
        return recordRevision(revision)
    }
}
```

**Automatic Resume Algorithm:**
```go
func (e *Executor) detectPartialState(migration) (startIndex int, err error) {
    revision := revisionSet.GetRevision(migration)
    if revision == nil || revision.Applied == revision.Total {
        return 0, nil // New migration or completed migration
    }
    
    // Validate migration file hasn't changed
    if err := e.validateStatementHashes(migration, revision); err != nil {
        return 0, err // Migration file modified since partial execution
    }
    
    // Resume from next statement after last successful one
    return revision.Applied, nil
}
```

## Operation Ordering Algorithm

Housekeeper ensures safe migration ordering by analyzing object dependencies:

### UP Migration Order
```
1. Databases (no dependencies)
2. Tables (may depend on databases)
3. Dictionaries (may depend on tables for sources)
4. Views (depend on tables and dictionaries)

Within each type:
1. CREATE operations
2. ALTER/REPLACE operations  
3. RENAME operations
4. DROP operations
```

### Dependency Resolution Example
```sql
-- Schema with dependencies:
CREATE DATABASE analytics;
CREATE TABLE analytics.events (...);
CREATE DICTIONARY analytics.events_dict SOURCE(CLICKHOUSE(...'analytics.events'...));
CREATE VIEW analytics.summary AS SELECT * FROM analytics.events;

-- Generated order:
1. CREATE DATABASE analytics;
2. CREATE TABLE analytics.events;
3. CREATE DICTIONARY analytics.events_dict;  -- Depends on events table
4. CREATE VIEW analytics.summary;            -- Depends on events table
```

## Error Handling and Validation

### Validation Framework

Housekeeper implements a comprehensive validation system:

```go
type ValidationRule interface {
    Validate(current, target Object) error
}

// Example validation rules
var validationRules = []ValidationRule{
    &EngineChangeValidator{},      // Prevents engine changes
    &ClusterChangeValidator{},     // Prevents cluster changes  
    &SystemObjectValidator{},      // Prevents system modifications
    &TypeCompatibilityValidator{}, // Validates type changes
}
```

### Forbidden Operations

Some operations require manual intervention:

```go
var (
    ErrEngineChange = errors.New("engine type changes not supported")
    ErrClusterChange = errors.New("cluster configuration changes not supported")  
    ErrSystemObject = errors.New("system object modifications not supported")
)
```

### Automatic Handling

Other operations are automatically handled with optimal strategies:

```go
func (g *Generator) shouldUseDropCreate(obj Object) bool {
    // Integration engines require DROP+CREATE
    if isIntegrationEngine(obj.Engine) {
        return true
    }
    
    // Materialized views with query changes
    if obj.Type == MaterializedView && queryChanged(obj) {
        return true
    }
    
    return false
}
```

## Performance Characteristics

### Memory Usage
- **Stateless Parser**: No memory accumulation across operations
- **Efficient Tokenization**: Minimal memory footprint during parsing
- **Structured Trees**: Memory usage scales linearly with schema complexity

### Speed Benchmarks
- **Simple Schemas** (10 objects): <10ms parsing time
- **Complex Schemas** (100+ objects): <100ms parsing time  
- **Large Schemas** (1000+ objects): <1s parsing time

### Scalability
- **File Size**: Handles multi-megabyte schema files efficiently
- **Import Depth**: Supports deep import hierarchies (configurable limit)
- **Concurrent Processing**: Thread-safe for parallel file processing

## Testing Architecture

### Testdata-Driven Testing

Housekeeper uses a comprehensive testdata system:

```
pkg/parser/testdata/
├── database_create.sql      # Input SQL
├── database_create.yaml     # Expected parse result
├── table_complex.sql        # Complex table definition
├── table_complex.yaml       # Expected structured output
└── ...
```

### Test Generation
```bash
# Regenerate test expectations from parsing results
go test -v ./pkg/parser -update
```

### Coverage Areas
- **Parser Tests**: 20+ SQL test files covering all DDL operations
- **Migration Tests**: 15+ YAML scenarios covering all migration patterns
- **Integration Tests**: Docker-based tests with real ClickHouse instances
- **Property Tests**: Randomized testing for edge cases

## Next Steps

- **[Parser Architecture](parser.md)** - Deep dive into the parsing system
- **[Migration Generation](migration-generation.md)** - Detailed migration algorithms
- **[Docker Integration](docker.md)** - Container management and testing
- **[Best Practices](../advanced/best-practices.md)** - Production deployment patterns