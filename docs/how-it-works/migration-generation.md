# Migration Generation

Learn how Housekeeper's intelligent migration generation system creates optimal DDL migrations for ClickHouse.

## Overview

Housekeeper's migration generation is the core process that transforms schema differences into executable ClickHouse DDL statements. The system employs sophisticated algorithms to ensure migrations are safe, efficient, and maintain data integrity.

## Migration Generation Pipeline

```
┌─────────────────┐    ┌─────────────────┐
│ Target Schema   │    │ Current Schema  │
│ (Compiled)      │    │ (From Database) │
└─────────┬───────┘    └─────────┬───────┘
          │                      │
          └──────────┬───────────┘
                     │
          ┌─────────────────┐
          │ Schema Differ   │
          │ (Comparison)    │
          └─────────┬───────┘
                     │
          ┌─────────────────┐
          │ Change Set      │
          │ Classification  │
          └─────────┬───────┘
                     │
          ┌─────────────────┐
          │ Strategy        │
          │ Selection       │
          └─────────┬───────┘
                     │
          ┌─────────────────┐
          │ DDL Generation  │
          │ & Ordering      │
          └─────────┬───────┘
                     │
          ┌─────────────────┐
          │ Migration File  │
          │ Output          │
          └─────────────────┘
```

## Schema Comparison Algorithm

### Object Detection

The comparison engine identifies four types of changes:

```go
type SchemaChanges struct {
    Added    []DatabaseObject  // Objects in target but not in current
    Modified []ObjectChange    // Objects that exist in both but differ
    Removed  []DatabaseObject  // Objects in current but not in target
    Renamed  []RenameChange    // Objects that appear to be renamed
}
```

### Property Comparison

For each object type, Housekeeper compares specific properties:

#### Database Comparison
```go
func compareDatabases(current, target *Database) []Change {
    var changes []Change
    
    // Compare engine (immutable - requires manual intervention)
    if current.Engine != target.Engine {
        return []Change{{Type: "error", Error: ErrEngineChange}}
    }
    
    // Compare cluster (immutable - requires manual intervention)
    if current.OnCluster != target.OnCluster {
        return []Change{{Type: "error", Error: ErrClusterChange}}
    }
    
    // Compare comment (can be altered)
    if current.Comment != target.Comment {
        changes = append(changes, Change{
            Type: "alter_comment",
            SQL:  fmt.Sprintf("ALTER DATABASE %s MODIFY COMMENT '%s'", target.Name, target.Comment),
        })
    }
    
    return changes
}
```

#### Table Comparison
```go
func compareTables(current, target *Table) []Change {
    var changes []Change
    
    // Engine changes not supported
    if current.Engine.Name != target.Engine.Name {
        return []Change{{Type: "error", Error: ErrEngineChange}}
    }
    
    // For integration engines, use DROP+CREATE strategy
    if isIntegrationEngine(current.Engine.Name) {
        return []Change{{
            Type: "drop_create",
            SQL:  generateDropCreateSQL(current, target),
        }}
    }
    
    // Compare columns
    columnChanges := compareColumns(current.Columns, target.Columns)
    changes = append(changes, columnChanges...)
    
    // Compare table properties
    if current.OrderBy != target.OrderBy {
        changes = append(changes, Change{
            Type: "modify_order_by",
            SQL:  fmt.Sprintf("ALTER TABLE %s MODIFY ORDER BY %s", target.Name, target.OrderBy),
        })
    }
    
    return changes
}
```

### Rename Detection Algorithm

Housekeeper includes sophisticated rename detection to avoid unnecessary DROP+CREATE operations:

```go
func detectRenames(current, target []DatabaseObject) []RenameChange {
    var renames []RenameChange
    var matched []int // Track which target objects are matched
    
    for _, currentObj := range current {
        bestMatch := -1
        bestScore := 0.0
        
        for i, targetObj := range target {
            // Skip already matched objects
            if contains(matched, i) {
                continue
            }
            
            // Calculate similarity score
            score := calculateSimilarity(currentObj, targetObj)
            
            // Require high similarity for rename detection
            if score > 0.95 && score > bestScore {
                bestMatch = i
                bestScore = score
            }
        }
        
        if bestMatch != -1 {
            renames = append(renames, RenameChange{
                From: currentObj,
                To:   target[bestMatch],
            })
            matched = append(matched, bestMatch)
        }
    }
    
    return renames
}

func calculateSimilarity(obj1, obj2 DatabaseObject) float64 {
    switch obj1.Type {
    case "table":
        return compareTableProperties(obj1.(*Table), obj2.(*Table))
    case "dictionary":
        return compareDictionaryProperties(obj1.(*Dictionary), obj2.(*Dictionary))
    case "view":
        return compareViewProperties(obj1.(*View), obj2.(*View))
    default:
        return 0.0
    }
}

func compareTableProperties(t1, t2 *Table) float64 {
    score := 0.0
    totalChecks := 0
    
    // Engine must match exactly
    if t1.Engine.Name == t2.Engine.Name {
        score += 0.3
    } else {
        return 0.0 // Engine mismatch disqualifies rename
    }
    totalChecks++
    
    // Columns must match exactly
    if compareColumns(t1.Columns, t2.Columns) == 0 {
        score += 0.4
    } else {
        return 0.0 // Column mismatch disqualifies rename
    }
    totalChecks++
    
    // Other properties (ORDER BY, PARTITION BY, etc.)
    if t1.OrderBy == t2.OrderBy {
        score += 0.1
    }
    totalChecks++
    
    if t1.PartitionBy == t2.PartitionBy {
        score += 0.1
    }
    totalChecks++
    
    if t1.Comment == t2.Comment {
        score += 0.1
    }
    totalChecks++
    
    return score / totalChecks
}
```

## Migration Strategies

### Strategy Selection Matrix

| Object Type | Change Type | Strategy | Implementation |
|-------------|-------------|----------|----------------|
| Database | Create | Direct DDL | `CREATE DATABASE ...` |
| Database | Comment | ALTER | `ALTER DATABASE ... MODIFY COMMENT` |
| Database | Engine/Cluster | Error | Manual intervention required |
| Table (Standard) | Add Column | ALTER | `ALTER TABLE ... ADD COLUMN` |
| Table (Standard) | Drop Column | ALTER | `ALTER TABLE ... DROP COLUMN` |
| Table (Standard) | Modify Column | ALTER | `ALTER TABLE ... MODIFY COLUMN` |
| Table (Integration) | Any Change | DROP+CREATE | More reliable for read-only engines |
| Dictionary | Any Change | CREATE OR REPLACE | No ALTER DICTIONARY support |
| View (Regular) | Query Change | CREATE OR REPLACE | Supported by ClickHouse |
| View (Materialized) | Query Change | DROP+CREATE | More reliable than ALTER |

### Strategy Implementation

#### Standard Table Operations
```go
func generateTableAlterMigration(current, target *Table) Migration {
    var statements []string
    
    // Generate column operations
    columnChanges := compareColumns(current.Columns, target.Columns)
    
    for _, change := range columnChanges {
        switch change.Type {
        case "add_column":
            stmt := fmt.Sprintf(
                "ALTER TABLE %s ADD COLUMN %s %s",
                target.FullName(),
                change.Column.Name,
                change.Column.Type,
            )
            if change.Column.Default != nil {
                stmt += fmt.Sprintf(" DEFAULT %s", change.Column.Default)
            }
            statements = append(statements, stmt)
            
        case "drop_column":
            statements = append(statements, fmt.Sprintf(
                "ALTER TABLE %s DROP COLUMN %s",
                target.FullName(),
                change.Column.Name,
            ))
            
        case "modify_column":
            statements = append(statements, fmt.Sprintf(
                "ALTER TABLE %s MODIFY COLUMN %s %s",
                target.FullName(),
                change.Column.Name,
                change.Column.Type,
            ))
        }
    }
    
    return Migration{
        Type:       "alter_table",
        Statements: statements,
    }
}
```

#### Integration Engine Strategy
```go
func generateIntegrationEngineStrategy(current, target *Table) Migration {
    return Migration{
        Type: "drop_create",
        Statements: []string{
            fmt.Sprintf("DROP TABLE IF EXISTS %s", current.FullName()),
            generateCreateTableSQL(target),
        },
        Comment: "Integration engine tables require DROP+CREATE for modifications",
    }
}
```

#### Dictionary Strategy
```go
func generateDictionaryStrategy(current, target *Dictionary) Migration {
    // Dictionaries always use CREATE OR REPLACE
    return Migration{
        Type: "create_or_replace",
        Statements: []string{
            generateCreateDictionarySQL(target, true), // true = OR REPLACE
        },
        Comment: "Dictionaries use CREATE OR REPLACE (no ALTER DICTIONARY support)",
    }
}
```

#### Materialized View Strategy
```go
func generateMaterializedViewStrategy(current, target *MaterializedView) Migration {
    if queryChanged(current, target) {
        // Use DROP+CREATE for query changes
        return Migration{
            Type: "drop_create",
            Statements: []string{
                fmt.Sprintf("DROP TABLE %s", current.FullName()), // MV uses DROP TABLE
                generateCreateMaterializedViewSQL(target),
            },
            Comment: "Materialized view query change requires DROP+CREATE",
        }
    }
    
    // Other changes can use ALTER TABLE
    return generateAlterTableMigration(current.AsTable(), target.AsTable())
}
```

## Operation Ordering

### Dependency Resolution

Housekeeper ensures safe migration ordering by analyzing object dependencies:

```go
type DependencyGraph struct {
    Nodes []DatabaseObject
    Edges []Dependency
}

type Dependency struct {
    From DatabaseObject
    To   DatabaseObject
    Type DependencyType // "table_reference", "dictionary_source", etc.
}

func buildDependencyGraph(schema *Schema) *DependencyGraph {
    graph := &DependencyGraph{}
    
    // Add all objects as nodes
    for _, db := range schema.Databases {
        graph.Nodes = append(graph.Nodes, db)
        
        // Named collections are global but tracked per database for organization
        for _, collection := range db.NamedCollections {
            graph.Nodes = append(graph.Nodes, collection)
        }
        
        for _, table := range db.Tables {
            graph.Nodes = append(graph.Nodes, table)
            
            // Table may depend on named collection (integration engines)
            if table.Engine.UsesNamedCollection() {
                namedCollection := findNamedCollection(schema, table.Engine.NamedCollection)
                if namedCollection != nil {
                    graph.Edges = append(graph.Edges, Dependency{
                        From: table,
                        To:   namedCollection,
                        Type: "named_collection_reference",
                    })
                }
            }
        }
        
        for _, dict := range db.Dictionaries {
            graph.Nodes = append(graph.Nodes, dict)
            
            // Dictionary depends on source table
            if dict.Source.Type == "CLICKHOUSE" {
                sourceTable := findTable(schema, dict.Source.Database, dict.Source.Table)
                if sourceTable != nil {
                    graph.Edges = append(graph.Edges, Dependency{
                        From: dict,
                        To:   sourceTable,
                        Type: "dictionary_source",
                    })
                }
            }
        }
        
        for _, view := range db.Views {
            graph.Nodes = append(graph.Nodes, view)
            
            // View depends on referenced tables
            referencedTables := extractTableReferences(view.Query)
            for _, table := range referencedTables {
                graph.Edges = append(graph.Edges, Dependency{
                    From: view,
                    To:   table,
                    Type: "table_reference",
                })
            }
        }
    }
    
    return graph
}
```

### Migration Ordering Algorithm

```go
func orderMigrations(changes []Change) []Change {
    // UP migration order (creation order with dependencies)
    order := []string{
        "create_database",
        "create_named_collection",  // Before tables (integration engines may use them)
        "create_table", 
        "create_dictionary",        // After tables (may reference tables)
        "create_view",             // Last (may reference tables and dictionaries)
        "alter_database",
        "alter_table",
        "rename_named_collection",
        "rename_table",
        "rename_dictionary",
        "drop_view",               // First to drop (depends on tables/dictionaries)
        "drop_dictionary",         // Before tables (references them)
        "drop_table",              // Before named collections (may use them)
        "drop_named_collection",   // After tables that use them
        "drop_database",           // Last (contains everything)
    }
    
    // Sort changes by dependency order
    var ordered []Change
    for _, changeType := range order {
        for _, change := range changes {
            if change.Type == changeType {
                ordered = append(ordered, change)
            }
        }
    }
    
    return ordered
}
```

## Migration File Generation

### File Structure

```go
type MigrationFile struct {
    Timestamp   time.Time
    Version     string
    Description string
    Up          []string
    Down        []string
    Checksum    string
}

func generateMigrationFile(changes []Change) *MigrationFile {
    timestamp := time.Now().UTC()
    
    file := &MigrationFile{
        Timestamp:   timestamp,
        Version:     timestamp.Format("20060102150405"),
        Description: generateDescription(changes),
        Up:          generateUpStatements(changes),
        Down:        generateDownStatements(changes),
    }
    
    file.Checksum = calculateChecksum(file.Up)
    
    return file
}
```

### File Content Generation

```go
func generateFileContent(migration *MigrationFile) string {
    var buf strings.Builder
    
    // Header
    buf.WriteString(fmt.Sprintf("-- Schema migration generated at %s\n", 
        migration.Timestamp.Format("2006-01-02 15:04:05 UTC")))
    buf.WriteString("-- Down migration: swap current and target schemas and regenerate\n\n")
    
    // Migration description
    if migration.Description != "" {
        buf.WriteString(fmt.Sprintf("-- %s\n\n", migration.Description))
    }
    
    // UP statements
    for i, stmt := range migration.Up {
        if i > 0 {
            buf.WriteString("\n")
        }
        
        // Add contextual comments
        if comment := getStatementComment(stmt); comment != "" {
            buf.WriteString(fmt.Sprintf("-- %s\n", comment))
        }
        
        buf.WriteString(stmt)
        if !strings.HasSuffix(stmt, ";") {
            buf.WriteString(";")
        }
        buf.WriteString("\n")
    }
    
    return buf.String()
}

func getStatementComment(stmt string) string {
    switch {
    case strings.HasPrefix(stmt, "CREATE DATABASE"):
        return "Create database"
    case strings.HasPrefix(stmt, "CREATE TABLE"):
        tableName := extractTableName(stmt)
        return fmt.Sprintf("Create table '%s'", tableName)
    case strings.HasPrefix(stmt, "ALTER TABLE"):
        tableName := extractTableName(stmt)
        return fmt.Sprintf("Alter table '%s'", tableName)
    case strings.HasPrefix(stmt, "RENAME"):
        return "Rename operation"
    default:
        return ""
    }
}
```

## Error Handling and Validation

### Validation Rules

```go
type ValidationRule interface {
    Validate(change Change) error
}

type EngineChangeValidator struct{}

func (v *EngineChangeValidator) Validate(change Change) error {
    if change.Type == "modify_engine" {
        return errors.Wrap(ErrEngineChange, 
            "table engine changes require manual DROP+CREATE")
    }
    return nil
}

type ClusterChangeValidator struct{}

func (v *ClusterChangeValidator) Validate(change Change) error {
    if change.Type == "modify_cluster" {
        return errors.Wrap(ErrClusterChange,
            "cluster configuration changes require manual intervention")
    }
    return nil
}

type SystemObjectValidator struct{}

func (v *SystemObjectValidator) Validate(change Change) error {
    if isSystemObject(change.Object) {
        return errors.Wrap(ErrSystemObject,
            "system object modifications are not allowed")
    }
    return nil
}
```

### Migration Validation

```go
func validateMigration(migration *MigrationFile) error {
    validators := []ValidationRule{
        &EngineChangeValidator{},
        &ClusterChangeValidator{},
        &SystemObjectValidator{},
        &TypeCompatibilityValidator{},
    }
    
    for _, stmt := range migration.Up {
        change := parseStatement(stmt)
        
        for _, validator := range validators {
            if err := validator.Validate(change); err != nil {
                return fmt.Errorf("validation failed for statement '%s': %w", stmt, err)
            }
        }
    }
    
    return nil
}
```

## Performance Optimizations

### Batch Operations

```go
func optimizeMigration(migration *MigrationFile) *MigrationFile {
    optimized := &MigrationFile{
        Timestamp:   migration.Timestamp,
        Version:     migration.Version,
        Description: migration.Description,
    }
    
    // Group related operations
    groups := groupRelatedOperations(migration.Up)
    
    for _, group := range groups {
        if canBatch(group) {
            optimized.Up = append(optimized.Up, batchStatements(group)...)
        } else {
            optimized.Up = append(optimized.Up, group...)
        }
    }
    
    return optimized
}

func canBatch(statements []string) bool {
    // Can batch multiple ADD COLUMN operations on same table
    if allSameTable(statements) && allAddColumn(statements) {
        return true
    }
    
    return false
}

func batchStatements(statements []string) []string {
    // Combine multiple ADD COLUMN into single ALTER TABLE
    tableName := extractTableName(statements[0])
    
    var columns []string
    for _, stmt := range statements {
        column := extractColumnDefinition(stmt)
        columns = append(columns, column)
    }
    
    batchedStmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s",
        tableName, strings.Join(columns, ", ADD COLUMN "))
    
    return []string{batchedStmt}
}
```

## Next Steps

- **[Parser Architecture](parser.md)** - Understand how DDL is parsed
- **[Docker Integration](docker.md)** - Test migrations with containers
- **[Overview](overview.md)** - High-level system architecture
- **[Best Practices](../advanced/best-practices.md)** - Production deployment patterns