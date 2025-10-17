package utils

import (
	"fmt"
	"strings"
)

// SQLBuilder provides a fluent interface for building ClickHouse DDL statements.
// It handles common patterns like cluster injection, identifier backticking,
// and conditional clause building to reduce code duplication across the schema package.
//
// Example usage:
//
//	sql := New().
//		CREATE("DATABASE").
//		Name("analytics").
//		OnCluster("production").
//		Engine("Atomic").
//		Comment("Analytics database").
//		String()
//	// Output: CREATE DATABASE `analytics` ON CLUSTER `production` ENGINE = Atomic COMMENT 'Analytics database';
type SQLBuilder struct {
	parts []string
}

// NewSQLBuilder creates a new SQLBuilder instance.
//
// Example:
//
//	builder := sqlbuilder.NewSQLBuilder()
func NewSQLBuilder() *SQLBuilder {
	return &SQLBuilder{
		parts: make([]string, 0, 10),
	}
}

// Create adds a Create clause with the specified object type.
//
// Example:
//
//	builder.Create("DATABASE")  // Create DATABASE
//	builder.Create("TABLE")     // Create TABLE
func (b *SQLBuilder) Create(objectType string) *SQLBuilder {
	b.parts = append(b.parts, "CREATE", objectType)
	return b
}

// CreateOrReplace adds a CREATE OR REPLACE clause with the specified object type.
//
// Example:
//
//	builder.CreateOrReplace("DICTIONARY")  // CREATE OR REPLACE DICTIONARY
func (b *SQLBuilder) CreateOrReplace(objectType string) *SQLBuilder {
	b.parts = append(b.parts, "CREATE", "OR", "REPLACE", objectType)
	return b
}

// Drop adds a Drop clause with the specified object type.
//
// Example:
//
//	builder.Drop("DATABASE")    // Drop DATABASE
//	builder.Drop("TABLE")       // Drop TABLE
func (b *SQLBuilder) Drop(objectType string) *SQLBuilder {
	b.parts = append(b.parts, "DROP", objectType)
	return b
}

// Alter adds an Alter clause with the specified object type.
//
// Example:
//
//	builder.Alter("DATABASE")   // Alter DATABASE
//	builder.Alter("TABLE")      // Alter TABLE
func (b *SQLBuilder) Alter(objectType string) *SQLBuilder {
	b.parts = append(b.parts, "ALTER", objectType)
	return b
}

// Rename adds a Rename clause with the specified object type.
//
// Example:
//
//	builder.Rename("DATABASE")  // Rename DATABASE
//	builder.Rename("TABLE")     // Rename TABLE
func (b *SQLBuilder) Rename(objectType string) *SQLBuilder {
	b.parts = append(b.parts, "RENAME", objectType)
	return b
}

// IfExists adds an IF EXISTS clause. This should be called after DROP operations.
//
// Example:
//
//	builder.DROP("DATABASE").IfExists()  // DROP DATABASE IF EXISTS
func (b *SQLBuilder) IfExists() *SQLBuilder {
	b.parts = append(b.parts, "IF", "EXISTS")
	return b
}

// IfNotExists adds an IF NOT EXISTS clause. This should be called after CREATE operations.
//
// Example:
//
//	builder.CREATE("DATABASE").IfNotExists()  // CREATE DATABASE IF NOT EXISTS
func (b *SQLBuilder) IfNotExists() *SQLBuilder {
	b.parts = append(b.parts, "IF", "NOT", "EXISTS")
	return b
}

// Name adds a backticked object name.
//
// Example:
//
//	builder.Name("analytics")           // `analytics`
//	builder.Name("db.table")            // `db`.`table`
func (b *SQLBuilder) Name(name string) *SQLBuilder {
	if name != "" {
		b.parts = append(b.parts, BacktickIdentifier(name))
	}
	return b
}

// QualifiedName adds a qualified name with optional database prefix.
// If database is nil or empty, only the name is added with backticks.
//
// Example:
//
//	builder.QualifiedName(nil, "events")              // `events`
//	builder.QualifiedName(&"analytics", "events")     // `analytics`.`events`
func (b *SQLBuilder) QualifiedName(database *string, name string) *SQLBuilder {
	qualifiedName := BacktickQualifiedName(database, name)
	if qualifiedName != "" {
		b.parts = append(b.parts, qualifiedName)
	}
	return b
}

// OnCluster adds an ON CLUSTER clause if cluster is not empty.
//
// Example:
//
//	builder.OnCluster("production")  // ON CLUSTER `production`
//	builder.OnCluster("")            // (nothing added)
func (b *SQLBuilder) OnCluster(cluster string) *SQLBuilder {
	if cluster != "" {
		b.parts = append(b.parts, "ON", "CLUSTER", BacktickIdentifier(cluster))
	}
	return b
}

// Engine adds an ENGINE clause with the specified engine name.
//
// Example:
//
//	builder.Engine("Atomic")         // ENGINE = Atomic
//	builder.Engine("MergeTree()")    // ENGINE = MergeTree()
func (b *SQLBuilder) Engine(engine string) *SQLBuilder {
	if engine != "" {
		b.parts = append(b.parts, "ENGINE", "=", engine)
	}
	return b
}

// Comment adds a COMMENT clause with the specified comment text.
// The comment is automatically quoted and SQL-escaped.
//
// Example:
//
//	builder.Comment("Analytics database")  // COMMENT 'Analytics database'
//	builder.Comment("")                     // (nothing added)
func (b *SQLBuilder) Comment(comment string) *SQLBuilder {
	if comment != "" {
		escapedComment := strings.ReplaceAll(comment, "'", "\\'")
		b.parts = append(b.parts, "COMMENT", fmt.Sprintf("'%s'", escapedComment))
	}
	return b
}

// Escaped adds an escaped SQL string value with single quotes.
// This is useful for cases where you need just the quoted value without a keyword.
//
// Example:
//
//	builder.Modify("COMMENT").Escaped("User's database")  // MODIFY COMMENT 'User\'s database'
//	builder.Raw("DEFAULT").Escaped("hello")               // DEFAULT 'hello'
func (b *SQLBuilder) Escaped(value string) *SQLBuilder {
	if value != "" {
		escapedValue := strings.ReplaceAll(value, "'", "\\'")
		b.parts = append(b.parts, fmt.Sprintf("'%s'", escapedValue))
	}
	return b
}

// To adds a To clause for rename operations.
//
// Example:
//
//	builder.To("new_name")  // To `new_name`
func (b *SQLBuilder) To(name string) *SQLBuilder {
	if name != "" {
		b.parts = append(b.parts, "TO", BacktickIdentifier(name))
	}
	return b
}

// QualifiedTo adds a TO clause with qualified name for rename operations.
//
// Example:
//
//	builder.QualifiedTo(nil, "new_table")           // TO `new_table`
//	builder.QualifiedTo(&"newdb", "new_table")      // TO `newdb`.`new_table`
func (b *SQLBuilder) QualifiedTo(database *string, name string) *SQLBuilder {
	qualifiedName := BacktickQualifiedName(database, name)
	if qualifiedName != "" {
		b.parts = append(b.parts, "TO", qualifiedName)
	}
	return b
}

// Modify adds a Modify clause for ALTER operations.
//
// Example:
//
//	builder.Modify("COMMENT")  // Modify COMMENT
func (b *SQLBuilder) Modify(clause string) *SQLBuilder {
	b.parts = append(b.parts, "MODIFY", clause)
	return b
}

// As adds an As clause for CREATE FUNCTION operations.
//
// Example:
//
//	builder.As("(x) -> x * 2")  // As (x) -> x * 2
func (b *SQLBuilder) As(expression string) *SQLBuilder {
	if expression != "" {
		b.parts = append(b.parts, "AS", expression)
	}
	return b
}

// Raw adds raw SQL text to the builder. Use sparingly for complex constructs
// that don't fit the fluent pattern.
//
// Example:
//
//	builder.Raw("SYNC")  // SYNC
func (b *SQLBuilder) Raw(sql string) *SQLBuilder {
	if sql != "" {
		b.parts = append(b.parts, sql)
	}
	return b
}

// String builds and returns the final SQL statement with a semicolon.
//
// Example:
//
//	sql := builder.Create("DATABASE").Name("test").String()
//	// Returns: "CREATE DATABASE `test`;"
func (b *SQLBuilder) String() string {
	if len(b.parts) == 0 {
		return ""
	}
	return strings.Join(b.parts, " ") + ";"
}

// StringWithoutSemicolon builds and returns the final SQL statement without a semicolon.
// Useful for building parts of larger statements.
//
// Example:
//
//	clause := builder.OnCluster("prod").StringWithoutSemicolon()
//	// Returns: "ON CLUSTER `prod`"
func (b *SQLBuilder) StringWithoutSemicolon() string {
	return strings.Join(b.parts, " ")
}
