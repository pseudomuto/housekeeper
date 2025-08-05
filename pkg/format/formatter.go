// Package format provides well-formatted SQL output for ClickHouse DDL statements.
//
// This package takes parsed parser statements and generates clean, readable SQL
// with consistent formatting, proper indentation, and standardized styling.
// It separates formatting concerns from parsing and migration logic.
//
// Key features:
// - Consistent indentation and spacing
// - Proper line breaks for readability
// - Standardized keyword casing
// - Column alignment in table definitions
// - Comprehensive support for all DDL statement types
//
// Example usage:
//
//	stmt := &parser.CreateTableStmt{
//		Name: "users",
//		Database: stringPtr("analytics"),
//		Columns: []*parser.Column{
//			{Name: "id", Type: &parser.DataType{Simple: &parser.SimpleDataType{Name: "UInt64"}}},
//			{Name: "name", Type: &parser.DataType{Simple: &parser.SimpleDataType{Name: "String"}}},
//		},
//		Engine: &parser.TableEngine{Name: "MergeTree"},
//	}
//
//	formatted := format.Statement(&parser.Statement{CreateTable: stmt})
//	fmt.Println(formatted)
//
// Output:
//
//	CREATE TABLE analytics.users (
//	    id UInt64,
//	    name String
//	)
//	ENGINE = MergeTree();
package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// FormatterOptions controls formatting behavior
type FormatterOptions struct {
	// IndentSize specifies the number of spaces for each indent level
	IndentSize int
	// MaxLineLength suggests when to break long lines (0 = no limit)
	MaxLineLength int
	// UppercaseKeywords whether to uppercase SQL keywords
	UppercaseKeywords bool
	// AlignColumns whether to align column definitions in tables
	AlignColumns bool
}

// DefaultOptions returns standard formatting options
func DefaultOptions() *FormatterOptions {
	return &FormatterOptions{
		IndentSize:        4,
		MaxLineLength:     120,
		UppercaseKeywords: true,
		AlignColumns:      true,
	}
}

// Formatter handles SQL statement formatting with configurable options
type Formatter struct {
	options *FormatterOptions
}

// New creates a new Formatter with the specified options
func New(options *FormatterOptions) *Formatter {
	if options == nil {
		options = DefaultOptions()
	}
	return &Formatter{options: options}
}

// NewDefault creates a new Formatter with default options
func NewDefault() *Formatter {
	return New(DefaultOptions())
}

// Statement formats a complete parser statement
func (f *Formatter) Statement(stmt *parser.Statement) string {
	if stmt == nil {
		return ""
	}

	switch {
	case stmt.CreateDatabase != nil:
		return f.CreateDatabase(stmt.CreateDatabase)
	case stmt.AlterDatabase != nil:
		return f.AlterDatabase(stmt.AlterDatabase)
	case stmt.AttachDatabase != nil:
		return f.AttachDatabase(stmt.AttachDatabase)
	case stmt.DetachDatabase != nil:
		return f.DetachDatabase(stmt.DetachDatabase)
	case stmt.DropDatabase != nil:
		return f.DropDatabase(stmt.DropDatabase)
	case stmt.RenameDatabase != nil:
		return f.RenameDatabase(stmt.RenameDatabase)
	case stmt.CreateTable != nil:
		return f.CreateTable(stmt.CreateTable)
	case stmt.AlterTable != nil:
		return f.AlterTable(stmt.AlterTable)
	case stmt.AttachTable != nil:
		return f.AttachTable(stmt.AttachTable)
	case stmt.DetachTable != nil:
		return f.DetachTable(stmt.DetachTable)
	case stmt.DropTable != nil:
		return f.DropTable(stmt.DropTable)
	case stmt.RenameTable != nil:
		return f.RenameTable(stmt.RenameTable)
	case stmt.CreateDictionary != nil:
		return f.CreateDictionary(stmt.CreateDictionary)
	case stmt.AttachDictionary != nil:
		return f.AttachDictionary(stmt.AttachDictionary)
	case stmt.DetachDictionary != nil:
		return f.DetachDictionary(stmt.DetachDictionary)
	case stmt.DropDictionary != nil:
		return f.DropDictionary(stmt.DropDictionary)
	case stmt.RenameDictionary != nil:
		return f.RenameDictionary(stmt.RenameDictionary)
	case stmt.CreateView != nil:
		return f.CreateView(stmt.CreateView)
	case stmt.AttachView != nil:
		return f.AttachView(stmt.AttachView)
	case stmt.DetachView != nil:
		return f.DetachView(stmt.DetachView)
	case stmt.DropView != nil:
		return f.DropView(stmt.DropView)
	case stmt.SelectStatement != nil:
		return f.SelectStatement(stmt.SelectStatement)
	default:
		return ""
	}
}

// keyword formats a keyword according to the formatter options
func (f *Formatter) keyword(kw string) string {
	if f.options.UppercaseKeywords {
		return strings.ToUpper(kw)
	}
	return strings.ToLower(kw)
}

// indent returns the specified number of indent levels as spaces
func (f *Formatter) indent(level int) string {
	return strings.Repeat(" ", level*f.options.IndentSize)
}

// qualifiedName formats a database-qualified name with backticks
func (f *Formatter) qualifiedName(database *string, name string) string {
	if database != nil && *database != "" {
		return "`" + *database + "`.`" + name + "`"
	}
	return "`" + name + "`"
}

// identifier formats a single identifier with backticks
func (f *Formatter) identifier(name string) string {
	return "`" + name + "`"
}

// Statement formats a single statement (convenience function)
func Statement(stmt *parser.Statement) string {
	return NewDefault().Statement(stmt)
}

// Grammar formats a complete grammar with multiple statements
func (f *Formatter) Grammar(grammar *parser.Grammar) string {
	if grammar == nil || len(grammar.Statements) == 0 {
		return ""
	}

	var formattedStmts []string
	for _, stmt := range grammar.Statements {
		if formatted := f.Statement(stmt); formatted != "" {
			formattedStmts = append(formattedStmts, formatted)
		}
	}

	return strings.Join(formattedStmts, "\n\n")
}

// Grammar formats a complete grammar (convenience function)
func Grammar(grammar *parser.Grammar) string {
	return NewDefault().Grammar(grammar)
}
