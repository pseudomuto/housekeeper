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
//	stmt := &parser.Statement{CreateTable: &parser.CreateTableStmt{
//		Name: "users",
//		Database: stringPtr("analytics"),
//		Columns: []*parser.Column{
//			{Name: "id", Type: &parser.DataType{Simple: &parser.SimpleDataType{Name: "UInt64"}}},
//			{Name: "name", Type: &parser.DataType{Simple: &parser.SimpleDataType{Name: "String"}}},
//		},
//		Engine: &parser.TableEngine{Name: "MergeTree"},
//	}}
//
//	formatted := format.Format(nil, stmt)
//	fmt.Println(formatted)
//
// Output:
//
//	CREATE TABLE `analytics`.`users` (
//	    `id` UInt64,
//	    `name` String
//	) ENGINE = MergeTree();
package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

type (
	// FormatterOptions controls formatting behavior
	FormatterOptions struct {
		// IndentSize specifies the number of spaces for each indent level
		IndentSize int
		// MaxLineLength suggests when to break long lines (0 = no limit)
		MaxLineLength int
		// UppercaseKeywords whether to uppercase SQL keywords
		UppercaseKeywords bool
		// AlignColumns whether to align column definitions in tables
		AlignColumns bool
	}

	// formatter handles SQL statement formatting with configurable options
	formatter struct {
		options *FormatterOptions
	}
)

// DefaultOptions returns standard formatting options
func DefaultOptions() *FormatterOptions {
	return &FormatterOptions{
		IndentSize:        4,
		MaxLineLength:     120,
		UppercaseKeywords: true,
		AlignColumns:      true,
	}
}

// Format formats one or more statements with the given options
func Format(options *FormatterOptions, statements ...*parser.Statement) string {
	if len(statements) == 0 {
		return ""
	}

	f := newFormatter(options)
	formattedStmts := make([]string, 0, len(statements))

	for _, stmt := range statements {
		if formatted := f.statement(stmt); formatted != "" {
			formattedStmts = append(formattedStmts, formatted)
		}
	}

	return strings.Join(formattedStmts, "\n\n")
}

// statement formats a complete parser statement
func (f *formatter) statement(stmt *parser.Statement) string {
	if stmt == nil {
		return ""
	}

	switch {
	case stmt.CreateDatabase != nil:
		return f.createDatabase(stmt.CreateDatabase)
	case stmt.AlterDatabase != nil:
		return f.alterDatabase(stmt.AlterDatabase)
	case stmt.AttachDatabase != nil:
		return f.attachDatabase(stmt.AttachDatabase)
	case stmt.DetachDatabase != nil:
		return f.detachDatabase(stmt.DetachDatabase)
	case stmt.DropDatabase != nil:
		return f.dropDatabase(stmt.DropDatabase)
	case stmt.RenameDatabase != nil:
		return f.renameDatabase(stmt.RenameDatabase)
	case stmt.CreateTable != nil:
		return f.createTable(stmt.CreateTable)
	case stmt.AlterTable != nil:
		return f.alterTable(stmt.AlterTable)
	case stmt.AttachTable != nil:
		return f.attachTable(stmt.AttachTable)
	case stmt.DetachTable != nil:
		return f.detachTable(stmt.DetachTable)
	case stmt.DropTable != nil:
		return f.dropTable(stmt.DropTable)
	case stmt.RenameTable != nil:
		return f.renameTable(stmt.RenameTable)
	case stmt.CreateDictionary != nil:
		return f.createDictionary(stmt.CreateDictionary)
	case stmt.AttachDictionary != nil:
		return f.attachDictionary(stmt.AttachDictionary)
	case stmt.DetachDictionary != nil:
		return f.detachDictionary(stmt.DetachDictionary)
	case stmt.DropDictionary != nil:
		return f.dropDictionary(stmt.DropDictionary)
	case stmt.RenameDictionary != nil:
		return f.renameDictionary(stmt.RenameDictionary)
	case stmt.CreateView != nil:
		return f.createView(stmt.CreateView)
	case stmt.AttachView != nil:
		return f.attachView(stmt.AttachView)
	case stmt.DetachView != nil:
		return f.detachView(stmt.DetachView)
	case stmt.DropView != nil:
		return f.dropView(stmt.DropView)
	case stmt.SelectStatement != nil:
		return f.selectStatement(stmt.SelectStatement)
	default:
		return ""
	}
}

// keyword formats a keyword according to the formatter options
func (f *formatter) keyword(kw string) string {
	if f.options.UppercaseKeywords {
		return strings.ToUpper(kw)
	}
	return strings.ToLower(kw)
}

// indent returns the specified number of indent levels as spaces
func (f *formatter) indent(level int) string {
	return strings.Repeat(" ", level*f.options.IndentSize)
}

// qualifiedName formats a database-qualified name with backticks
func (f *formatter) qualifiedName(database *string, name string) string {
	if database != nil && *database != "" {
		return "`" + *database + "`.`" + name + "`"
	}
	return "`" + name + "`"
}

// identifier formats a single identifier with backticks
func (f *formatter) identifier(name string) string {
	return "`" + name + "`"
}

// newFormatter creates a new formatter with the specified options
func newFormatter(options *FormatterOptions) *formatter {
	if options == nil {
		options = DefaultOptions()
	}
	return &formatter{options: options}
}
