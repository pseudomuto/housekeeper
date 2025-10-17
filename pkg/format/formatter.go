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
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
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
		// MultilineFunctions enables multi-line formatting for complex function expressions
		MultilineFunctions bool
		// FunctionArgThreshold is the number of function arguments that triggers multi-line formatting
		FunctionArgThreshold int
		// MultilineFunctionNames contains function names that should always be formatted multi-line
		MultilineFunctionNames []string
		// FunctionIndentSize specifies extra indentation for function arguments (defaults to IndentSize)
		FunctionIndentSize int
		// SmartFunctionPairing enables intelligent argument pairing for specific functions
		SmartFunctionPairing bool
		// PairedFunctionNames contains function names that should use argument pairing
		PairedFunctionNames []string
		// PairSize specifies how many arguments to group per line (default: 2)
		PairSize int
	}

	// Formatter handles SQL statement formatting with configurable options
	Formatter struct {
		options *FormatterOptions
	}
)

// Defaults provides the standard formatting options for ClickHouse DDL statements.
//
// These defaults use:
//   - 4-space indentation
//   - Uppercase SQL keywords
//   - Column alignment in table definitions
//   - 120 character line length suggestion
//   - Multi-line formatting for complex functions
//   - 4+ arguments triggers multi-line formatting
//   - Specific functions always formatted multi-line
//   - Smart argument pairing for conditional functions
var Defaults = FormatterOptions{
	IndentSize:             4,
	MaxLineLength:          120,
	UppercaseKeywords:      true,
	AlignColumns:           true,
	MultilineFunctions:     true,
	FunctionArgThreshold:   4,
	MultilineFunctionNames: []string{"multiIf", "case", "transform", "multiSearchAllPositions"},
	FunctionIndentSize:     4,
	SmartFunctionPairing:   true,
	PairedFunctionNames:    []string{"multiIf", "if", "case", "transform"},
	PairSize:               2,
}

// New creates a new Formatter with the specified options.
//
// To use default options, pass format.Defaults. The returned Formatter
// can be used to format ClickHouse DDL statements with consistent styling.
//
// Example:
//
//	import (
//		"bytes"
//		"fmt"
//		"github.com/pseudomuto/housekeeper/pkg/format"
//		"github.com/pseudomuto/housekeeper/pkg/parser"
//	)
//
//	// Create formatter with default options
//	formatter := format.New(format.Defaults)
//
//	// Create formatter with custom options
//	formatter := format.New(format.FormatterOptions{
//		IndentSize:        2,
//		UppercaseKeywords: false,
//		AlignColumns:      true,
//	})
//
//	// Parse SQL statement
//	sqlResult, err := parser.ParseString("CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree();")
//	if err != nil {
//		panic(err)
//	}
//
//	// Format to buffer
//	var buf bytes.Buffer
//	err = formatter.Format(&buf, sqlResult.Statements...)
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Println(buf.String())
//	// Output:
//	// CREATE TABLE `users` (
//	//     `id`   UInt64,
//	//     `name` String
//	// )
//	// ENGINE = MergeTree();

// New creates a new Formatter instance with the specified formatting options.
// Use this when you need to format multiple statements with the same options.
//
// Example:
//
//	// Create formatter with custom options
//	formatter := format.New(format.FormatterOptions{
//		IndentSize:        4,
//		UppercaseKeywords: true,
//		AlignColumns:      true,
//	})
//
//	// Format multiple statements
//	var buf1, buf2 bytes.Buffer
//	err := formatter.Format(&buf1, stmt1, stmt2)
//	err = formatter.Format(&buf2, stmt3, stmt4)
func New(options FormatterOptions) *Formatter {
	return &Formatter{options: &options}
}

// Format provides a convenient way to format statements without creating a Formatter instance.
//
// This is equivalent to calling New(&opts).Format(w, statements...) but more concise
// when you only need to format statements once.
//
// Example:
//
//	import (
//		"bytes"
//		"github.com/pseudomuto/housekeeper/pkg/format"
//		"github.com/pseudomuto/housekeeper/pkg/parser"
//	)
//
//	sql, _ := parser.ParseString("CREATE DATABASE test;")
//	var buf bytes.Buffer
//	err := format.Format(&buf, format.Defaults, sqlResult.Statements...)
//	if err != nil {
//		panic(err)
//	}
func Format(w io.Writer, opts FormatterOptions, statements ...*parser.Statement) error {
	return New(opts).Format(w, statements...)
}

// FormatSQL provides a convenient way to format all statements from a parsed SQL structure.
//
// This is equivalent to calling Format(w, opts, sqlResult.Statements...) but more concise
// when you have a complete parsed SQL object.
//
// Example:
//
//	import (
//		"bytes"
//		"github.com/pseudomuto/housekeeper/pkg/format"
//		"github.com/pseudomuto/housekeeper/pkg/parser"
//	)
//
//	sqlResult, _ := parser.ParseString("CREATE DATABASE test; CREATE TABLE test.users (id UInt64) ENGINE = MergeTree();")
//	var buf bytes.Buffer
//	err := format.FormatSQL(&buf, format.Defaults, sqlResult)
//	if err != nil {
//		panic(err)
//	}
func FormatSQL(w io.Writer, opts FormatterOptions, sql *parser.SQL) error {
	if sql == nil {
		return nil
	}
	return Format(w, opts, sql.Statements...)
}

// Format writes formatted SQL statements to the provided writer.
//
// Each statement is formatted according to the formatter's configuration and
// written to the writer. Sequential comments have no blank lines between them,
// while comments and SQL statements are separated by blank lines.
// Any write errors are returned immediately.
//
// Parameters:
//   - w: The io.Writer to write formatted SQL to
//   - statements: Variable number of parsed statements to format
//
// Returns an error if any write operation fails, nil otherwise.
func (f *Formatter) Format(w io.Writer, statements ...*parser.Statement) error {
	if len(statements) == 0 {
		return nil
	}

	first := true
	var prevWasComment bool

	for _, stmt := range statements {
		if stmt == nil {
			continue
		}

		currentIsComment := stmt.CommentStatement != nil

		if !first { // nolint: nestif
			// Sequential comments: single newline
			// Comment to SQL or SQL to comment: double newline
			// SQL to SQL: double newline
			if prevWasComment && currentIsComment {
				if _, err := w.Write([]byte("\n")); err != nil {
					return err
				}
			} else {
				if _, err := w.Write([]byte("\n\n")); err != nil {
					return err
				}
			}
		}

		if err := f.statement(w, stmt); err != nil {
			return err
		}

		first = false
		prevWasComment = currentIsComment
	}
	return nil
}

// FormatSQL formats all statements from a parsed SQL structure using this formatter's configuration.
//
// This is equivalent to calling f.Format(w, sqlResult.Statements...) but more concise
// when you have a complete parsed SQL object.
func (f *Formatter) FormatSQL(w io.Writer, sql *parser.SQL) error {
	if sql == nil {
		return nil
	}
	return f.Format(w, sql.Statements...)
}

// statement formats a complete parser statement
func (f *Formatter) statement(w io.Writer, stmt *parser.Statement) error {
	if stmt == nil {
		return nil
	}

	// Use a slice of formatting functions for cleaner code structure
	steps := []func(io.Writer, *parser.Statement) error{
		f.formatDatabaseStatements,
		f.formatTableStatements,
		f.formatDictionaryStatements,
		f.formatViewStatements,
		f.formatRoleStatements,
		f.formatFunctionStatements,
		f.formatOtherStatements,
	}

	for _, step := range steps {
		if err := step(w, stmt); err != nil {
			return err
		}
	}

	return nil
}

// formatDatabaseStatements handles database-related statements
func (f *Formatter) formatDatabaseStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateDatabase != nil:
		return f.createDatabase(w, stmt.CreateDatabase)
	case stmt.AlterDatabase != nil:
		return f.alterDatabase(w, stmt.AlterDatabase)
	case stmt.AttachDatabase != nil:
		return f.attachDatabase(w, stmt.AttachDatabase)
	case stmt.DetachDatabase != nil:
		return f.detachDatabase(w, stmt.DetachDatabase)
	case stmt.DropDatabase != nil:
		return f.dropDatabase(w, stmt.DropDatabase)
	case stmt.RenameDatabase != nil:
		return f.renameDatabase(w, stmt.RenameDatabase)
	}
	return nil
}

// formatTableStatements handles table-related statements
func (f *Formatter) formatTableStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateTable != nil:
		return f.createTable(w, stmt.CreateTable)
	case stmt.AlterTable != nil:
		return f.alterTable(w, stmt.AlterTable)
	case stmt.AttachTable != nil:
		return f.attachTable(w, stmt.AttachTable)
	case stmt.DetachTable != nil:
		return f.detachTable(w, stmt.DetachTable)
	case stmt.DropTable != nil:
		return f.dropTable(w, stmt.DropTable)
	case stmt.RenameTable != nil:
		return f.renameTable(w, stmt.RenameTable)
	}
	return nil
}

// formatDictionaryStatements handles dictionary-related statements
func (f *Formatter) formatDictionaryStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateDictionary != nil:
		return f.createDictionary(w, stmt.CreateDictionary)
	case stmt.CreateNamedCollection != nil:
		return f.createNamedCollection(w, stmt.CreateNamedCollection)
	case stmt.AlterNamedCollection != nil:
		return f.alterNamedCollection(w, stmt.AlterNamedCollection)
	case stmt.DropNamedCollection != nil:
		return f.dropNamedCollection(w, stmt.DropNamedCollection)
	case stmt.AttachDictionary != nil:
		return f.attachDictionary(w, stmt.AttachDictionary)
	case stmt.DetachDictionary != nil:
		return f.detachDictionary(w, stmt.DetachDictionary)
	case stmt.DropDictionary != nil:
		return f.dropDictionary(w, stmt.DropDictionary)
	case stmt.RenameDictionary != nil:
		return f.renameDictionary(w, stmt.RenameDictionary)
	}
	return nil
}

// formatViewStatements handles view-related statements
func (f *Formatter) formatViewStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateView != nil:
		return f.createView(w, stmt.CreateView)
	case stmt.AttachView != nil:
		return f.attachView(w, stmt.AttachView)
	case stmt.DetachView != nil:
		return f.detachView(w, stmt.DetachView)
	case stmt.DropView != nil:
		return f.dropView(w, stmt.DropView)
	}
	return nil
}

// formatRoleStatements handles role-related statements
func (f *Formatter) formatRoleStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateRole != nil:
		return f.createRole(w, stmt.CreateRole)
	case stmt.AlterRole != nil:
		return f.alterRole(w, stmt.AlterRole)
	case stmt.DropRole != nil:
		return f.dropRole(w, stmt.DropRole)
	case stmt.SetRole != nil:
		return f.setRole(w, stmt.SetRole)
	case stmt.SetDefaultRole != nil:
		return f.setDefaultRole(w, stmt.SetDefaultRole)
	case stmt.Grant != nil:
		return f.grant(w, stmt.Grant)
	case stmt.Revoke != nil:
		return f.revoke(w, stmt.Revoke)
	}
	return nil
}

// formatFunctionStatements handles function-related statements
func (f *Formatter) formatFunctionStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CreateFunction != nil:
		return f.formatCreateFunction(w, stmt.CreateFunction)
	case stmt.DropFunction != nil:
		return f.formatDropFunction(w, stmt.DropFunction)
	}
	return nil
}

// formatOtherStatements handles other statements
func (f *Formatter) formatOtherStatements(w io.Writer, stmt *parser.Statement) error {
	switch {
	case stmt.CommentStatement != nil:
		return f.formatCommentStatement(w, stmt.CommentStatement)
	case stmt.SelectStatement != nil:
		return f.selectStatement(w, stmt.SelectStatement)
	}
	return nil
}

// keyword formats a keyword according to the formatter options
func (f *Formatter) keyword(kw string) string {
	if f.options.UppercaseKeywords {
		return strings.ToUpper(kw)
	}
	return strings.ToLower(kw)
}

// indent returns the specified number of indent levels as spaces
func (f *Formatter) indent(level int) string { // nolint: unparam
	return strings.Repeat(" ", level*f.options.IndentSize)
}

// qualifiedName formats a database-qualified name with backticks
func (f *Formatter) qualifiedName(database *string, name string) string {
	return utils.BacktickQualifiedName(database, name)
}

// identifier formats a single identifier with backticks
func (f *Formatter) identifier(name string) string {
	return utils.BacktickIdentifier(name)
}

// commentable represents any statement that can have leading and trailing comments
type commentable interface {
	GetLeadingComments() []string
	GetTrailingComments() []string
}

// formatWithComments wraps statement formatting with automatic comment handling.
// It formats leading comments, calls the provided format function for the core statement,
// then formats trailing comments. This provides a DRY approach to comment handling
// across all statement types.
func (f *Formatter) formatWithComments(w io.Writer, stmt commentable, formatCore func(io.Writer) error) error {
	// Format leading comments
	if comments := stmt.GetLeadingComments(); len(comments) > 0 {
		if err := f.formatCommentSequence(w, comments); err != nil {
			return err
		}
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
	}

	// Format the core statement
	if err := formatCore(w); err != nil {
		return err
	}

	// Format trailing comments
	if comments := stmt.GetTrailingComments(); len(comments) > 0 {
		if _, err := w.Write([]byte("\n\n")); err != nil {
			return err
		}
		if err := f.formatCommentSequence(w, comments); err != nil {
			return err
		}
	}

	return nil
}

// formatCommentStatement formats a standalone comment statement
func (f *Formatter) formatCommentStatement(w io.Writer, stmt *parser.CommentStatement) error {
	// Preserve comments exactly as they are
	_, err := w.Write([]byte(stmt.Comment))
	return err
}

// formatCommentSequence formats a sequence of comments with proper line breaks
func (f *Formatter) formatCommentSequence(w io.Writer, comments []string) error {
	for i, comment := range comments {
		if i > 0 {
			// Sequential comments: single newline only
			if _, err := w.Write([]byte("\n")); err != nil {
				return err
			}
		}
		if _, err := w.Write([]byte(comment)); err != nil {
			return err
		}
	}
	return nil
}
