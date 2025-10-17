package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// formatCreateFunction formats CREATE FUNCTION statements
func (f *Formatter) formatCreateFunction(w io.Writer, stmt *parser.CreateFunctionStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		if _, err := w.Write([]byte(f.keyword("create") + " " + f.keyword("function") + " ")); err != nil {
			return err
		}

		if _, err := w.Write([]byte(f.identifier(stmt.Name))); err != nil {
			return err
		}

		// Add ON CLUSTER if specified
		if stmt.OnCluster != nil {
			if _, err := w.Write([]byte(" " + f.keyword("on") + " " + f.keyword("cluster") + " ")); err != nil {
				return err
			}
			if _, err := w.Write([]byte(f.identifier(*stmt.OnCluster))); err != nil {
				return err
			}
		}

		// Format AS clause with parameters
		if _, err := w.Write([]byte(" " + f.keyword("as") + " (")); err != nil {
			return err
		}

		// Format parameters
		for i, param := range stmt.Parameters {
			if i > 0 {
				if _, err := w.Write([]byte(", ")); err != nil {
					return err
				}
			}
			if _, err := w.Write([]byte(f.identifier(param.Name))); err != nil {
				return err
			}
		}

		if _, err := w.Write([]byte(") -> ")); err != nil {
			return err
		}

		// Format the expression with multi-line context if needed
		// Calculate base indentation for the expression (length of "CREATE FUNCTION name ON CLUSTER cluster AS (params) -> ")
		baseIndent := len("CREATE FUNCTION ") + len(stmt.Name)
		if stmt.OnCluster != nil {
			baseIndent += len(" ON CLUSTER ") + len(*stmt.OnCluster)
		}
		baseIndent += len(" AS (")
		for i, param := range stmt.Parameters {
			if i > 0 {
				baseIndent += len(", ")
			}
			baseIndent += len(param.Name)
		}
		baseIndent += len(") -> ")

		// Only use multi-line context if the formatter has multi-line functions enabled
		// Let the individual function calls decide if they need multi-line formatting
		exprStr := f.formatExpressionWithContext(stmt.Expression, false, baseIndent)
		if _, err := w.Write([]byte(exprStr)); err != nil {
			return err
		}

		if _, err := w.Write([]byte(";")); err != nil {
			return err
		}

		return nil
	})
}

// formatDropFunction formats DROP FUNCTION statements
func (f *Formatter) formatDropFunction(w io.Writer, stmt *parser.DropFunctionStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		parts = append(parts, f.keyword("drop")+" "+f.keyword("function"))

		if stmt.IfExists {
			parts = append(parts, f.keyword("if")+" "+f.keyword("exists"))
		}

		parts = append(parts, f.identifier(stmt.Name))

		// Add ON CLUSTER if specified
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("on")+" "+f.keyword("cluster"), f.identifier(*stmt.OnCluster))
		}

		if _, err := w.Write([]byte(strings.Join(parts, " "))); err != nil {
			return err
		}

		if _, err := w.Write([]byte(";")); err != nil {
			return err
		}

		return nil
	})
}
