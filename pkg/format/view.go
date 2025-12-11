package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateView formats a CREATE VIEW statement (both regular and materialized)
func (f *Formatter) createView(w io.Writer, stmt *parser.CreateViewStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var lines []string

		// Build the header line
		var headerParts []string
		headerParts = append(headerParts, f.keyword("CREATE"))

		if stmt.OrReplace {
			headerParts = append(headerParts, f.keyword("OR REPLACE"))
		}

		if stmt.Materialized {
			headerParts = append(headerParts, f.keyword("MATERIALIZED"))
		}

		headerParts = append(headerParts, f.keyword("VIEW"))

		if stmt.IfNotExists {
			headerParts = append(headerParts, f.keyword("IF NOT EXISTS"))
		}

		headerParts = append(headerParts, f.qualifiedName(stmt.Database, stmt.Name))

		if stmt.OnCluster != nil {
			headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		lines = append(lines, strings.Join(headerParts, " "))

		// TO table (for materialized views)
		if stmt.To != nil {
			toClause := f.keyword("TO") + " "
			// Format either table reference or table function
			if stmt.To.Table != nil {
				toClause += f.qualifiedName(stmt.To.Database, *stmt.To.Table)
			} else if stmt.To.Function != nil {
				toClause += f.formatViewTableFunction(stmt.To.Function)
			}
			lines = append(lines, toClause)
		}

		// ENGINE (for materialized views)
		if stmt.Engine != nil {
			lines = append(lines, f.formatViewEngine(stmt.Engine))
		}

		// POPULATE (for materialized views)
		if stmt.Populate {
			lines = append(lines, f.keyword("POPULATE"))
		}

		// AS SELECT
		if stmt.AsSelect != nil {
			lines = append(lines, f.keyword("AS")+" "+f.formatSelectStatement(stmt.AsSelect))
		}

		_, err := w.Write([]byte(strings.Join(lines, "\n") + ";"))
		return err
	})
}

// AttachView formats an ATTACH VIEW statement
func (f *Formatter) attachView(w io.Writer, stmt *parser.AttachViewStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildAttachStatement("VIEW", stmt.IfNotExists, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)

		return ddl.formatBasicDDL(w, parts)
	})
}

// DetachView formats a DETACH VIEW statement
func (f *Formatter) detachView(w io.Writer, stmt *parser.DetachViewStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildDetachStatement("VIEW", stmt.IfExists, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)
		parts = ddl.appendPermanently(parts, stmt.Permanently)
		parts = ddl.appendSync(parts, stmt.Sync)

		return ddl.formatBasicDDL(w, parts)
	})
}

// DropView formats a DROP VIEW statement
func (f *Formatter) dropView(w io.Writer, stmt *parser.DropViewStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildDropStatement("VIEW", stmt.IfExists, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)
		parts = ddl.appendSync(parts, stmt.Sync)

		return ddl.formatBasicDDL(w, parts)
	})
}

// formatViewEngine formats a materialized view ENGINE clause with optional DDL
func (f *Formatter) formatViewEngine(engine *parser.ViewEngine) string {
	if engine == nil {
		return ""
	}

	var parts []string

	// ENGINE = EngineName
	engineStr := f.keyword("ENGINE") + " = " + engine.Name

	// Add parameters if present
	if len(engine.Parameters) > 0 {
		engineStr += "("
		var params []string
		for _, param := range engine.Parameters {
			if param.Expression != nil {
				params = append(params, f.formatEngineParameter(param.Expression))
			} else if param.Ident != nil {
				params = append(params, f.identifier(*param.Ident))
			} else {
				params = append(params, *param.String)
			}
		}
		engineStr += strings.Join(params, ", ")
		engineStr += ")"
	} else {
		// Always add parentheses for consistency with table engines
		engineStr += "()"
	}

	parts = append(parts, engineStr)

	// Add optional clauses
	if engine.OrderBy != nil {
		parts = append(parts, f.keyword("ORDER BY")+" "+f.formatExpression(&engine.OrderBy.Expression))
	}

	if engine.PartitionBy != nil {
		parts = append(parts, f.keyword("PARTITION BY")+" "+f.formatExpression(&engine.PartitionBy.Expression))
	}

	if engine.PrimaryKey != nil {
		parts = append(parts, f.keyword("PRIMARY KEY")+" "+f.formatExpression(&engine.PrimaryKey.Expression))
	}

	if engine.SampleBy != nil {
		parts = append(parts, f.keyword("SAMPLE BY")+" "+f.formatExpression(&engine.SampleBy.Expression))
	}

	return strings.Join(parts, " ")
}

// formatViewTableFunction formats a table function call in view's TO clause
func (f *Formatter) formatViewTableFunction(fn *parser.TableFunction) string {
	if fn == nil {
		return ""
	}

	result := f.identifier(fn.Name)
	if len(fn.Arguments) > 0 {
		var params []string
		for _, arg := range fn.Arguments {
			params = append(params, f.formatFunctionArg(&arg))
		}
		result += "(" + strings.Join(params, ", ") + ")"
	} else {
		result += "()"
	}
	return result
}
