package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateView formats a CREATE VIEW statement (both regular and materialized)
func (f *formatter) createView(stmt *parser.CreateViewStmt) string {
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
		// Parse the TO table name to handle database.table format
		parts := strings.Split(*stmt.To, ".")
		if len(parts) == 2 {
			lines = append(lines, f.keyword("TO")+" "+f.qualifiedName(&parts[0], parts[1]))
		} else {
			lines = append(lines, f.keyword("TO")+" "+f.identifier(*stmt.To))
		}
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

	return strings.Join(lines, "\n") + ";"
}

// AttachView formats an ATTACH VIEW statement
func (f *formatter) attachView(stmt *parser.AttachViewStmt) string {
	var parts []string

	parts = append(parts, f.keyword("ATTACH VIEW"))

	if stmt.IfNotExists {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	return strings.Join(parts, " ") + ";"
}

// DetachView formats a DETACH VIEW statement
func (f *formatter) detachView(stmt *parser.DetachViewStmt) string {
	var parts []string

	parts = append(parts, f.keyword("DETACH VIEW"))

	if stmt.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	if stmt.Permanently {
		parts = append(parts, f.keyword("PERMANENTLY"))
	}

	if stmt.Sync {
		parts = append(parts, f.keyword("SYNC"))
	}

	return strings.Join(parts, " ") + ";"
}

// DropView formats a DROP VIEW statement
func (f *formatter) dropView(stmt *parser.DropViewStmt) string {
	var parts []string

	parts = append(parts, f.keyword("DROP VIEW"))

	if stmt.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	if stmt.Sync {
		parts = append(parts, f.keyword("SYNC"))
	}

	return strings.Join(parts, " ") + ";"
}

// formatViewEngine formats a materialized view ENGINE clause with optional DDL
func (f *formatter) formatViewEngine(engine *parser.ViewEngine) string {
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
			if param.String != nil {
				params = append(params, *param.String)
			} else if param.Number != nil {
				params = append(params, *param.Number)
			} else if param.Ident != nil {
				params = append(params, f.identifier(*param.Ident))
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
