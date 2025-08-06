package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// SelectStatement formats a top-level SELECT statement
func (f *Formatter) selectStatement(w io.Writer, stmt *parser.TopLevelSelectStatement) error {
	if stmt == nil {
		return nil
	}

	result := f.formatSelectStatement(&stmt.SelectStatement)
	if result != "" {
		result += ";"
		_, err := w.Write([]byte(result))
		return err
	}
	return nil
}

// formatSelectStatement formats a SELECT statement for views and subqueries
func (f *Formatter) formatSelectStatement(stmt *parser.SelectStatement) string {
	if stmt == nil {
		return ""
	}

	var lines []string

	// WITH clause (CTEs)
	if stmt.With != nil {
		lines = append(lines, f.formatWithClause(stmt.With))
	}

	// SELECT clause with columns
	selectLine := f.keyword("SELECT")
	if stmt.Distinct {
		selectLine += " " + f.keyword("DISTINCT")
	}

	// Format columns
	lines = f.appendSelectColumns(lines, selectLine, stmt.Columns)

	// FROM clause
	if stmt.From != nil {
		lines = append(lines, f.formatFromClause(stmt.From))
	}

	// WHERE clause
	if stmt.Where != nil {
		lines = append(lines, f.formatWhereClause(stmt.Where))
	}

	// GROUP BY clause
	if stmt.GroupBy != nil {
		lines = append(lines, f.formatGroupByClause(stmt.GroupBy))
	}

	// HAVING clause
	if stmt.Having != nil {
		lines = append(lines, f.formatHavingClause(stmt.Having))
	}

	// ORDER BY clause
	if stmt.OrderBy != nil {
		lines = append(lines, f.formatSelectOrderByClause(stmt.OrderBy))
	}

	// LIMIT clause
	if stmt.Limit != nil {
		lines = append(lines, f.formatLimitClause(stmt.Limit))
	}

	// SETTINGS clause
	if stmt.Settings != nil {
		lines = append(lines, f.formatSettingsClause(stmt.Settings))
	}

	return strings.Join(lines, "\n")
}

// formatWithClause formats WITH clause for CTEs
func (f *Formatter) formatWithClause(with *parser.WithClause) string {
	if with == nil || len(with.CTEs) == 0 {
		return ""
	}

	lines := make([]string, 0, len(with.CTEs)*6+1) // Approximate capacity
	lines = append(lines, f.keyword("WITH"))
	for i, cte := range with.CTEs {
		// Start the CTE definition
		cteHeader := "    " + f.identifier(cte.Name) + " " + f.keyword("AS") + " ("
		lines = append(lines, cteHeader)

		// Format the SELECT statement with additional indentation
		selectContent := f.formatSelectStatement(cte.Query)
		selectLines := strings.Split(selectContent, "\n")
		for _, line := range selectLines {
			if line != "" {
				lines = append(lines, "        "+line) // Extra 4 spaces for CTE content
			}
		}

		// Closing parenthesis on its own line
		if i == len(with.CTEs)-1 {
			// Last CTE - no comma
			lines = append(lines, "    )")
		} else {
			// Not last CTE - add comma
			lines = append(lines, "    ),")
		}
	}

	return strings.Join(lines, "\n")
}

// formatSelectColumns formats the column list in SELECT
func (f *Formatter) formatSelectColumns(columns []parser.SelectColumn) []string {
	var result []string
	for _, col := range columns {
		if col.Star != nil {
			result = append(result, "*")
		} else if col.Expression != nil {
			colStr := f.formatExpression(col.Expression)
			if col.Alias != nil {
				colStr += " " + f.keyword("AS") + " " + f.identifier(*col.Alias)
			}
			result = append(result, colStr)
		}
	}
	return result
}

// formatFromClause formats FROM clause with joins
func (f *Formatter) formatFromClause(from *parser.FromClause) string {
	if from == nil {
		return ""
	}

	result := f.keyword("FROM") + " " + f.formatTableRef(&from.Table)

	// Add joins
	for _, join := range from.Joins {
		result += "\n" + f.formatJoinClause(&join)
	}

	return result
}

// formatTableRef formats a table reference
func (f *Formatter) formatTableRef(ref *parser.TableRef) string {
	if ref == nil {
		return ""
	}

	if ref.TableName != nil {
		return f.formatTableNameWithAlias(ref.TableName)
	} else if ref.Subquery != nil {
		return f.formatSubqueryWithAlias(ref.Subquery)
	} else if ref.Function != nil {
		return f.formatFunctionWithAlias(ref.Function)
	}
	return ""
}

// formatTableNameWithAlias formats table name with optional alias
func (f *Formatter) formatTableNameWithAlias(table *parser.TableNameWithAlias) string {
	if table == nil {
		return ""
	}

	result := f.qualifiedName(table.Database, table.Table)
	if table.Alias != nil && table.Alias.Name != nil {
		result += " " + f.keyword("AS") + " " + f.identifier(*table.Alias.Name)
	}
	return result
}

// formatSubqueryWithAlias formats subquery with optional alias
func (f *Formatter) formatSubqueryWithAlias(sub *parser.SubqueryWithAlias) string {
	if sub == nil {
		return ""
	}

	result := "(" + f.formatSelectStatement(&sub.Subquery) + ")"
	if sub.Alias != nil {
		result += " " + f.keyword("AS") + " " + f.identifier(*sub.Alias)
	}
	return result
}

// formatFunctionWithAlias formats function with optional alias
func (f *Formatter) formatFunctionWithAlias(fn *parser.FunctionWithAlias) string {
	if fn == nil {
		return ""
	}

	result := fn.Function.Name
	if len(fn.Function.Arguments) > 0 {
		var params []string
		for _, arg := range fn.Function.Arguments {
			params = append(params, f.formatFunctionArg(&arg))
		}
		result += "(" + strings.Join(params, ", ") + ")"
	} else {
		result += "()"
	}

	if fn.Alias != nil {
		result += " " + f.keyword("AS") + " " + f.identifier(*fn.Alias)
	}
	return result
}

// formatFunctionArg formats function argument
func (f *Formatter) formatFunctionArg(arg *parser.FunctionArg) string {
	if arg == nil {
		return ""
	}

	if arg.Star != nil {
		return "*"
	}
	if arg.Expression != nil {
		return f.formatExpression(arg.Expression)
	}
	return ""
}

// formatJoinClause formats JOIN clauses
func (f *Formatter) formatJoinClause(join *parser.JoinClause) string {
	if join == nil {
		return ""
	}

	result := ""
	if join.Type != "" {
		result = f.keyword(join.Type) + " " + f.keyword(join.Join)
	} else {
		result = f.keyword(join.Join) // Default JOIN
	}

	result += " " + f.formatTableRef(&join.Table)

	if join.Condition != nil {
		if join.Condition.On != nil {
			result += " " + f.keyword("ON") + " " + f.formatExpression(join.Condition.On)
		} else if len(join.Condition.Using) > 0 {
			var cols []string
			for _, col := range join.Condition.Using {
				cols = append(cols, f.identifier(col))
			}
			result += " " + f.keyword("USING") + " (" + strings.Join(cols, ", ") + ")"
		}
	}

	return result
}

// formatWhereClause formats WHERE clause
func (f *Formatter) formatWhereClause(where *parser.WhereClause) string {
	if where == nil {
		return ""
	}
	return f.keyword("WHERE") + " " + f.formatExpression(&where.Condition)
}

// formatGroupByClause formats GROUP BY clause
func (f *Formatter) formatGroupByClause(groupBy *parser.GroupByClause) string {
	if groupBy == nil || len(groupBy.Columns) == 0 {
		return ""
	}

	exprs := make([]string, 0, len(groupBy.Columns))
	for _, expr := range groupBy.Columns {
		exprs = append(exprs, f.formatExpression(&expr))
	}
	result := f.keyword("GROUP BY") + " " + strings.Join(exprs, ", ")

	if groupBy.WithClause != nil {
		result += " " + f.keyword("WITH") + " " + f.keyword(*groupBy.WithClause)
	}

	return result
}

// formatHavingClause formats HAVING clause
func (f *Formatter) formatHavingClause(having *parser.HavingClause) string {
	if having == nil {
		return ""
	}
	return f.keyword("HAVING") + " " + f.formatExpression(&having.Condition)
}

// formatSelectOrderByClause formats ORDER BY clause for SELECT
func (f *Formatter) formatSelectOrderByClause(orderBy *parser.SelectOrderByClause) string {
	if orderBy == nil || len(orderBy.Columns) == 0 {
		return ""
	}

	items := make([]string, 0, len(orderBy.Columns))
	for _, item := range orderBy.Columns {
		itemStr := f.formatExpression(&item.Expression)
		if item.Direction != nil {
			itemStr += " " + f.keyword(*item.Direction)
		}
		if item.Nulls != nil {
			itemStr += " " + f.keyword("NULLS") + " " + f.keyword(*item.Nulls)
		}
		if item.Collate != nil {
			itemStr += " " + f.keyword("COLLATE") + " " + *item.Collate
		}
		items = append(items, itemStr)
	}
	return f.keyword("ORDER BY") + " " + strings.Join(items, ", ")
}

// formatLimitClause formats LIMIT clause
func (f *Formatter) formatLimitClause(limit *parser.LimitClause) string {
	if limit == nil {
		return ""
	}

	result := f.keyword("LIMIT") + " " + f.formatExpression(&limit.Count)
	if limit.Offset != nil {
		result += " " + f.keyword("OFFSET") + " " + f.formatExpression(&limit.Offset.Value)
	}
	return result
}

// formatSettingsClause formats SETTINGS clause for SELECT
func (f *Formatter) formatSettingsClause(settings *parser.SettingsClause) string {
	if settings == nil || len(settings.Values) == 0 {
		return ""
	}

	settingStrs := make([]string, 0, len(settings.Values))
	for _, setting := range settings.Values {
		settingStrs = append(settingStrs, f.identifier(setting.Key)+" = "+f.formatExpression(&setting.Value))
	}
	return f.keyword("SETTINGS") + " " + strings.Join(settingStrs, ", ")
}

// appendSelectColumns appends SELECT columns to lines, handling single/multiple line formatting
func (f *Formatter) appendSelectColumns(lines []string, selectLine string, columns []parser.SelectColumn) []string {
	if len(columns) == 0 {
		return append(lines, selectLine)
	}

	columnLines := f.formatSelectColumns(columns)
	if len(columnLines) == 1 {
		// Single line if short
		return append(lines, selectLine+" "+columnLines[0])
	}

	// Multiple lines for better readability
	lines = append(lines, selectLine)
	for i, col := range columnLines {
		if i == len(columnLines)-1 {
			lines = append(lines, "    "+col) // No comma on last column
		} else {
			lines = append(lines, "    "+col+",")
		}
	}
	return lines
}
