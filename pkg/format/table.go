package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateTable formats a CREATE TABLE statement with proper indentation and alignment
func (f *Formatter) createTable(w io.Writer, stmt *parser.CreateTableStmt) error {
	// Format leading comments first
	if err := f.formatLeadingComments(w, stmt.LeadingComments); err != nil {
		return err
	}

	lines := f.buildCreateTableHeader(stmt)

	// Only append elements and closing paren if not using AS syntax
	if stmt.AsTable == nil {
		lines = f.appendTableElements(lines, stmt.Elements)
		lines = append(lines, ")")
	}

	// Format pre-engine comments
	if len(stmt.PreEngineComments) > 0 {
		if err := f.writePreEngineComments(w, lines, stmt.PreEngineComments); err != nil {
			return err
		}
		lines = lines[:0] // Clear lines since we already wrote them
	}

	lines = f.appendEngineAndClauses(lines, stmt)

	// Write the main statement
	if _, err := w.Write([]byte(strings.Join(lines, "\n") + ";")); err != nil {
		return err
	}

	// Format trailing comments
	return f.formatTrailingComments(w, stmt.TrailingComments)
}

// buildCreateTableHeader builds the CREATE TABLE header line
func (f *Formatter) buildCreateTableHeader(stmt *parser.CreateTableStmt) []string {
	var headerParts []string
	headerParts = append(headerParts, f.keyword("CREATE"))

	if stmt.OrReplace {
		headerParts = append(headerParts, f.keyword("OR REPLACE"))
	}

	headerParts = append(headerParts, f.keyword("TABLE"))

	if stmt.IfNotExists {
		headerParts = append(headerParts, f.keyword("IF NOT EXISTS"))
	}

	headerParts = append(headerParts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// Handle AS clause
	if stmt.AsTable != nil {
		headerParts = append(headerParts, f.keyword("AS"))
		// Format either table function or table reference
		if stmt.AsTable.Function != nil {
			headerParts = append(headerParts, f.formatTableFunction(stmt.AsTable.Function))
		} else if stmt.AsTable.TableRef != nil {
			asTableName := f.qualifiedName(stmt.AsTable.TableRef.Database, stmt.AsTable.TableRef.Table)
			headerParts = append(headerParts, asTableName)
		}
		// No opening parenthesis when using AS syntax
		return []string{strings.Join(headerParts, " ")}
	}

	return []string{strings.Join(headerParts, " ") + " ("}
}

// appendTableElements formats and appends table elements (columns, indexes, constraints)
func (f *Formatter) appendTableElements(lines []string, elements []parser.TableElement) []string {
	// Convert slice to pointer slice
	elementPtrs := make([]*parser.TableElement, len(elements))
	for i := range elements {
		elementPtrs[i] = &elements[i]
	}

	maxWidth := f.calculateMaxNameWidth(elementPtrs)

	// Format each element individually with proper comma handling
	for i, element := range elementPtrs {
		// Handle leading comments for this element
		if element.Column != nil && len(element.Column.LeadingComments) > 0 {
			for _, comment := range element.Column.LeadingComments {
				lines = append(lines, f.indent(1)+comment)
			}
		}

		// Format the actual element
		elementLine := f.formatTableElement(element, maxWidth)

		// Add comma to element (but not to the last element)
		if i < len(elementPtrs)-1 {
			elementLine += ","
		}

		lines = append(lines, f.indent(1)+elementLine)
	}

	return lines
}

// formatTableElement formats a single table element (column, index, or constraint)
func (f *Formatter) formatTableElement(element *parser.TableElement, maxWidth int) string {
	if element.Column != nil {
		return f.formatColumnWithoutComments(element.Column, maxWidth)
	}
	if element.Index != nil {
		return f.formatIndexDefinition(element.Index)
	}
	if element.Constraint != nil {
		return f.formatConstraintDefinition(element.Constraint)
	}
	return ""
}

// writePreEngineComments writes lines and pre-engine comments to writer
func (f *Formatter) writePreEngineComments(w io.Writer, lines []string, comments []string) error {
	if _, err := w.Write([]byte(strings.Join(lines, "\n"))); err != nil {
		return err
	}
	if _, err := w.Write([]byte("\n")); err != nil {
		return err
	}
	if err := f.formatCommentSequence(w, comments); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n"))
	return err
}

// appendEngineAndClauses appends engine and table clauses to lines
func (f *Formatter) appendEngineAndClauses(lines []string, stmt *parser.CreateTableStmt) []string {
	// Format table options
	if stmt.Engine != nil {
		lines = append(lines, f.keyword("ENGINE")+" = "+f.formatTableEngine(stmt.Engine))

		// Handle trailing comments from engine
		if len(stmt.Engine.TrailingComments) > 0 {
			lines = append(lines, stmt.Engine.TrailingComments...)
		}
	}

	// Sort clauses into canonical ClickHouse order before formatting
	sortedClauses := f.sortTableClauses(stmt.Clauses)

	// Format clauses with their comments
	for _, clause := range sortedClauses {
		lines = f.appendTableClause(lines, clause)
	}

	if stmt.Comment != nil {
		lines = append(lines, f.keyword("COMMENT")+" "+*stmt.Comment)
	}

	return lines
}

// appendTableClause appends a single table clause with its comments
func (f *Formatter) appendTableClause(lines []string, clause *parser.TableClause) []string {
	// Leading comments for the clause
	if len(clause.LeadingComments) > 0 {
		lines = append(lines, clause.LeadingComments...)
	}

	// Format the clause itself
	lines = append(lines, f.formatTableClauseType(clause))

	// Trailing comments for the clause
	if len(clause.TrailingComments) > 0 {
		lines = append(lines, clause.TrailingComments...)
	}

	return lines
}

// sortTableClauses sorts table clauses into ClickHouse's canonical order.
// This ensures consistent output regardless of parse order or ClickHouse version.
// Order: ORDER BY, PARTITION BY, PRIMARY KEY, SAMPLE BY, TTL, SETTINGS
func (f *Formatter) sortTableClauses(clauses []parser.TableClause) []*parser.TableClause {
	if len(clauses) == 0 {
		return nil
	}

	result := make([]*parser.TableClause, 0, len(clauses))

	// Convert to pointers for consistent handling
	for i := range clauses {
		result = append(result, &clauses[i])
	}

	// Sort by clause type priority
	clausePriority := func(clause *parser.TableClause) int {
		switch {
		case clause.OrderBy != nil:
			return 1
		case clause.PartitionBy != nil:
			return 2
		case clause.PrimaryKey != nil:
			return 3
		case clause.SampleBy != nil:
			return 4
		case clause.TTL != nil:
			return 5
		case clause.Settings != nil:
			return 6
		default:
			return 99
		}
	}

	// Simple insertion sort since we typically have very few clauses
	for i := 1; i < len(result); i++ {
		j := i
		for j > 0 && clausePriority(result[j]) < clausePriority(result[j-1]) {
			result[j], result[j-1] = result[j-1], result[j]
			j--
		}
	}

	return result
}

// formatTableClauseType formats a specific type of table clause
func (f *Formatter) formatTableClauseType(clause *parser.TableClause) string {
	if clause.OrderBy != nil {
		return f.keyword("ORDER BY") + " " + f.formatExpression(&clause.OrderBy.Expression)
	}
	if clause.PartitionBy != nil {
		return f.keyword("PARTITION BY") + " " + f.formatExpression(&clause.PartitionBy.Expression)
	}
	if clause.PrimaryKey != nil {
		return f.keyword("PRIMARY KEY") + " " + f.formatExpression(&clause.PrimaryKey.Expression)
	}
	if clause.SampleBy != nil {
		return f.keyword("SAMPLE BY") + " " + f.formatExpression(&clause.SampleBy.Expression)
	}
	if clause.TTL != nil {
		return f.keyword("TTL") + " " + f.formatExpression(&clause.TTL.Expression)
	}
	if clause.Settings != nil && len(clause.Settings.Settings) > 0 {
		return f.formatTableSettings(clause.Settings)
	}
	return ""
}

// formatLeadingComments formats leading comments with proper spacing
func (f *Formatter) formatLeadingComments(w io.Writer, comments []string) error {
	if len(comments) == 0 {
		return nil
	}
	if err := f.formatCommentSequence(w, comments); err != nil {
		return err
	}
	_, err := w.Write([]byte("\n\n"))
	return err
}

// formatTrailingComments formats trailing comments with proper spacing
func (f *Formatter) formatTrailingComments(w io.Writer, comments []string) error {
	if len(comments) == 0 {
		return nil
	}
	if _, err := w.Write([]byte("\n\n")); err != nil {
		return err
	}
	return f.formatCommentSequence(w, comments)
}

// AlterTable formats an ALTER TABLE statement
func (f *Formatter) alterTable(w io.Writer, stmt *parser.AlterTableStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		lines := make([]string, 0, len(stmt.Operations)+1) // Header + operations

		// Header
		var headerParts []string
		headerParts = append(headerParts, f.keyword("ALTER TABLE"))
		headerParts = append(headerParts, f.qualifiedName(stmt.Database, stmt.Name))

		if stmt.OnCluster != nil {
			headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		lines = append(lines, strings.Join(headerParts, " "))

		// Format operations
		for i, op := range stmt.Operations {
			opLine := f.indent(1) + f.formatAlterOperation(&op)

			// Add comma to the end of the line if not the last operation
			if i < len(stmt.Operations)-1 {
				opLine += ","
			}

			lines = append(lines, opLine)
		}

		_, err := w.Write([]byte(strings.Join(lines, "\n") + ";"))
		return err
	})
}

// AttachTable formats an ATTACH TABLE statement
func (f *Formatter) attachTable(w io.Writer, stmt *parser.AttachTableStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		parts = append(parts, f.keyword("ATTACH TABLE"))

		if stmt.IfNotExists {
			parts = append(parts, f.keyword("IF NOT EXISTS"))
		}

		parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// DetachTable formats a DETACH TABLE statement
func (f *Formatter) detachTable(w io.Writer, stmt *parser.DetachTableStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		parts = append(parts, f.keyword("DETACH TABLE"))

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

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// DropTable formats a DROP TABLE statement
func (f *Formatter) dropTable(w io.Writer, stmt *parser.DropTableStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		parts = append(parts, f.keyword("DROP TABLE"))

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

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// RenameTable formats a RENAME TABLE statement
func (f *Formatter) renameTable(w io.Writer, stmt *parser.RenameTableStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		parts = append(parts, f.keyword("RENAME TABLE"))

		renameParts := make([]string, 0, len(stmt.Renames))
		for _, rename := range stmt.Renames {
			fromName := f.qualifiedName(rename.FromDatabase, rename.FromName)
			toName := f.qualifiedName(rename.ToDatabase, rename.ToName)
			renameParts = append(renameParts, fromName+" "+f.keyword("TO")+" "+toName)
		}
		parts = append(parts, strings.Join(renameParts, ", "))

		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// formatColumn formats a single column definition with leading and trailing comments
func (f *Formatter) formatColumn(col *parser.Column, alignWidth int) string {
	var parts []string

	// Leading comments (inline with column)
	if len(col.LeadingComments) > 0 {
		parts = append(parts, col.LeadingComments...)
	}

	// Column name (with optional alignment)
	name := f.identifier(col.Name)
	if alignWidth > 0 && f.options.AlignColumns {
		name = padRight(name, alignWidth)
	}
	parts = append(parts, name)

	// Data type
	parts = append(parts, f.formatDataType(col.DataType))

	// Note: ClickHouse columns don't have NULL/NOT NULL constraints like other databases
	// Instead they use Nullable(T) data types

	// Default value
	if defaultClause := col.GetDefault(); defaultClause != nil {
		parts = append(parts, f.keyword(defaultClause.Type))
		parts = append(parts, f.formatExpression(&defaultClause.Expression))
	}

	// Codec
	if codecClause := col.GetCodec(); codecClause != nil {
		parts = append(parts, f.formatCodec(codecClause))
	}

	// TTL
	if ttlClause := col.GetTTL(); ttlClause != nil {
		parts = append(parts, f.keyword("TTL"), f.formatExpression(&ttlClause.Expression))
	}

	// Comment
	if comment := col.GetComment(); comment != nil {
		parts = append(parts, f.keyword("COMMENT"), *comment)
	}

	// Trailing comments (inline with column)
	if len(col.TrailingComments) > 0 {
		parts = append(parts, col.TrailingComments...)
	}

	return strings.Join(parts, " ")
}

// formatColumnWithoutComments formats a single column definition without leading/trailing comments
func (f *Formatter) formatColumnWithoutComments(col *parser.Column, alignWidth int) string {
	var parts []string

	// Column name (with optional alignment)
	name := f.identifier(col.Name)
	if alignWidth > 0 && f.options.AlignColumns {
		name = padRight(name, alignWidth)
	}
	parts = append(parts, name)

	// Data type
	parts = append(parts, f.formatDataType(col.DataType))

	// Default value
	if defaultClause := col.GetDefault(); defaultClause != nil {
		parts = append(parts, f.keyword(defaultClause.Type))
		parts = append(parts, f.formatExpression(&defaultClause.Expression))
	}

	// Codec
	if codecClause := col.GetCodec(); codecClause != nil {
		parts = append(parts, f.formatCodec(codecClause))
	}

	// TTL
	if ttlClause := col.GetTTL(); ttlClause != nil {
		parts = append(parts, f.keyword("TTL"), f.formatExpression(&ttlClause.Expression))
	}

	// Comment
	if comment := col.GetComment(); comment != nil {
		parts = append(parts, f.keyword("COMMENT"), *comment)
	}

	return strings.Join(parts, " ")
}

// formatIndexDefinition formats an index definition
func (f *Formatter) formatIndexDefinition(idx *parser.IndexDefinition) string {
	var parts []string

	parts = append(parts, f.keyword("INDEX"))
	parts = append(parts, f.identifier(idx.Name))
	parts = append(parts, f.formatExpression(&idx.Expression))
	parts = append(parts, f.keyword("TYPE"), idx.Type)

	if idx.Granularity != nil {
		parts = append(parts, f.keyword("GRANULARITY"), *idx.Granularity)
	}

	return strings.Join(parts, " ")
}

// formatConstraintDefinition formats a constraint definition
func (f *Formatter) formatConstraintDefinition(constraint *parser.ConstraintDefinition) string {
	var parts []string

	parts = append(parts, f.keyword("CONSTRAINT"))
	parts = append(parts, f.identifier(constraint.Name))
	parts = append(parts, f.keyword("CHECK"))
	parts = append(parts, f.formatExpression(&constraint.Expression))

	return strings.Join(parts, " ")
}

// formatTableEngine formats a table engine specification
func (f *Formatter) formatTableEngine(engine *parser.TableEngine) string {
	if engine == nil {
		return ""
	}

	result := engine.Name
	// Always add parentheses for consistency
	result += "("
	if len(engine.Parameters) > 0 {
		var params []string
		for _, param := range engine.Parameters {
			if param.Expression != nil {
				params = append(params, f.formatEngineParameter(param.Expression))
			} else if param.String != nil {
				params = append(params, *param.String)
			} else if param.Number != nil {
				params = append(params, *param.Number)
			} else if param.Ident != nil {
				params = append(params, f.identifier(*param.Ident))
			}
		}
		result += strings.Join(params, ", ")
	}
	result += ")"
	return result
}

// formatEngineParameter formats an expression in engine parameters with special handling for simple identifiers
func (f *Formatter) formatEngineParameter(expr *parser.Expression) string {
	// Check if this is a simple identifier that should be backticked
	if isSimpleIdentifier(expr) {
		identifier := getSimpleIdentifierName(expr)
		return f.identifier(identifier)
	}
	// Otherwise format as regular expression
	return f.formatExpression(expr)
}

// isSimpleIdentifier checks if an expression is just a simple identifier (no dots, functions, etc.)
func isSimpleIdentifier(expr *parser.Expression) bool {
	if expr == nil || expr.Or == nil || expr.Or.And == nil || expr.Or.And.Not == nil {
		return false
	}

	comparison := expr.Or.And.Not.Comparison
	if comparison == nil || comparison.Addition == nil || comparison.Addition.Multiplication == nil {
		return false
	}

	unary := comparison.Addition.Multiplication.Unary
	if unary == nil || unary.Primary == nil || unary.Primary.Identifier == nil {
		return false
	}

	// Check if it's a simple identifier (no database or table qualifiers)
	identifier := unary.Primary.Identifier
	return identifier.Database == nil && identifier.Table == nil && identifier.Name != ""
}

// getSimpleIdentifierName extracts the identifier name from a simple identifier expression
func getSimpleIdentifierName(expr *parser.Expression) string {
	if !isSimpleIdentifier(expr) {
		return ""
	}
	return expr.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Identifier.Name
}

// formatTableSettings formats the SETTINGS clause
func (f *Formatter) formatTableSettings(settings *parser.TableSettingsClause) string {
	if settings == nil || len(settings.Settings) == 0 {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("SETTINGS"))

	settingParts := make([]string, 0, len(settings.Settings))
	for _, setting := range settings.Settings {
		settingParts = append(settingParts, setting.Name+" = "+setting.Value)
	}
	parts = append(parts, strings.Join(settingParts, ", "))

	return strings.Join(parts, " ")
}

// formatAlterOperation formats a single ALTER TABLE operation
func (f *Formatter) formatAlterOperation(op *parser.AlterTableOperation) string {
	switch {
	case op.AddColumn != nil:
		return f.formatAddColumn(op.AddColumn)
	case op.DropColumn != nil:
		return f.formatDropColumn(op.DropColumn)
	case op.ModifyColumn != nil:
		return f.formatModifyColumn(op.ModifyColumn)
	case op.RenameColumn != nil:
		return f.formatRenameColumn(op.RenameColumn)
	case op.CommentColumn != nil:
		return f.formatCommentColumn(op.CommentColumn)
	case op.ClearColumn != nil:
		return f.formatClearColumn(op.ClearColumn)
	case op.AddIndex != nil:
		return f.formatAddIndex(op.AddIndex)
	case op.DropIndex != nil:
		return f.formatDropIndex(op.DropIndex)
	case op.AddConstraint != nil:
		return f.formatAddConstraint(op.AddConstraint)
	case op.DropConstraint != nil:
		return f.formatDropConstraint(op.DropConstraint)
	default:
		return ""
	}
}

// Helper functions for ALTER operations
func (f *Formatter) formatAddColumn(op *parser.AddColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("ADD COLUMN"))

	if op.IfNotExists {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.formatColumn(&op.Column, 0))

	if op.After != nil {
		parts = append(parts, f.keyword("AFTER"), f.identifier(*op.After))
	} else if op.First {
		parts = append(parts, f.keyword("FIRST"))
	}

	return strings.Join(parts, " ")
}

func (f *Formatter) formatDropColumn(op *parser.DropColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("DROP COLUMN"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))
	return strings.Join(parts, " ")
}

func (f *Formatter) formatModifyColumn(op *parser.ModifyColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("MODIFY COLUMN"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))
	if op.Type != nil {
		parts = append(parts, f.formatDataType(op.Type))
	}
	if op.Default != nil {
		parts = append(parts, f.keyword(op.Default.Type))
		parts = append(parts, f.formatExpression(&op.Default.Expression))
	}
	return strings.Join(parts, " ")
}

func (f *Formatter) formatRenameColumn(op *parser.RenameColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("RENAME COLUMN"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.From), f.keyword("TO"), f.identifier(op.To))
	return strings.Join(parts, " ")
}

func (f *Formatter) formatCommentColumn(op *parser.CommentColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("COMMENT COLUMN"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name), op.Comment)
	return strings.Join(parts, " ")
}

func (f *Formatter) formatClearColumn(op *parser.ClearColumnOperation) string {
	var parts []string
	parts = append(parts, f.keyword("CLEAR COLUMN"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))

	parts = append(parts, f.keyword("IN PARTITION"), f.identifier(op.Partition))

	return strings.Join(parts, " ")
}

// formatAddIndex formats ADD INDEX operations
func (f *Formatter) formatAddIndex(op *parser.AddIndexOperation) string {
	if op == nil {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("ADD INDEX"))

	if op.IfNotExists {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))
	parts = append(parts, f.formatExpression(&op.Expression))

	if op.Type != "" {
		parts = append(parts, f.keyword("TYPE"), f.identifier(op.Type))
	}

	if op.Granularity != "" {
		parts = append(parts, f.keyword("GRANULARITY"), op.Granularity)
	}

	if op.After != nil {
		parts = append(parts, f.keyword("AFTER"), f.identifier(*op.After))
	}

	if op.First {
		parts = append(parts, f.keyword("FIRST"))
	}

	return strings.Join(parts, " ")
}

// formatDropIndex formats DROP INDEX operations
func (f *Formatter) formatDropIndex(op *parser.DropIndexOperation) string {
	if op == nil {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("DROP INDEX"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))

	return strings.Join(parts, " ")
}

// formatAddConstraint formats ADD CONSTRAINT operations
func (f *Formatter) formatAddConstraint(op *parser.AddConstraintOperation) string {
	if op == nil {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("ADD CONSTRAINT"))

	if op.IfNotExists {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))
	parts = append(parts, f.keyword("CHECK"))
	parts = append(parts, f.formatExpression(&op.Expression))

	return strings.Join(parts, " ")
}

// formatDropConstraint formats DROP CONSTRAINT operations
func (f *Formatter) formatDropConstraint(op *parser.DropConstraintOperation) string {
	if op == nil {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("DROP CONSTRAINT"))

	if op.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.identifier(op.Name))

	return strings.Join(parts, " ")
}

// calculateMaxNameWidth calculates the maximum column name width for alignment
func (f *Formatter) calculateMaxNameWidth(elements []*parser.TableElement) int {
	if !f.options.AlignColumns {
		return 0
	}

	var maxNameWidth int
	for _, element := range elements {
		if element.Column != nil {
			// Include backticks in width calculation
			nameLen := len(f.identifier(element.Column.Name))
			if nameLen > maxNameWidth {
				maxNameWidth = nameLen
			}
		}
	}
	return maxNameWidth
}

// padRight pads a string to the specified width with spaces
func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}

// formatTableFunction formats a table function call (e.g., remote(...), s3Table(...), numbers(...))
func (f *Formatter) formatTableFunction(fn *parser.TableFunction) string {
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
