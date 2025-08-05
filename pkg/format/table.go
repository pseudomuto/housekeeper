package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateTable formats a CREATE TABLE statement with proper indentation and alignment
func (f *Formatter) CreateTable(stmt *parser.CreateTableStmt) string {
	lines := make([]string, 0, 10) // Approximate capacity for typical table

	// Build the header line
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

	lines = append(lines, strings.Join(headerParts, " ")+" (")

	// Format table elements (columns, indexes, constraints)
	if len(stmt.Elements) > 0 {
		// Convert slice to pointer slice
		var elements []*parser.TableElement
		for i := range stmt.Elements {
			elements = append(elements, &stmt.Elements[i])
		}
		elementLines := f.formatTableElements(elements)
		for i, line := range elementLines {
			prefix := f.indent(1)
			if i < len(elementLines)-1 {
				line += ","
			}
			lines = append(lines, prefix+line)
		}
	}

	lines = append(lines, ")")

	// Format table options
	if stmt.Engine != nil {
		lines = append(lines, f.keyword("ENGINE")+" = "+f.formatTableEngine(stmt.Engine))
	}

	if stmt.OrderBy != nil {
		lines = append(lines, f.keyword("ORDER BY")+" "+f.formatExpression(&stmt.OrderBy.Expression))
	}

	if stmt.PartitionBy != nil {
		lines = append(lines, f.keyword("PARTITION BY")+" "+f.formatExpression(&stmt.PartitionBy.Expression))
	}

	if stmt.PrimaryKey != nil {
		lines = append(lines, f.keyword("PRIMARY KEY")+" "+f.formatExpression(&stmt.PrimaryKey.Expression))
	}

	if stmt.SampleBy != nil {
		lines = append(lines, f.keyword("SAMPLE BY")+" "+f.formatExpression(&stmt.SampleBy.Expression))
	}

	if stmt.TTL != nil {
		lines = append(lines, f.keyword("TTL")+" "+f.formatExpression(&stmt.TTL.Expression))
	}

	if stmt.Settings != nil && len(stmt.Settings.Settings) > 0 {
		lines = append(lines, f.formatTableSettings(stmt.Settings))
	}

	if stmt.Comment != nil {
		lines = append(lines, f.keyword("COMMENT")+" "+*stmt.Comment)
	}

	return strings.Join(lines, "\n") + ";"
}

// AlterTable formats an ALTER TABLE statement
func (f *Formatter) AlterTable(stmt *parser.AlterTableStmt) string {
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

	return strings.Join(lines, "\n") + ";"
}

// AttachTable formats an ATTACH TABLE statement
func (f *Formatter) AttachTable(stmt *parser.AttachTableStmt) string {
	var parts []string

	parts = append(parts, f.keyword("ATTACH TABLE"))

	if stmt.IfNotExists {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	return strings.Join(parts, " ") + ";"
}

// DetachTable formats a DETACH TABLE statement
func (f *Formatter) DetachTable(stmt *parser.DetachTableStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// DropTable formats a DROP TABLE statement
func (f *Formatter) DropTable(stmt *parser.DropTableStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// RenameTable formats a RENAME TABLE statement
func (f *Formatter) RenameTable(stmt *parser.RenameTableStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// formatTableElements formats table elements with optional alignment
func (f *Formatter) formatTableElements(elements []*parser.TableElement) []string {
	if len(elements) == 0 {
		return nil
	}

	lines := make([]string, 0, len(elements)) // Pre-allocate based on element count
	var maxNameWidth int

	// Calculate alignment if enabled
	if f.options.AlignColumns {
		for _, element := range elements {
			if element.Column != nil {
				// Include backticks in width calculation
				nameLen := len(f.identifier(element.Column.Name))
				if nameLen > maxNameWidth {
					maxNameWidth = nameLen
				}
			}
		}
	}

	// Format each element
	for _, element := range elements {
		if element.Column != nil {
			lines = append(lines, f.formatColumn(element.Column, maxNameWidth))
		} else if element.Index != nil {
			lines = append(lines, f.formatIndexDefinition(element.Index))
		} else if element.Constraint != nil {
			lines = append(lines, f.formatConstraintDefinition(element.Constraint))
		}
	}

	return lines
}

// formatColumn formats a single column definition
func (f *Formatter) formatColumn(col *parser.Column, alignWidth int) string {
	var parts []string

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
	if col.Default != nil {
		parts = append(parts, f.keyword(col.Default.Type))
		parts = append(parts, f.formatExpression(&col.Default.Expression))
	}

	// Codec
	if col.Codec != nil {
		parts = append(parts, f.formatCodec(col.Codec))
	}

	// TTL
	if col.TTL != nil {
		parts = append(parts, f.keyword("TTL"), f.formatExpression(&col.TTL.Expression))
	}

	// Comment
	if col.Comment != nil {
		parts = append(parts, f.keyword("COMMENT"), *col.Comment)
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
			if param.String != nil {
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

// formatTableSettings formats the SETTINGS clause
func (f *Formatter) formatTableSettings(settings *parser.TableSettingsClause) string {
	if settings == nil || len(settings.Settings) == 0 {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("SETTINGS"))

	settingParts := make([]string, 0, len(settings.Settings))
	for _, setting := range settings.Settings {
		settingParts = append(settingParts, f.identifier(setting.Name)+" = "+setting.Value)
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

// padRight pads a string to the specified width with spaces
func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	return s + strings.Repeat(" ", width-len(s))
}
