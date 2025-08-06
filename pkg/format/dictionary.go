package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateDictionary formats a CREATE DICTIONARY statement
func (f *Formatter) createDictionary(w io.Writer, stmt *parser.CreateDictionaryStmt) error {
	var lines []string

	// Build the header line
	var headerParts []string
	headerParts = append(headerParts, f.keyword("CREATE"))

	if stmt.OrReplace {
		headerParts = append(headerParts, f.keyword("OR REPLACE"))
	}

	headerParts = append(headerParts, f.keyword("DICTIONARY"))

	if stmt.IfNotExists != nil {
		headerParts = append(headerParts, f.keyword("IF NOT EXISTS"))
	}

	headerParts = append(headerParts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	lines = append(lines, strings.Join(headerParts, " ")+" (")

	// Format dictionary columns
	if len(stmt.Columns) > 0 {
		columnLines := f.formatDictionaryColumns(stmt.Columns)
		for i, line := range columnLines {
			prefix := f.indent(1)
			if i < len(columnLines)-1 {
				line += ","
			}
			lines = append(lines, prefix+line)
		}
	}

	lines = append(lines, ")")

	// PRIMARY KEY
	if stmt.PrimaryKey != nil {
		var keys []string
		for _, key := range stmt.PrimaryKey.Keys {
			keys = append(keys, f.identifier(key))
		}
		lines = append(lines, f.keyword("PRIMARY KEY")+" "+strings.Join(keys, ", "))
	}

	// SOURCE
	if stmt.Source != nil {
		lines = append(lines, f.formatDictionarySource(stmt.Source))
	}

	// LAYOUT
	if stmt.Layout != nil {
		lines = append(lines, f.formatDictionaryLayout(stmt.Layout))
	}

	// LIFETIME
	if stmt.Lifetime != nil {
		lines = append(lines, f.formatDictionaryLifetime(stmt.Lifetime))
	}

	// SETTINGS
	if stmt.Settings != nil && len(stmt.Settings.Settings) > 0 {
		lines = append(lines, f.formatDictionarySettings(stmt.Settings))
	}

	// COMMENT
	if stmt.Comment != nil {
		lines = append(lines, f.keyword("COMMENT")+" "+*stmt.Comment)
	}

	_, err := w.Write([]byte(strings.Join(lines, "\n") + ";"))
	return err
}

// AttachDictionary formats an ATTACH DICTIONARY statement
func (f *Formatter) attachDictionary(w io.Writer, stmt *parser.AttachDictionaryStmt) error {
	var parts []string

	parts = append(parts, f.keyword("ATTACH DICTIONARY"))

	if stmt.IfNotExists != nil {
		parts = append(parts, f.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// DetachDictionary formats a DETACH DICTIONARY statement
func (f *Formatter) detachDictionary(w io.Writer, stmt *parser.DetachDictionaryStmt) error {
	var parts []string

	parts = append(parts, f.keyword("DETACH DICTIONARY"))

	if stmt.IfExists != nil {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	if stmt.Permanently != nil {
		parts = append(parts, f.keyword("PERMANENTLY"))
	}

	if stmt.Sync != nil {
		parts = append(parts, f.keyword("SYNC"))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// DropDictionary formats a DROP DICTIONARY statement
func (f *Formatter) dropDictionary(w io.Writer, stmt *parser.DropDictionaryStmt) error {
	var parts []string

	parts = append(parts, f.keyword("DROP DICTIONARY"))

	if stmt.IfExists != nil {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	parts = append(parts, f.qualifiedName(stmt.Database, stmt.Name))

	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	if stmt.Sync != nil {
		parts = append(parts, f.keyword("SYNC"))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// RenameDictionary formats a RENAME DICTIONARY statement
func (f *Formatter) renameDictionary(w io.Writer, stmt *parser.RenameDictionaryStmt) error {
	var parts []string

	parts = append(parts, f.keyword("RENAME DICTIONARY"))

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
}

// formatDictionaryColumns formats dictionary column definitions
func (f *Formatter) formatDictionaryColumns(columns []*parser.DictionaryColumn) []string {
	if len(columns) == 0 {
		return nil
	}

	lines := make([]string, 0, len(columns)) // Pre-allocate based on column count
	var maxNameWidth int

	// Calculate alignment if enabled
	if f.options.AlignColumns {
		for _, col := range columns {
			// Include backticks in width calculation
			nameLen := len(f.identifier(col.Name))
			if nameLen > maxNameWidth {
				maxNameWidth = nameLen
			}
		}
	}

	// Format each column
	for _, col := range columns {
		lines = append(lines, f.formatDictionaryColumn(col, maxNameWidth))
	}

	return lines
}

// formatDictionaryColumn formats a single dictionary column definition
func (f *Formatter) formatDictionaryColumn(col *parser.DictionaryColumn, alignWidth int) string {
	parts := make([]string, 0, 6) // Approximate capacity for typical column parts

	// Column name (with optional alignment)
	name := f.identifier(col.Name)
	if alignWidth > 0 && f.options.AlignColumns {
		name = padRight(name, alignWidth)
	}
	parts = append(parts, name)

	// Data type
	parts = append(parts, col.Type)

	// Default value
	if col.Default != nil {
		parts = append(parts, f.keyword(col.Default.Type))
		parts = append(parts, col.Default.Expression)
	}

	// Attributes
	for _, attr := range col.Attributes {
		parts = append(parts, f.keyword(attr.Name))
	}

	return strings.Join(parts, " ")
}

// formatDictionarySource formats a dictionary source specification
func (f *Formatter) formatDictionarySource(source *parser.DictionarySource) string {
	if source == nil {
		return ""
	}

	result := f.keyword("SOURCE") + "(" + source.Name
	if len(source.Parameters) > 0 {
		result += "("
		var params []string
		for _, param := range source.Parameters {
			params = append(params, param.Name+" "+param.Value)
		}
		result += strings.Join(params, " ")
		result += ")"
	}
	result += ")"
	return result
}

// formatDictionaryLayout formats a dictionary layout specification
func (f *Formatter) formatDictionaryLayout(layout *parser.DictionaryLayout) string {
	if layout == nil {
		return ""
	}

	result := f.keyword("LAYOUT") + "(" + layout.Name
	// Always add parentheses for consistency
	result += "("
	if len(layout.Parameters) > 0 {
		var params []string
		for _, param := range layout.Parameters {
			params = append(params, param.Name+" "+param.Value)
		}
		result += strings.Join(params, " ")
	}
	result += "))"
	return result
}

// formatDictionaryLifetime formats a dictionary lifetime specification
func (f *Formatter) formatDictionaryLifetime(lifetime *parser.DictionaryLifetime) string {
	if lifetime == nil {
		return ""
	}

	if lifetime.Single != nil {
		return f.keyword("LIFETIME") + "(" + *lifetime.Single + ")"
	}
	if lifetime.MinMax != nil {
		if lifetime.MinMax.MinFirst != nil {
			return f.keyword("LIFETIME") + "(" + f.keyword("MIN") + " " + lifetime.MinMax.MinFirst.MinValue + " " + f.keyword("MAX") + " " + lifetime.MinMax.MinFirst.MaxValue + ")"
		} else if lifetime.MinMax.MaxFirst != nil {
			return f.keyword("LIFETIME") + "(" + f.keyword("MAX") + " " + lifetime.MinMax.MaxFirst.MaxValue + " " + f.keyword("MIN") + " " + lifetime.MinMax.MaxFirst.MinValue + ")"
		}
	}
	return ""
}

// formatDictionarySettings formats dictionary settings
func (f *Formatter) formatDictionarySettings(settings *parser.DictionarySettings) string {
	if settings == nil || len(settings.Settings) == 0 {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("SETTINGS"))

	settingParts := make([]string, 0, len(settings.Settings))
	for _, setting := range settings.Settings {
		settingParts = append(settingParts, f.identifier(setting.Name)+"="+setting.Value)
	}
	parts = append(parts, "("+strings.Join(settingParts, ", ")+")")

	return strings.Join(parts, "")
}
