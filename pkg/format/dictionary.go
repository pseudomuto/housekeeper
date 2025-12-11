package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateDictionary formats a CREATE DICTIONARY statement
func (f *Formatter) createDictionary(w io.Writer, stmt *parser.CreateDictionaryStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
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
		if primaryKey := stmt.GetPrimaryKey(); primaryKey != nil {
			var keys []string
			for _, key := range primaryKey.Keys {
				keys = append(keys, f.identifier(key))
			}
			lines = append(lines, f.keyword("PRIMARY KEY")+" "+strings.Join(keys, ", "))
		}

		// SOURCE
		if source := stmt.GetSource(); source != nil {
			lines = append(lines, f.formatDictionarySource(source))
		}

		// LAYOUT
		if layout := stmt.GetLayout(); layout != nil {
			lines = append(lines, f.formatDictionaryLayout(layout))
		}

		// LIFETIME
		if lifetime := stmt.GetLifetime(); lifetime != nil {
			lines = append(lines, f.formatDictionaryLifetime(lifetime))
		}

		// SETTINGS
		if settings := stmt.GetSettings(); settings != nil && len(settings.Settings) > 0 {
			lines = append(lines, f.formatDictionarySettings(settings))
		}

		// COMMENT
		if stmt.Comment != nil {
			lines = append(lines, f.keyword("COMMENT")+" "+*stmt.Comment)
		}

		_, err := w.Write([]byte(strings.Join(lines, "\n") + ";"))
		return err
	})
}

// AttachDictionary formats an ATTACH DICTIONARY statement
func (f *Formatter) attachDictionary(w io.Writer, stmt *parser.AttachDictionaryStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildAttachStatement("DICTIONARY", stmt.IfNotExists != nil, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)

		return ddl.formatBasicDDL(w, parts)
	})
}

// DetachDictionary formats a DETACH DICTIONARY statement
func (f *Formatter) detachDictionary(w io.Writer, stmt *parser.DetachDictionaryStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildDetachStatement("DICTIONARY", stmt.IfExists != nil, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)
		parts = ddl.appendPermanently(parts, stmt.Permanently != nil)
		parts = ddl.appendSync(parts, stmt.Sync != nil)

		return ddl.formatBasicDDL(w, parts)
	})
}

// DropDictionary formats a DROP DICTIONARY statement
func (f *Formatter) dropDictionary(w io.Writer, stmt *parser.DropDictionaryStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		parts := ddl.buildDropStatement("DICTIONARY", stmt.IfExists != nil, f.qualifiedName(stmt.Database, stmt.Name))
		parts = ddl.appendOnCluster(parts, stmt.OnCluster)
		parts = ddl.appendSync(parts, stmt.Sync != nil)

		return ddl.formatBasicDDL(w, parts)
	})
}

// RenameDictionary formats a RENAME DICTIONARY statement
func (f *Formatter) renameDictionary(w io.Writer, stmt *parser.RenameDictionaryStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		ddl := NewDDLFormatter(f)

		renames := make([]RenameItem, 0, len(stmt.Renames))
		for _, rename := range stmt.Renames {
			renames = append(renames, RenameItem{
				From: f.qualifiedName(rename.FromDatabase, rename.FromName),
				To:   f.qualifiedName(rename.ToDatabase, rename.ToName),
			})
		}

		parts := ddl.buildRenameStatement("DICTIONARY", renames, stmt.OnCluster)
		return ddl.formatBasicDDL(w, parts)
	})
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
		parts = append(parts, col.Default.GetValue())
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
			if param.DSLFunction != nil {
				params = append(params, param.GetValue())
			} else {
				params = append(params, param.GetName()+" "+param.GetValue())
			}
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
			if param.DSLFunction != nil {
				params = append(params, param.GetValue())
			} else {
				params = append(params, param.GetName()+" "+param.GetValue())
			}
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
		settingParts = append(settingParts, setting.Name+" = "+setting.Value)
	}
	parts = append(parts, "("+strings.Join(settingParts, ", ")+")")

	return strings.Join(parts, "")
}
