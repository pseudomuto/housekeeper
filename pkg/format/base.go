package format

import (
	"io"
	"strings"
)

// DDLFormatter provides common functionality for formatting DDL statements.
type DDLFormatter struct {
	formatter *Formatter
}

// NewDDLFormatter creates a new DDL formatter with the given formatter.
func NewDDLFormatter(f *Formatter) *DDLFormatter {
	return &DDLFormatter{formatter: f}
}

// formatBasicDDL formats a basic DDL statement with common patterns.
func (d *DDLFormatter) formatBasicDDL(w io.Writer, parts []string) error {
	sql := strings.Join(parts, " ")
	if !strings.HasSuffix(sql, ";") {
		sql += ";"
	}
	_, err := w.Write([]byte(sql))
	return err
}

// buildCreateStatement builds common CREATE statement parts.
// The name parameter should be pre-formatted (e.g., via identifier() or qualifiedName()).
func (d *DDLFormatter) buildCreateStatement(objectType string, orReplace, ifNotExists bool, name string) []string {
	var parts []string

	if orReplace {
		parts = append(parts, d.formatter.keyword("CREATE OR REPLACE "+objectType))
	} else {
		parts = append(parts, d.formatter.keyword("CREATE "+objectType))
	}

	if ifNotExists && !orReplace {
		parts = append(parts, d.formatter.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, name)

	return parts
}

// buildDropStatement builds common DROP statement parts.
// The name parameter should be pre-formatted (e.g., via identifier() or qualifiedName()).
func (d *DDLFormatter) buildDropStatement(objectType string, ifExists bool, name string) []string {
	var parts []string
	parts = append(parts, d.formatter.keyword("DROP "+objectType))
	parts = d.appendIfExists(parts, ifExists)
	parts = append(parts, name)
	return parts
}

// appendOnCluster appends ON CLUSTER clause if present.
func (d *DDLFormatter) appendOnCluster(parts []string, cluster *string) []string {
	if cluster != nil {
		parts = append(parts, d.formatter.keyword("ON CLUSTER"))
		parts = append(parts, d.formatter.identifier(*cluster))
	}
	return parts
}

// appendComment appends COMMENT clause if present.
func (d *DDLFormatter) appendComment(parts []string, comment *string) []string {
	if comment != nil {
		parts = append(parts, d.formatter.keyword("COMMENT"))
		parts = append(parts, *comment) // Comment is already quoted
	}
	return parts
}

// appendSync appends SYNC keyword if true.
func (d *DDLFormatter) appendSync(parts []string, sync bool) []string {
	if sync {
		parts = append(parts, d.formatter.keyword("SYNC"))
	}
	return parts
}

// appendIfExists appends IF EXISTS clause if true.
func (d *DDLFormatter) appendIfExists(parts []string, ifExists bool) []string {
	if ifExists {
		parts = append(parts, d.formatter.keyword("IF EXISTS"))
	}
	return parts
}

// appendIfNotExists appends IF NOT EXISTS clause if true.
func (d *DDLFormatter) appendIfNotExists(parts []string, ifNotExists bool) []string {
	if ifNotExists {
		parts = append(parts, d.formatter.keyword("IF NOT EXISTS"))
	}
	return parts
}

// appendPermanently appends PERMANENTLY keyword if true.
func (d *DDLFormatter) appendPermanently(parts []string, permanently bool) []string {
	if permanently {
		parts = append(parts, d.formatter.keyword("PERMANENTLY"))
	}
	return parts
}

// buildAttachStatement builds common ATTACH statement parts.
func (d *DDLFormatter) buildAttachStatement(objectType string, ifNotExists bool, name string) []string {
	var parts []string
	parts = append(parts, d.formatter.keyword("ATTACH "+objectType))
	parts = d.appendIfNotExists(parts, ifNotExists)
	parts = append(parts, name)
	return parts
}

// buildDetachStatement builds common DETACH statement parts.
func (d *DDLFormatter) buildDetachStatement(objectType string, ifExists bool, name string) []string {
	var parts []string
	parts = append(parts, d.formatter.keyword("DETACH "+objectType))
	parts = d.appendIfExists(parts, ifExists)
	parts = append(parts, name)
	return parts
}

// RenameItem represents a single rename operation (from -> to).
type RenameItem struct {
	From string
	To   string
}

// buildRenameStatement builds common RENAME statement parts.
func (d *DDLFormatter) buildRenameStatement(objectType string, renames []RenameItem, onCluster *string) []string {
	var parts []string
	parts = append(parts, d.formatter.keyword("RENAME "+objectType))

	renameParts := make([]string, 0, len(renames))
	for _, rename := range renames {
		renameParts = append(renameParts, rename.From+" "+d.formatter.keyword("TO")+" "+rename.To)
	}
	parts = append(parts, strings.Join(renameParts, ", "))

	parts = d.appendOnCluster(parts, onCluster)
	return parts
}
