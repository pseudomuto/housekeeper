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
func (d *DDLFormatter) buildCreateStatement(objectType string, orReplace, ifNotExists bool, name string) []string {
	var parts []string

	if orReplace {
		parts = append(parts, d.formatter.keyword("CREATE OR REPLACE"))
	} else {
		parts = append(parts, d.formatter.keyword("CREATE"))
	}

	parts = append(parts, d.formatter.keyword(objectType))

	if ifNotExists && !orReplace {
		parts = append(parts, d.formatter.keyword("IF NOT EXISTS"))
	}

	parts = append(parts, d.formatter.identifier(name))

	return parts
}

// buildDropStatement builds common DROP statement parts.
func (d *DDLFormatter) buildDropStatement(objectType string, ifExists bool, name string) []string {
	var parts []string

	parts = append(parts, d.formatter.keyword("DROP"))
	parts = append(parts, d.formatter.keyword(objectType))

	if ifExists {
		parts = append(parts, d.formatter.keyword("IF EXISTS"))
	}

	parts = append(parts, d.formatter.identifier(name))

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
