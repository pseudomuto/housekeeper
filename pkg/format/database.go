package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateDatabase formats a CREATE DATABASE statement
func (f *Formatter) createDatabase(w io.Writer, stmt *parser.CreateDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// CREATE DATABASE
		parts = append(parts, f.keyword("CREATE DATABASE"))

		// IF NOT EXISTS
		if stmt.IfNotExists {
			parts = append(parts, f.keyword("IF NOT EXISTS"))
		}

		// Database name
		parts = append(parts, f.identifier(stmt.Name))

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		// ENGINE
		if stmt.Engine != nil {
			parts = append(parts, f.keyword("ENGINE"), "=", f.formatDatabaseEngine(stmt.Engine))
		}

		// COMMENT
		if stmt.Comment != nil {
			parts = append(parts, f.keyword("COMMENT"), *stmt.Comment)
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// AlterDatabase formats an ALTER DATABASE statement
func (f *Formatter) alterDatabase(w io.Writer, stmt *parser.AlterDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// ALTER DATABASE
		parts = append(parts, f.keyword("ALTER DATABASE"), f.identifier(stmt.Name))

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		// Action
		if stmt.Action != nil && stmt.Action.ModifyComment != nil {
			parts = append(parts, f.keyword("MODIFY COMMENT"), *stmt.Action.ModifyComment)
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// AttachDatabase formats an ATTACH DATABASE statement
func (f *Formatter) attachDatabase(w io.Writer, stmt *parser.AttachDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// ATTACH DATABASE
		parts = append(parts, f.keyword("ATTACH DATABASE"))

		// IF NOT EXISTS
		if stmt.IfNotExists {
			parts = append(parts, f.keyword("IF NOT EXISTS"))
		}

		// Database name
		parts = append(parts, f.identifier(stmt.Name))

		// ENGINE
		if stmt.Engine != nil {
			parts = append(parts, f.keyword("ENGINE"), "=", f.formatDatabaseEngine(stmt.Engine))
		}

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// DetachDatabase formats a DETACH DATABASE statement
func (f *Formatter) detachDatabase(w io.Writer, stmt *parser.DetachDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// DETACH DATABASE
		parts = append(parts, f.keyword("DETACH DATABASE"))

		// IF EXISTS
		if stmt.IfExists {
			parts = append(parts, f.keyword("IF EXISTS"))
		}

		// Database name
		parts = append(parts, f.identifier(stmt.Name))

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		// PERMANENTLY
		if stmt.Permanently {
			parts = append(parts, f.keyword("PERMANENTLY"))
		}

		// SYNC
		if stmt.Sync {
			parts = append(parts, f.keyword("SYNC"))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// DropDatabase formats a DROP DATABASE statement
func (f *Formatter) dropDatabase(w io.Writer, stmt *parser.DropDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// DROP DATABASE
		parts = append(parts, f.keyword("DROP DATABASE"))

		// IF EXISTS
		if stmt.IfExists {
			parts = append(parts, f.keyword("IF EXISTS"))
		}

		// Database name
		parts = append(parts, f.identifier(stmt.Name))

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		// SYNC
		if stmt.Sync {
			parts = append(parts, f.keyword("SYNC"))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// RenameDatabase formats a RENAME DATABASE statement
func (f *Formatter) renameDatabase(w io.Writer, stmt *parser.RenameDatabaseStmt) error {
	return f.formatWithComments(w, stmt, func(w io.Writer) error {
		var parts []string

		// RENAME DATABASE
		parts = append(parts, f.keyword("RENAME DATABASE"))

		// Renames
		renameParts := make([]string, 0, len(stmt.Renames))
		for _, rename := range stmt.Renames {
			renameParts = append(renameParts, f.identifier(rename.From)+" "+f.keyword("TO")+" "+f.identifier(rename.To))
		}
		parts = append(parts, strings.Join(renameParts, ", "))

		// ON CLUSTER
		if stmt.OnCluster != nil {
			parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
		}

		_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
		return err
	})
}

// formatDatabaseEngine formats a database engine specification
func (f *Formatter) formatDatabaseEngine(engine *parser.DatabaseEngine) string {
	if engine == nil {
		return ""
	}

	result := engine.Name
	if len(engine.Parameters) > 0 {
		result += "("
		var params []string
		for _, param := range engine.Parameters {
			params = append(params, param.Value)
		}
		result += strings.Join(params, ", ")
		result += ")"
	}
	return result
}
