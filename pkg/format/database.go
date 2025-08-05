package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateDatabase formats a CREATE DATABASE statement
func (f *formatter) createDatabase(stmt *parser.CreateDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// AlterDatabase formats an ALTER DATABASE statement
func (f *formatter) alterDatabase(stmt *parser.AlterDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// AttachDatabase formats an ATTACH DATABASE statement
func (f *formatter) attachDatabase(stmt *parser.AttachDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// DetachDatabase formats a DETACH DATABASE statement
func (f *formatter) detachDatabase(stmt *parser.DetachDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// DropDatabase formats a DROP DATABASE statement
func (f *formatter) dropDatabase(stmt *parser.DropDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// RenameDatabase formats a RENAME DATABASE statement
func (f *formatter) renameDatabase(stmt *parser.RenameDatabaseStmt) string {
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

	return strings.Join(parts, " ") + ";"
}

// formatDatabaseEngine formats a database engine specification
func (f *formatter) formatDatabaseEngine(engine *parser.DatabaseEngine) string {
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
