package schemadiff

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// DatabaseDiffCreate indicates a database needs to be created
	DatabaseDiffCreate DatabaseDiffType = "CREATE"
	// DatabaseDiffDrop indicates a database needs to be dropped
	DatabaseDiffDrop DatabaseDiffType = "DROP"
	// DatabaseDiffAlter indicates a database needs to be altered
	DatabaseDiffAlter DatabaseDiffType = "ALTER"
	// DatabaseDiffRename indicates a database needs to be renamed
	DatabaseDiffRename DatabaseDiffType = "RENAME"
)

type (
	// DatabaseDiff represents a difference between current and target database states.
	// It contains all information needed to generate migration SQL statements for
	// database operations including CREATE, ALTER, DROP, and RENAME.
	DatabaseDiff struct {
		Type            DatabaseDiffType // Type of operation (CREATE, ALTER, DROP, RENAME)
		DatabaseName    string           // Name of the database being modified
		Description     string           // Human-readable description of the change
		UpSQL           string           // SQL to apply the change (forward migration)
		DownSQL         string           // SQL to rollback the change (reverse migration)
		Current         *DatabaseInfo    // Current state (nil if database doesn't exist)
		Target          *DatabaseInfo    // Target state (nil if database should be dropped)
		NewDatabaseName string           // For rename operations - the new name
	}

	// DatabaseDiffType represents the type of database difference
	DatabaseDiffType string

	// DatabaseInfo represents parsed database information extracted from DDL statements.
	// This structure contains all the properties needed for database comparison and
	// migration generation, including metadata for cluster and engine configuration.
	DatabaseInfo struct {
		Name    string // Database name
		Engine  string // Engine type (e.g., "Atomic", "MySQL", "Memory")
		Comment string // Database comment (without quotes)
		Cluster string // Cluster name if specified (empty if not clustered)
	}
)

// compareDatabases compares current and target database schemas and returns migration diffs.
// It analyzes both schemas to identify differences and generates appropriate migration operations.
//
// The function identifies:
//   - Databases that need to be created (exist in target but not current)
//   - Databases that need to be dropped (exist in current but not target)
//   - Databases that need to be altered (exist in both but have differences)
//   - Databases that need to be renamed (same properties but different names)
//
// Rename Detection:
// The function intelligently detects rename operations by comparing database properties
// (engine, comment, cluster) excluding the name. If two databases have identical
// properties but different names, it generates a RENAME operation instead of DROP+CREATE.
func compareDatabases(current, target *parser.SQL) ([]*DatabaseDiff, error) {
	// Extract database information from both SQL structures
	currentDBs := extractDatabaseInfo(current)
	targetDBs := extractDatabaseInfo(target)

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*DatabaseDiff, 0, len(currentDBs)+len(targetDBs))

	// Detect renames first to avoid treating them as drop+create
	renameDiffs, processedCurrent, processedTarget := detectDatabaseRenames(currentDBs, targetDBs)
	diffs = append(diffs, renameDiffs...)

	// Find databases to create or modify (sorted for deterministic order)
	targetNames := make([]string, 0, len(processedTarget))
	for name := range processedTarget {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	for _, name := range targetNames {
		targetDB := processedTarget[name]
		currentDB, exists := processedCurrent[name]
		diff, err := createDatabaseDiff(name, currentDB, targetDB, exists)
		if err != nil {
			return nil, err
		}
		if diff != nil {
			diffs = append(diffs, diff)
		}
	}

	// Find databases to drop (sorted for deterministic order)
	currentNames := make([]string, 0, len(processedCurrent))
	for name := range processedCurrent {
		if _, exists := processedTarget[name]; !exists {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentDB := processedCurrent[name]
		// Database should be dropped
		diff := &DatabaseDiff{
			Type:         DatabaseDiffDrop,
			DatabaseName: name,
			Description:  fmt.Sprintf("Drop database '%s'", name),
			Current:      currentDB,
			UpSQL:        generateDropDatabaseSQL(currentDB),
			DownSQL:      generateCreateDatabaseSQL(currentDB),
		}
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// extractDatabaseInfo extracts database information from CREATE DATABASE statements in SQL
func extractDatabaseInfo(sql *parser.SQL) map[string]*DatabaseInfo {
	databases := make(map[string]*DatabaseInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateDatabase != nil {
			db := stmt.CreateDatabase
			info := &DatabaseInfo{
				Name: db.Name,
			}

			if db.OnCluster != nil {
				info.Cluster = *db.OnCluster
			}

			if db.Engine != nil {
				info.Engine = formatEngine(db.Engine)
			}

			if db.Comment != nil {
				info.Comment = removeQuotes(*db.Comment)
			}

			databases[info.Name] = info
		}
	}

	return databases
}

// detectDatabaseRenames identifies potential rename operations between current and target states.
// It returns rename diffs and filtered maps with renamed databases removed.
func detectDatabaseRenames(currentDBs, targetDBs map[string]*DatabaseInfo) ([]*DatabaseDiff, map[string]*DatabaseInfo, map[string]*DatabaseInfo) {
	var renameDiffs []*DatabaseDiff
	processedCurrent := make(map[string]*DatabaseInfo)
	processedTarget := make(map[string]*DatabaseInfo)

	// Copy all databases to processed maps initially
	for name, db := range currentDBs {
		processedCurrent[name] = db
	}
	for name, db := range targetDBs {
		processedTarget[name] = db
	}

	// Look for potential renames: databases that don't exist by name but have identical properties
	for currentName, currentDB := range currentDBs {
		if _, exists := targetDBs[currentName]; exists {
			continue // Database exists in both, not a rename
		}

		// Look for a database in target with identical properties but different name
		for targetName, targetDB := range targetDBs {
			if _, exists := currentDBs[targetName]; exists {
				continue // Target database exists in current, not a rename target
			}

			// Check if properties match (everything except name)
			if databasePropertiesMatch(currentDB, targetDB) {
				// This is a rename operation
				diff := &DatabaseDiff{
					Type:            DatabaseDiffRename,
					DatabaseName:    currentName,
					NewDatabaseName: targetName,
					Description:     fmt.Sprintf("Rename database '%s' to '%s'", currentName, targetName),
					Current:         currentDB,
					Target:          targetDB,
					UpSQL:           generateRenameDatabaseSQL(currentName, targetName, currentDB.Cluster),
					DownSQL:         generateRenameDatabaseSQL(targetName, currentName, currentDB.Cluster),
				}
				renameDiffs = append(renameDiffs, diff)

				// Remove from processed maps so they're not treated as drop+create
				delete(processedCurrent, currentName)
				delete(processedTarget, targetName)
				break // Found the rename target, move to next current database
			}
		}
	}

	return renameDiffs, processedCurrent, processedTarget
}

// databasePropertiesMatch checks if two databases have identical properties (excluding name)
func databasePropertiesMatch(db1, db2 *DatabaseInfo) bool {
	return db1.Engine == db2.Engine &&
		db1.Comment == db2.Comment &&
		db1.Cluster == db2.Cluster
}

// generateRenameDatabaseSQL generates RENAME DATABASE SQL
func generateRenameDatabaseSQL(oldName, newName, onCluster string) string {
	var parts []string
	parts = append(parts, "RENAME DATABASE", oldName, "TO", newName)

	if onCluster != "" {
		parts = append(parts, "ON CLUSTER", onCluster)
	}

	return strings.Join(parts, " ") + ";"
}

// needsModification checks if a database needs to be modified
func needsModification(current, target *DatabaseInfo) bool {
	return current.Comment != target.Comment ||
		current.Engine != target.Engine ||
		current.Cluster != target.Cluster
}

// generateCreateDatabaseSQL generates CREATE DATABASE SQL from database info
func generateCreateDatabaseSQL(db *DatabaseInfo) string {
	var parts []string
	parts = append(parts, "CREATE DATABASE", db.Name)

	if db.Cluster != "" {
		parts = append(parts, "ON CLUSTER", db.Cluster)
	}

	if db.Engine != "" {
		parts = append(parts, "ENGINE =", db.Engine)
	}

	if db.Comment != "" {
		parts = append(parts, "COMMENT", fmt.Sprintf("'%s'", escapeSQL(db.Comment)))
	}

	return strings.Join(parts, " ") + ";"
}

// generateDropDatabaseSQL generates DROP DATABASE SQL from database info
func generateDropDatabaseSQL(db *DatabaseInfo) string {
	var parts []string
	parts = append(parts, "DROP DATABASE IF EXISTS", db.Name)

	if db.Cluster != "" {
		parts = append(parts, "ON CLUSTER", db.Cluster)
	}

	return strings.Join(parts, " ") + ";"
}

// generateAlterDatabaseSQL generates ALTER DATABASE SQL to change from current to target state
func generateAlterDatabaseSQL(current, target *DatabaseInfo) (string, error) {
	var statements []string

	// Check for unsupported operations first
	if current.Engine != target.Engine {
		return "", errors.Wrapf(ErrUnsupported, "engine change from '%s' to '%s' - requires manual database recreation", current.Engine, target.Engine)
	}

	if current.Cluster != target.Cluster {
		return "", errors.Wrapf(ErrUnsupported, "cluster change from '%s' to '%s' - requires manual intervention", current.Cluster, target.Cluster)
	}

	// Check if comment changed
	if current.Comment != target.Comment {
		var parts []string
		parts = append(parts, "ALTER DATABASE", target.Name)

		if target.Cluster != "" {
			parts = append(parts, "ON CLUSTER", target.Cluster)
		}

		parts = append(parts, "MODIFY COMMENT", fmt.Sprintf("'%s'", escapeSQL(target.Comment)))
		statements = append(statements, strings.Join(parts, " ")+";")
	}

	if len(statements) == 0 {
		return "-- No changes needed", nil
	}

	return strings.Join(statements, "\n"), nil
}

func createDatabaseDiff(name string, currentDB, targetDB *DatabaseInfo, exists bool) (*DatabaseDiff, error) {
	// Validate operation before proceeding
	if err := validateDatabaseOperation(currentDB, targetDB); err != nil {
		return nil, err
	}

	if !exists {
		// Database needs to be created
		return &DatabaseDiff{
			Type:         DatabaseDiffCreate,
			DatabaseName: name,
			Description:  fmt.Sprintf("Create database '%s'", name),
			Target:       targetDB,
			UpSQL:        generateCreateDatabaseSQL(targetDB),
			DownSQL:      generateDropDatabaseSQL(targetDB),
		}, nil
	}

	// Database exists, check for modifications
	if !needsModification(currentDB, targetDB) {
		return nil, nil
	}

	upSQL, err := generateAlterDatabaseSQL(currentDB, targetDB)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to generate UP migration for database '%s'", name)
	}

	downSQL, err := generateAlterDatabaseSQL(targetDB, currentDB)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to generate DOWN migration for database '%s'", name)
	}

	return &DatabaseDiff{
		Type:         DatabaseDiffAlter,
		DatabaseName: name,
		Description:  fmt.Sprintf("Alter database '%s'", name),
		Current:      currentDB,
		Target:       targetDB,
		UpSQL:        upSQL,
		DownSQL:      downSQL,
	}, nil
}
