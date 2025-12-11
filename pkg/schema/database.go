package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
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
		DiffBase               // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current  *DatabaseInfo // Current state (nil if database doesn't exist)
		Target   *DatabaseInfo // Target state (nil if database should be dropped)
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

// GetName implements SchemaObject interface
func (d *DatabaseInfo) GetName() string {
	return d.Name
}

// GetCluster implements SchemaObject interface
func (d *DatabaseInfo) GetCluster() string {
	return d.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two databases have identical properties (excluding name).
func (d *DatabaseInfo) PropertiesMatch(other SchemaObject) bool {
	otherDB, ok := other.(*DatabaseInfo)
	if !ok {
		return false
	}
	return d.Engine == otherDB.Engine &&
		d.Comment == otherDB.Comment &&
		d.Cluster == otherDB.Cluster
}

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

	// Detect renames using generic algorithm
	renames, processedCurrent, processedTarget := DetectRenames(currentDBs, targetDBs)

	// Create rename diffs
	for _, rename := range renames {
		currentDB := currentDBs[rename.OldName]
		targetDB := targetDBs[rename.NewName]
		diff := &DatabaseDiff{
			DiffBase: DiffBase{
				Type:        string(DatabaseDiffRename),
				Name:        rename.OldName,
				NewName:     rename.NewName,
				Description: fmt.Sprintf("Rename database '%s' to '%s'", rename.OldName, rename.NewName),
				UpSQL:       generateRenameDatabaseSQL(rename.OldName, rename.NewName, currentDB.Cluster),
				DownSQL:     generateRenameDatabaseSQL(rename.NewName, rename.OldName, currentDB.Cluster),
			},
			Current: currentDB,
			Target:  targetDB,
		}
		diffs = append(diffs, diff)
	}

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
			DiffBase: DiffBase{
				Type:        string(DatabaseDiffDrop),
				Name:        name,
				Description: fmt.Sprintf("Drop database '%s'", name),
				UpSQL:       generateDropDatabaseSQL(currentDB),
				DownSQL:     generateCreateDatabaseSQL(currentDB),
			},
			Current: currentDB,
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
				Name: normalizeIdentifier(db.Name),
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

// generateRenameDatabaseSQL generates RENAME DATABASE SQL
func generateRenameDatabaseSQL(oldName, newName, onCluster string) string {
	return utils.NewSQLBuilder().
		Rename("DATABASE").
		Name(oldName).
		To(newName).
		OnCluster(onCluster).
		String()
}

// needsModification checks if a database needs to be modified
func needsModification(current, target *DatabaseInfo) bool {
	return current.Comment != target.Comment ||
		current.Engine != target.Engine ||
		current.Cluster != target.Cluster
}

// generateCreateDatabaseSQL generates CREATE DATABASE SQL from database info
func generateCreateDatabaseSQL(db *DatabaseInfo) string {
	return utils.NewSQLBuilder().
		Create("DATABASE").
		Name(db.Name).
		OnCluster(db.Cluster).
		Engine(db.Engine).
		Comment(db.Comment).
		String()
}

// generateDropDatabaseSQL generates DROP DATABASE SQL from database info
func generateDropDatabaseSQL(db *DatabaseInfo) string {
	return utils.NewSQLBuilder().
		Drop("DATABASE").
		IfExists().
		Name(db.Name).
		OnCluster(db.Cluster).
		String()
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
		sql := utils.NewSQLBuilder().
			Alter("DATABASE").
			Name(target.Name).
			OnCluster(target.Cluster).
			Modify("COMMENT").
			Escaped(target.Comment).
			String()
		statements = append(statements, sql)
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
			DiffBase: DiffBase{
				Type:        string(DatabaseDiffCreate),
				Name:        name,
				Description: fmt.Sprintf("Create database '%s'", name),
				UpSQL:       generateCreateDatabaseSQL(targetDB),
				DownSQL:     generateDropDatabaseSQL(targetDB),
			},
			Target: targetDB,
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
		DiffBase: DiffBase{
			Type:        string(DatabaseDiffAlter),
			Name:        name,
			Description: fmt.Sprintf("Alter database '%s'", name),
			UpSQL:       upSQL,
			DownSQL:     downSQL,
		},
		Current: currentDB,
		Target:  targetDB,
	}, nil
}
