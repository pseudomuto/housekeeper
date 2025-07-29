package migrator

import (
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// DatabaseDiffCreate indicates a database needs to be created
	DatabaseDiffCreate DatabaseDiffType = "CREATE"
	// DatabaseDiffDrop indicates a database needs to be dropped
	DatabaseDiffDrop   DatabaseDiffType = "DROP"
	// DatabaseDiffAlter indicates a database needs to be altered
	DatabaseDiffAlter  DatabaseDiffType = "ALTER"
)

var (
	// ErrUnsupported is returned when an operation is not supported
	ErrUnsupported = errors.New("unsupported operation")
)

type (
	// DatabaseDiff represents a difference between two database states
	DatabaseDiff struct {
		Type         DatabaseDiffType
		DatabaseName string
		Description  string
		UpSQL        string
		DownSQL      string
		Current      *DatabaseInfo // Current state (nil if database doesn't exist)
		Target       *DatabaseInfo // Target state (nil if database should be dropped)
	}

	// DatabaseDiffType represents the type of database difference
	DatabaseDiffType string

	// DatabaseInfo represents parsed database information for comparison
	DatabaseInfo struct {
		Name      string
		Engine    string
		Comment   string
		OnCluster string
	}
)

// CompareDatabaseGrammars compares current and target database grammars and returns migration diffs.
// It analyzes both grammars to identify:
//   - Databases that need to be created (exist in target but not current)
//   - Databases that need to be dropped (exist in current but not target)  
//   - Databases that need to be altered (exist in both but have differences)
//
// The function returns a slice of DatabaseDiff objects describing each change needed.
// It returns an error if an unsupported operation is detected (e.g., engine or cluster changes).
func CompareDatabaseGrammars(current, target *parser.Grammar) ([]*DatabaseDiff, error) {
	var diffs []*DatabaseDiff

	// Extract database information from both grammars
	currentDBs := extractDatabaseInfo(current)
	targetDBs := extractDatabaseInfo(target)

	// Find databases to create
	for name, targetDB := range targetDBs {
		currentDB, exists := currentDBs[name]
		if !exists {
			// Database needs to be created
			diff := &DatabaseDiff{
				Type:         DatabaseDiffCreate,
				DatabaseName: name,
				Description:  fmt.Sprintf("Create database '%s'", name),
				Target:       targetDB,
				UpSQL:        generateCreateDatabaseSQL(targetDB),
				DownSQL:      generateDropDatabaseSQL(targetDB),
			}
			diffs = append(diffs, diff)
		} else {
			// Database exists, check for modifications
			if needsModification(currentDB, targetDB) {
				upSQL, err := generateAlterDatabaseSQL(currentDB, targetDB)
				if err != nil {
					return nil, fmt.Errorf("failed to generate UP migration for database '%s': %w", name, err)
				}
				
				downSQL, err := generateAlterDatabaseSQL(targetDB, currentDB)
				if err != nil {
					return nil, fmt.Errorf("failed to generate DOWN migration for database '%s': %w", name, err)
				}
				
				diff := &DatabaseDiff{
					Type:         DatabaseDiffAlter,
					DatabaseName: name,
					Description:  fmt.Sprintf("Alter database '%s'", name),
					Current:      currentDB,
					Target:       targetDB,
					UpSQL:        upSQL,
					DownSQL:      downSQL,
				}
				diffs = append(diffs, diff)
			}
		}
	}

	// Find databases to drop
	for name, currentDB := range currentDBs {
		if _, exists := targetDBs[name]; !exists {
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
	}

	return diffs, nil
}

// extractDatabaseInfo extracts database information from CREATE DATABASE statements in a grammar
func extractDatabaseInfo(grammar *parser.Grammar) map[string]*DatabaseInfo {
	databases := make(map[string]*DatabaseInfo)

	for _, stmt := range grammar.Statements {
		if stmt.CreateDatabase != nil {
			db := stmt.CreateDatabase
			info := &DatabaseInfo{
				Name: db.Name,
			}

			if db.OnCluster != nil {
				info.OnCluster = *db.OnCluster
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

// needsModification checks if a database needs to be modified
func needsModification(current, target *DatabaseInfo) bool {
	return current.Comment != target.Comment ||
		current.Engine != target.Engine ||
		current.OnCluster != target.OnCluster
}

// generateCreateDatabaseSQL generates CREATE DATABASE SQL from database info
func generateCreateDatabaseSQL(db *DatabaseInfo) string {
	var parts []string
	parts = append(parts, "CREATE DATABASE", db.Name)

	if db.OnCluster != "" {
		parts = append(parts, "ON CLUSTER", db.OnCluster)
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

	if db.OnCluster != "" {
		parts = append(parts, "ON CLUSTER", db.OnCluster)
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

	if current.OnCluster != target.OnCluster {
		return "", errors.Wrapf(ErrUnsupported, "cluster change from '%s' to '%s' - requires manual intervention", current.OnCluster, target.OnCluster)
	}

	// Check if comment changed
	if current.Comment != target.Comment {
		var parts []string
		parts = append(parts, "ALTER DATABASE", target.Name)

		if target.OnCluster != "" {
			parts = append(parts, "ON CLUSTER", target.OnCluster)
		}

		parts = append(parts, "MODIFY COMMENT", fmt.Sprintf("'%s'", escapeSQL(target.Comment)))
		statements = append(statements, strings.Join(parts, " ")+";")
	}

	if len(statements) == 0 {
		return "-- No changes needed", nil
	}

	return strings.Join(statements, "\n"), nil
}

