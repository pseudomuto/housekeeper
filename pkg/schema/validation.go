package schema

import (
	"reflect"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// Engine classifications for validation

// integrationEngines contains ClickHouse engines that integrate with external systems
// These engines are read-only from ClickHouse perspective and cannot have schema modifications
var integrationEngines = map[string]bool{
	"Kafka":      true,
	"RabbitMQ":   true,
	"MySQL":      true,
	"PostgreSQL": true,
	"MongoDB":    true,
	"S3":         true,
	"HDFS":       true,
	"URL":        true,
	"File":       true,
}

// systemDatabases contains system databases that are protected from modification
var systemDatabases = map[string]bool{
	"system":             true,
	"INFORMATION_SCHEMA": true,
	"information_schema": true,
}

// engineClauseRestrictions defines which clauses are restricted for specific engine types
var engineClauseRestrictions = map[string][]string{
	"Distributed": {"PRIMARY KEY", "PARTITION BY", "SAMPLE BY", "ORDER BY"},
	"Buffer":      {"PRIMARY KEY", "PARTITION BY", "SAMPLE BY", "ORDER BY"},
	"Dictionary":  {"PRIMARY KEY", "PARTITION BY", "SAMPLE BY", "ORDER BY"},
	"View":        {"PRIMARY KEY", "PARTITION BY", "SAMPLE BY", "ORDER BY"},
	"LiveView":    {"PRIMARY KEY", "PARTITION BY", "SAMPLE BY", "ORDER BY"},
	"Memory":      {"PARTITION BY", "SAMPLE BY"}, // Memory supports ORDER BY and PRIMARY KEY
}

// equalAST is a generic helper for comparing AST types with Equal() methods
func equalAST[T interface{ Equal(T) bool }](a, b T) bool {
	// Use reflection to check if pointers are nil
	aVal := reflect.ValueOf(a)
	bVal := reflect.ValueOf(b)

	aIsNil := !aVal.IsValid() || (aVal.Kind() == reflect.Ptr && aVal.IsNil())
	bIsNil := !bVal.IsValid() || (bVal.Kind() == reflect.Ptr && bVal.IsNil())

	if aIsNil && bIsNil {
		return true
	}
	if aIsNil || bIsNil {
		return false
	}

	return a.Equal(b)
}

// isIntegrationEngine checks if an engine represents an integration engine
func isIntegrationEngine(engine *parser.TableEngine) bool {
	if engine == nil {
		return false
	}
	return integrationEngines[engine.Name]
}

// isSystemDatabase checks if a database name is a system database
func isSystemDatabase(dbName string) bool {
	return systemDatabases[dbName]
}

// validateTableOperation validates table operations for invalid migration rules
func validateTableOperation(current, target *TableInfo) error {
	// Category 1: Integration Engine Restrictions
	// Note: Integration engine modifications are now handled by DROP+CREATE strategy
	// instead of returning errors, so no validation is needed here for integration engines

	// Category 3: ON CLUSTER Configuration Changes
	if current != nil && target != nil {
		if current.Cluster != target.Cluster {
			return errors.Wrapf(ErrUnsupported,
				"cannot change cluster from '%s' to '%s': %v", current.Cluster, target.Cluster, ErrClusterChange)
		}
	}

	// Category 4: Engine Type Changes
	if current != nil && target != nil && !equalAST(current.Engine, target.Engine) {
		currentEngineName := ""
		targetEngineName := ""
		if current.Engine != nil {
			currentEngineName = current.Engine.Name
		}
		if target.Engine != nil {
			targetEngineName = target.Engine.Name
		}
		return errors.Wrapf(ErrUnsupported,
			"cannot change engine from %s to %s: %v", currentEngineName, targetEngineName, ErrEngineChange)
	}

	// Category 7: System Object Protection
	if current != nil && isSystemDatabase(current.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot modify system table %s.%s: %v", current.Database, current.Name, ErrSystemObject)
	}
	if target != nil && isSystemDatabase(target.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot create system table %s.%s: %v", target.Database, target.Name, ErrSystemObject)
	}

	// Category 8: Clause Restrictions for Engine Types
	if target != nil {
		if err := validateTableClauses(target); err != nil {
			return err
		}
	}

	return nil
}

// validateDatabaseOperation validates database operations for invalid migration rules
func validateDatabaseOperation(current, target *DatabaseInfo) error {
	// Category 3: ON CLUSTER Configuration Changes
	if current != nil && target != nil {
		if current.Cluster != target.Cluster {
			return errors.Wrapf(ErrUnsupported,
				"cannot change cluster from '%s' to '%s': %v", current.Cluster, target.Cluster, ErrClusterChange)
		}
	}

	// Category 4: Engine Type Changes
	if current != nil && target != nil {
		if current.Engine != target.Engine {
			return errors.Wrapf(ErrUnsupported,
				"cannot change database engine from %s to %s: %v", current.Engine, target.Engine, ErrEngineChange)
		}
	}

	// Category 7: System Object Protection
	if current != nil && isSystemDatabase(current.Name) {
		return errors.Wrapf(ErrUnsupported,
			"cannot modify system database %s: %v", current.Name, ErrSystemObject)
	}
	if target != nil && isSystemDatabase(target.Name) {
		return errors.Wrapf(ErrUnsupported,
			"cannot create system database %s: %v", target.Name, ErrSystemObject)
	}

	return nil
}

// validateDictionaryOperation validates dictionary operations for invalid migration rules
func validateDictionaryOperation(current, target *DictionaryInfo) error {
	// Note: Dictionary REPLACE operations are allowed and handled by the migrator
	// The migrator uses CREATE OR REPLACE for all dictionary modifications
	// We only validate other invalid operations here

	// Category 3: ON CLUSTER Configuration Changes
	if current != nil && target != nil {
		if current.Cluster != target.Cluster {
			return errors.Wrapf(ErrUnsupported,
				"cannot change cluster from '%s' to '%s': %v", current.Cluster, target.Cluster, ErrClusterChange)
		}
	}

	// Category 7: System Object Protection
	if current != nil && isSystemDatabase(current.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot modify system dictionary %s.%s: %v", current.Database, current.Name, ErrSystemObject)
	}
	if target != nil && isSystemDatabase(target.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot create system dictionary %s.%s: %v", target.Database, target.Name, ErrSystemObject)
	}

	return nil
}

// validateViewOperation validates view operations for invalid migration rules
func validateViewOperation(current, target *ViewInfo) error {
	// Category 3: ON CLUSTER Configuration Changes
	if current != nil && target != nil {
		if current.Cluster != target.Cluster {
			return errors.Wrapf(ErrUnsupported,
				"cannot change cluster from '%s' to '%s': %v", current.Cluster, target.Cluster, ErrClusterChange)
		}
	}

	// Category 5: Materialized View Query Changes
	// Note: Materialized view query changes are now handled by DROP+CREATE approach
	// instead of returning an error, so no validation is needed here

	// Category 7: System Object Protection
	if current != nil && isSystemDatabase(current.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot modify system view %s.%s: %v", current.Database, current.Name, ErrSystemObject)
	}
	if target != nil && isSystemDatabase(target.Database) {
		return errors.Wrapf(ErrUnsupported,
			"cannot create system view %s.%s: %v", target.Database, target.Name, ErrSystemObject)
	}

	return nil
}

// validateTableClauses validates that table clauses are appropriate for the engine type
func validateTableClauses(table *TableInfo) error {
	if table.Engine == nil {
		return nil // No engine specified, can't validate
	}

	restrictedClauses, hasRestrictions := engineClauseRestrictions[table.Engine.Name]
	if !hasRestrictions {
		return nil // Engine has no clause restrictions
	}

	// Define clause descriptors: name and presence check
	type clauseDescriptor struct {
		name    string
		present bool
	}
	clauses := []clauseDescriptor{
		{"PRIMARY KEY", table.PrimaryKey != nil},
		{"PARTITION BY", table.PartitionBy != nil},
		{"SAMPLE BY", table.SampleBy != nil},
		{"ORDER BY", table.OrderBy != nil},
	}

	var foundInvalidClauses []string
	restrictedSet := make(map[string]struct{}, len(restrictedClauses))
	for _, rc := range restrictedClauses {
		restrictedSet[rc] = struct{}{}
	}
	for _, clause := range clauses {
		if clause.present {
			if _, restricted := restrictedSet[clause.name]; restricted {
				foundInvalidClauses = append(foundInvalidClauses, clause.name)
			}
		}
	}

	if len(foundInvalidClauses) > 0 {
		clauseList := strings.Join(foundInvalidClauses, ", ")
		return errors.Wrapf(ErrUnsupported,
			"%s clause(s) not supported for %s tables: %v",
			clauseList, table.Engine.Name, ErrInvalidClause)
	}

	return nil
}
