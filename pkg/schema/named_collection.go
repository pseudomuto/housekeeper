package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
)

const (
	// NamedCollectionDiffCreate indicates a named collection needs to be created
	NamedCollectionDiffCreate NamedCollectionDiffType = "CREATE"
	// NamedCollectionDiffDrop indicates a named collection needs to be dropped
	NamedCollectionDiffDrop NamedCollectionDiffType = "DROP"
	// NamedCollectionDiffAlter indicates a named collection needs to be altered
	NamedCollectionDiffAlter NamedCollectionDiffType = "ALTER"
	// NamedCollectionDiffReplace indicates a named collection needs to be replaced
	NamedCollectionDiffReplace NamedCollectionDiffType = "REPLACE"
	// NamedCollectionDiffRename indicates a named collection needs to be renamed
	NamedCollectionDiffRename NamedCollectionDiffType = "RENAME"
)

type (
	// NamedCollectionDiff represents a difference between current and target named collection states.
	// It contains all information needed to generate migration SQL statements for
	// named collection operations including CREATE, DROP, ALTER, REPLACE, and RENAME.
	NamedCollectionDiff struct {
		Type              NamedCollectionDiffType // Type of operation (CREATE, DROP, ALTER, REPLACE, RENAME)
		CollectionName    string                  // Name of the named collection
		Description       string                  // Human-readable description of the change
		UpSQL             string                  // SQL to apply the change (forward migration)
		DownSQL           string                  // SQL to rollback the change (reverse migration)
		Current           *NamedCollectionInfo    // Current state (nil if collection doesn't exist)
		Target            *NamedCollectionInfo    // Target state (nil if collection should be dropped)
		NewCollectionName string                  // For rename operations - the new name
	}

	// NamedCollectionDiffType represents the type of named collection difference
	NamedCollectionDiffType string

	// NamedCollectionInfo represents parsed named collection information extracted from DDL statements.
	// This structure contains all the properties needed for named collection comparison and
	// migration generation, including the full parsed statement for deep comparison.
	NamedCollectionInfo struct {
		Name        string                            // Collection name
		Cluster     string                            // Cluster name if specified (empty if not clustered)
		Comment     string                            // Collection comment (without quotes)
		Parameters  map[string]string                 // Key-value parameters
		Overridable *bool                             // Global override setting (nil if not specified)
		Statement   *parser.CreateNamedCollectionStmt // Full parsed CREATE NAMED COLLECTION statement
	}
)

// extractNamedCollectionInfo extracts NamedCollectionInfo from a CREATE NAMED COLLECTION statement
func extractNamedCollectionInfo(stmt *parser.CreateNamedCollectionStmt) *NamedCollectionInfo {
	info := &NamedCollectionInfo{
		Name:      stmt.Name,
		Statement: stmt,
	}

	if stmt.OnCluster != nil {
		info.Cluster = *stmt.OnCluster
	}

	if stmt.Comment != nil {
		info.Comment = removeQuotes(*stmt.Comment)
	}

	if stmt.GlobalOverride != nil {
		overridable := stmt.GlobalOverride.IsOverridable()
		info.Overridable = &overridable
	}

	// Extract parameters
	if len(stmt.Parameters) > 0 {
		info.Parameters = make(map[string]string)
		for _, param := range stmt.Parameters {
			info.Parameters[param.Key] = param.Value.GetValue()
		}
	}

	return info
}

// generateNamedCollectionDiffs compares current and target named collections and generates differences.
// This function identifies what changes need to be made to transform the current state
// into the target state, generating appropriate SQL migration statements.
func generateNamedCollectionDiffs(current, target map[string]*NamedCollectionInfo) ([]*NamedCollectionDiff, error) {
	var diffs []*NamedCollectionDiff

	// Get sorted list of all collection names for deterministic processing
	allNames := make(map[string]bool)
	for name := range current {
		allNames[name] = true
	}
	for name := range target {
		allNames[name] = true
	}

	names := make([]string, 0, len(allNames))
	for name := range allNames {
		names = append(names, name)
	}
	sort.Strings(names)

	// Check for renames first to avoid DROP+CREATE
	processedCurrent := make(map[string]bool)
	processedTarget := make(map[string]bool)

	for _, currentName := range names {
		if processedCurrent[currentName] {
			continue
		}
		currentInfo, existsCurrent := current[currentName]
		if !existsCurrent {
			continue
		}

		// Look for a matching collection in target with different name (potential rename)
		for _, targetName := range names {
			if processedTarget[targetName] || targetName == currentName {
				continue
			}
			targetInfo, existsTarget := target[targetName]
			if !existsTarget {
				continue
			}

			// Check if this looks like a rename (same properties, different names)
			if isNamedCollectionRename(currentInfo, targetInfo) {
				// Generate rename diff
				diff := generateNamedCollectionRenameDiff(currentInfo, targetInfo)
				diffs = append(diffs, diff)

				processedCurrent[currentName] = true
				processedTarget[targetName] = true
				break
			}
		}
	}

	// Process remaining collections
	for _, name := range names {
		if processedCurrent[name] && processedTarget[name] {
			continue // Already processed as rename
		}

		currentInfo := current[name]
		targetInfo := target[name]

		switch {
		case currentInfo == nil && targetInfo != nil:
			// Collection needs to be created
			diff := generateNamedCollectionCreateDiff(targetInfo)
			diffs = append(diffs, diff)

		case currentInfo != nil && targetInfo == nil:
			// Collection needs to be dropped
			diff := generateNamedCollectionDropDiff(currentInfo)
			diffs = append(diffs, diff)

		case currentInfo != nil && targetInfo != nil:
			// Collection exists in both - check if it needs modification
			if !areNamedCollectionsEqual(currentInfo, targetInfo) {
				// Generate replace diff (ALTER operations are complex, so we use CREATE OR REPLACE)
				diff := generateNamedCollectionReplaceDiff(currentInfo, targetInfo)
				diffs = append(diffs, diff)
			}
		}
	}

	return diffs, nil
}

// isNamedCollectionRename checks if two named collections represent a rename operation
func isNamedCollectionRename(current, target *NamedCollectionInfo) bool {
	// Must have same properties except for name
	return current.Cluster == target.Cluster &&
		current.Comment == target.Comment &&
		areParametersEqual(current.Parameters, target.Parameters) &&
		areOverridesEqual(current.Overridable, target.Overridable)
}

// areNamedCollectionsEqual checks if two named collections are identical
func areNamedCollectionsEqual(current, target *NamedCollectionInfo) bool {
	return current.Name == target.Name &&
		current.Cluster == target.Cluster &&
		current.Comment == target.Comment &&
		areParametersEqual(current.Parameters, target.Parameters) &&
		areOverridesEqual(current.Overridable, target.Overridable)
}

// areParametersEqual checks if two parameter maps are equal
func areParametersEqual(current, target map[string]string) bool {
	if len(current) != len(target) {
		return false
	}
	for key, value := range current {
		if targetValue, exists := target[key]; !exists || targetValue != value {
			return false
		}
	}
	return true
}

// areOverridesEqual checks if two override settings are equal
func areOverridesEqual(current, target *bool) bool {
	if current == nil && target == nil {
		return true
	}
	if current == nil || target == nil {
		return false
	}
	return *current == *target
}

// generateNamedCollectionCreateDiff generates a CREATE NAMED COLLECTION diff
func generateNamedCollectionCreateDiff(info *NamedCollectionInfo) *NamedCollectionDiff {
	upSQL := generateCreateNamedCollectionSQL(info.Statement)

	downSQL := fmt.Sprintf("DROP NAMED COLLECTION IF EXISTS `%s`", info.Name)
	if info.Cluster != "" {
		downSQL += fmt.Sprintf(" ON CLUSTER `%s`", info.Cluster)
	}
	downSQL += ";"

	return &NamedCollectionDiff{
		Type:           NamedCollectionDiffCreate,
		CollectionName: info.Name,
		Description:    fmt.Sprintf("Create named collection '%s'", info.Name),
		UpSQL:          upSQL,
		DownSQL:        downSQL,
		Target:         info,
	}
}

// generateNamedCollectionDropDiff generates a DROP NAMED COLLECTION diff
func generateNamedCollectionDropDiff(info *NamedCollectionInfo) *NamedCollectionDiff {
	upSQL := "DROP NAMED COLLECTION IF EXISTS " + utils.BacktickIdentifier(info.Name)
	if info.Cluster != "" {
		upSQL += " ON CLUSTER " + utils.BacktickIdentifier(info.Cluster)
	}
	upSQL += ";"

	downSQL := generateCreateNamedCollectionSQL(info.Statement)

	return &NamedCollectionDiff{
		Type:           NamedCollectionDiffDrop,
		CollectionName: info.Name,
		Description:    fmt.Sprintf("Drop named collection '%s'", info.Name),
		UpSQL:          upSQL,
		DownSQL:        downSQL,
		Current:        info,
	}
}

// generateNamedCollectionReplaceDiff generates a CREATE OR REPLACE NAMED COLLECTION diff
func generateNamedCollectionReplaceDiff(current, target *NamedCollectionInfo) *NamedCollectionDiff {
	// Create the target statement with OR REPLACE
	targetStmt := *target.Statement
	targetStmt.OrReplace = true

	upSQL := generateCreateNamedCollectionSQL(&targetStmt)

	downSQL := generateCreateNamedCollectionSQL(current.Statement)

	return &NamedCollectionDiff{
		Type:           NamedCollectionDiffReplace,
		CollectionName: target.Name,
		Description:    fmt.Sprintf("Replace named collection '%s'", target.Name),
		UpSQL:          upSQL,
		DownSQL:        downSQL,
		Current:        current,
		Target:         target,
	}
}

// generateNamedCollectionRenameDiff generates a rename diff for named collections
func generateNamedCollectionRenameDiff(current, target *NamedCollectionInfo) *NamedCollectionDiff {
	// Named collections don't have a RENAME statement, so we use CREATE OR REPLACE + DROP
	// First create with new name, then drop old name

	// Create the target statement with OR REPLACE
	targetStmt := *target.Statement
	targetStmt.OrReplace = true

	upSQL := generateCreateNamedCollectionSQL(&targetStmt)
	upSQL += "\n"
	upSQL += "DROP NAMED COLLECTION IF EXISTS " + utils.BacktickIdentifier(current.Name)
	if current.Cluster != "" {
		upSQL += " ON CLUSTER " + utils.BacktickIdentifier(current.Cluster)
	}
	upSQL += ";"

	// Down migration recreates the old one and drops the new one
	downSQL := generateCreateNamedCollectionSQL(current.Statement)
	downSQL += "\n"
	downSQL += "DROP NAMED COLLECTION IF EXISTS " + utils.BacktickIdentifier(target.Name)
	if target.Cluster != "" {
		downSQL += " ON CLUSTER " + utils.BacktickIdentifier(target.Cluster)
	}
	downSQL += ";"

	return &NamedCollectionDiff{
		Type:              NamedCollectionDiffRename,
		CollectionName:    current.Name,
		NewCollectionName: target.Name,
		Description:       fmt.Sprintf("Rename named collection '%s' to '%s'", current.Name, target.Name),
		UpSQL:             upSQL,
		DownSQL:           downSQL,
		Current:           current,
		Target:            target,
	}
}

// generateCreateNamedCollectionSQL generates a CREATE NAMED COLLECTION SQL statement from a parsed statement
func generateCreateNamedCollectionSQL(stmt *parser.CreateNamedCollectionStmt) string {
	var parts []string

	// CREATE [OR REPLACE] NAMED COLLECTION
	parts = append(parts, "CREATE")
	if stmt.OrReplace {
		parts = append(parts, "OR REPLACE")
	}
	parts = append(parts, "NAMED COLLECTION")

	// IF NOT EXISTS
	if stmt.IfNotExists != nil {
		parts = append(parts, "IF NOT EXISTS")
	}

	// Collection name
	parts = append(parts, utils.BacktickIdentifier(stmt.Name))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, "ON CLUSTER", utils.BacktickIdentifier(*stmt.OnCluster))
	}

	// AS
	parts = append(parts, "AS")

	header := strings.Join(parts, " ")

	// Parameters
	paramLines := make([]string, 0, len(stmt.Parameters))
	for _, param := range stmt.Parameters {
		paramLine := fmt.Sprintf("    %s = %s", utils.BacktickIdentifier(param.Key), param.Value.GetValue())

		// Add override clause if present
		if param.Override != nil {
			if param.Override.NotOverridable {
				paramLine += " NOT OVERRIDABLE"
			} else if param.Override.Overridable {
				paramLine += " OVERRIDABLE"
			}
		}

		paramLines = append(paramLines, paramLine)
	}

	result := header
	if len(paramLines) > 0 {
		result += "\n" + strings.Join(paramLines, ",\n")
	}

	// Global override
	if stmt.GlobalOverride != nil {
		if stmt.GlobalOverride.NotOverridable {
			result += "\nNOT OVERRIDABLE"
		} else if stmt.GlobalOverride.Overridable {
			result += "\nOVERRIDABLE"
		}
	}

	// Comment
	if stmt.Comment != nil {
		result += "\nCOMMENT " + *stmt.Comment
	}

	result += ";"
	return result
}

// compareNamedCollections compares current and target named collections and generates diffs
func compareNamedCollections(current, target *parser.SQL) ([]*NamedCollectionDiff, error) {
	currentCollections := extractNamedCollections(current)
	targetCollections := extractNamedCollections(target)

	return generateNamedCollectionDiffs(currentCollections, targetCollections)
}

// extractNamedCollections extracts named collection information from SQL statements
func extractNamedCollections(sql *parser.SQL) map[string]*NamedCollectionInfo {
	collections := make(map[string]*NamedCollectionInfo)

	if sql == nil {
		return collections
	}

	for _, stmt := range sql.Statements {
		if stmt.CreateNamedCollection != nil {
			info := extractNamedCollectionInfo(stmt.CreateNamedCollection)
			collections[info.Name] = info
		}
	}

	return collections
}
