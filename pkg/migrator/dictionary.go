package migrator

import (
	"fmt"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// DictionaryDiffCreate indicates a dictionary needs to be created
	DictionaryDiffCreate DictionaryDiffType = "CREATE"
	// DictionaryDiffDrop indicates a dictionary needs to be dropped
	DictionaryDiffDrop DictionaryDiffType = "DROP"
	// DictionaryDiffReplace indicates a dictionary needs to be replaced (since dictionaries can't be altered)
	DictionaryDiffReplace DictionaryDiffType = "REPLACE"
	// DictionaryDiffRename indicates a dictionary needs to be renamed
	DictionaryDiffRename DictionaryDiffType = "RENAME"
)

type (
	// DictionaryDiff represents a difference between current and target dictionary states.
	// It contains all information needed to generate migration SQL statements for
	// dictionary operations including CREATE, DROP, REPLACE, and RENAME.
	DictionaryDiff struct {
		Type              DictionaryDiffType // Type of operation (CREATE, DROP, REPLACE, RENAME)
		DictionaryName    string             // Full name of the dictionary (database.name)
		Description       string             // Human-readable description of the change
		UpSQL             string             // SQL to apply the change (forward migration)
		DownSQL           string             // SQL to rollback the change (reverse migration)
		Current           *DictionaryInfo    // Current state (nil if dictionary doesn't exist)
		Target            *DictionaryInfo    // Target state (nil if dictionary should be dropped)
		NewDictionaryName string             // For rename operations - the new full name
	}

	// DictionaryDiffType represents the type of dictionary difference
	DictionaryDiffType string

	// DictionaryInfo represents parsed dictionary information extracted from DDL statements.
	// This structure contains all the properties needed for dictionary comparison and
	// migration generation, including the full parsed statement for deep comparison.
	DictionaryInfo struct {
		Name      string                       // Dictionary name
		Database  string                       // Database name (empty for default database)
		OnCluster string                       // Cluster name if specified (empty if not clustered)
		Comment   string                       // Dictionary comment (without quotes)
		Statement *parser.CreateDictionaryStmt // Full parsed CREATE DICTIONARY statement for deep comparison
	}
)

// compareDictionaries compares current and target dictionary schemas and returns migration diffs.
// It analyzes both schemas to identify differences and generates appropriate migration operations.
//
// The function identifies:
//   - Dictionaries that need to be created (exist in target but not current)
//   - Dictionaries that need to be dropped (exist in current but not target)
//   - Dictionaries that need to be replaced (exist in both but have differences)
//   - Dictionaries that need to be renamed (same properties but different names)
//
// Rename Detection:
// The function intelligently detects rename operations by comparing dictionary properties
// (columns, sources, layouts, lifetimes, comments) excluding the name/database. If two
// dictionaries have identical properties but different names, it generates a RENAME
// operation instead of DROP+CREATE.
//
// Since dictionaries cannot be altered in ClickHouse, any modification requires CREATE OR REPLACE.
func compareDictionaries(current, target *parser.SQL) ([]*DictionaryDiff, error) { // nolint: unparam
	var diffs []*DictionaryDiff

	// Extract dictionary information from both SQL structures
	currentDicts := extractDictionaryInfo(current)
	targetDicts := extractDictionaryInfo(target)

	// Detect renames first to avoid treating them as drop+create
	renameDiffs, processedCurrent, processedTarget := detectDictionaryRenames(currentDicts, targetDicts)
	diffs = append(diffs, renameDiffs...)

	// Find dictionaries to create or replace
	for name, targetDict := range processedTarget {
		currentDict, exists := processedCurrent[name]
		if !exists {
			// Dictionary needs to be created
			diff := &DictionaryDiff{
				Type:           DictionaryDiffCreate,
				DictionaryName: name,
				Description:    fmt.Sprintf("Create dictionary '%s'", name),
				Target:         targetDict,
				UpSQL:          generateCreateDictionarySQL(targetDict),
				DownSQL:        generateDropDictionarySQL(targetDict),
			}
			diffs = append(diffs, diff)
		} else {
			// Dictionary exists, check for modifications
			if needsDictionaryModification(currentDict, targetDict) {
				// Since dictionaries can't be altered, use CREATE OR REPLACE
				diff := &DictionaryDiff{
					Type:           DictionaryDiffReplace,
					DictionaryName: name,
					Description:    fmt.Sprintf("Replace dictionary '%s'", name),
					Current:        currentDict,
					Target:         targetDict,
					UpSQL:          generateReplaceDictionarySQL(targetDict),
					DownSQL:        generateReplaceDictionarySQL(currentDict),
				}
				diffs = append(diffs, diff)
			}
		}
	}

	// Find dictionaries to drop
	for name, currentDict := range processedCurrent {
		if _, exists := processedTarget[name]; !exists {
			// Dictionary should be dropped
			diff := &DictionaryDiff{
				Type:           DictionaryDiffDrop,
				DictionaryName: name,
				Description:    fmt.Sprintf("Drop dictionary '%s'", name),
				Current:        currentDict,
				UpSQL:          generateDropDictionarySQL(currentDict),
				DownSQL:        generateCreateDictionarySQL(currentDict),
			}
			diffs = append(diffs, diff)
		}
	}

	return diffs, nil
}

// extractDictionaryInfo extracts dictionary information from CREATE DICTIONARY statements in a grammar
func extractDictionaryInfo(sql *parser.SQL) map[string]*DictionaryInfo {
	dictionaries := make(map[string]*DictionaryInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateDictionary != nil {
			dict := stmt.CreateDictionary
			info := &DictionaryInfo{
				Name:      dict.Name,
				Statement: dict,
			}

			if dict.Database != nil {
				info.Database = *dict.Database
			}

			if dict.OnCluster != nil {
				info.OnCluster = *dict.OnCluster
			}

			if dict.Comment != nil {
				info.Comment = removeQuotes(*dict.Comment)
			}

			// Use full name (database.name) as key for uniqueness
			fullName := info.Name
			if info.Database != "" {
				fullName = info.Database + "." + info.Name
			}

			dictionaries[fullName] = info
		}
	}

	return dictionaries
}

// detectDictionaryRenames identifies potential rename operations between current and target states.
// It returns rename diffs and filtered maps with renamed dictionaries removed.
func detectDictionaryRenames(currentDicts, targetDicts map[string]*DictionaryInfo) ([]*DictionaryDiff, map[string]*DictionaryInfo, map[string]*DictionaryInfo) {
	var renameDiffs []*DictionaryDiff
	processedCurrent := make(map[string]*DictionaryInfo)
	processedTarget := make(map[string]*DictionaryInfo)

	// Copy all dictionaries to processed maps initially
	for name, dict := range currentDicts {
		processedCurrent[name] = dict
	}
	for name, dict := range targetDicts {
		processedTarget[name] = dict
	}

	// Look for potential renames: dictionaries that don't exist by name but have identical properties
	for currentName, currentDict := range currentDicts {
		if _, exists := targetDicts[currentName]; exists {
			continue // Dictionary exists in both, not a rename
		}

		// Look for a dictionary in target with identical properties but different name
		for targetName, targetDict := range targetDicts {
			if _, exists := currentDicts[targetName]; exists {
				continue // Target dictionary exists in current, not a rename target
			}

			// Check if properties match (everything except name)
			if dictionaryPropertiesMatch(currentDict, targetDict) {
				// This is a rename operation
				diff := &DictionaryDiff{
					Type:              DictionaryDiffRename,
					DictionaryName:    currentName,
					NewDictionaryName: targetName,
					Description:       fmt.Sprintf("Rename dictionary '%s' to '%s'", currentName, targetName),
					Current:           currentDict,
					Target:            targetDict,
					UpSQL:             generateRenameDictionarySQL(currentName, targetName, currentDict.OnCluster),
					DownSQL:           generateRenameDictionarySQL(targetName, currentName, currentDict.OnCluster),
				}
				renameDiffs = append(renameDiffs, diff)

				// Remove from processed maps so they're not treated as drop+create
				delete(processedCurrent, currentName)
				delete(processedTarget, targetName)
				break // Found the rename target, move to next current dictionary
			}
		}
	}

	return renameDiffs, processedCurrent, processedTarget
}

// dictionaryPropertiesMatch checks if two dictionaries have identical properties (excluding name)
func dictionaryPropertiesMatch(dict1, dict2 *DictionaryInfo) bool {
	// Compare basic metadata (excluding name)
	if dict1.Comment != dict2.Comment ||
		dict1.OnCluster != dict2.OnCluster ||
		dict1.Database != dict2.Database {
		return false
	}

	// Deep comparison of dictionary structure
	return dictionaryStatementsEqual(dict1.Statement, dict2.Statement)
}

// generateRenameDictionarySQL generates RENAME DICTIONARY SQL
func generateRenameDictionarySQL(oldName, newName, onCluster string) string {
	var parts []string
	parts = append(parts, "RENAME DICTIONARY", oldName, "TO", newName)

	if onCluster != "" {
		parts = append(parts, "ON CLUSTER", onCluster)
	}

	return strings.Join(parts, " ") + ";"
}

// needsDictionaryModification checks if a dictionary needs to be modified
// This compares the essential properties that would require a CREATE OR REPLACE
func needsDictionaryModification(current, target *DictionaryInfo) bool {
	// Basic metadata comparison
	if current.Comment != target.Comment ||
		current.OnCluster != target.OnCluster ||
		current.Database != target.Database {
		return true
	}

	// Deep comparison of dictionary structure
	return !dictionaryStatementsEqual(current.Statement, target.Statement)
}

// dictionaryStatementsEqual performs deep comparison of dictionary statements
func dictionaryStatementsEqual(current, target *parser.CreateDictionaryStmt) bool {
	if current == nil || target == nil {
		return current == target
	}

	// Compare basic flags
	if current.OrReplace != target.OrReplace ||
		!stringPtrEqual(current.IfNotExists, target.IfNotExists) {
		return false
	}

	// Compare columns
	if !dictionaryColumnsEqual(current.Columns, target.Columns) {
		return false
	}

	// Compare primary key
	if !dictionaryPrimaryKeyEqual(current.GetPrimaryKey(), target.GetPrimaryKey()) {
		return false
	}

	// Compare source
	if !dictionarySourceEqual(current.GetSource(), target.GetSource()) {
		return false
	}

	// Compare layout
	if !dictionaryLayoutEqual(current.GetLayout(), target.GetLayout()) {
		return false
	}

	// Compare lifetime
	if !dictionaryLifetimeEqual(current.GetLifetime(), target.GetLifetime()) {
		return false
	}

	// Compare settings
	if !dictionarySettingsEqual(current.GetSettings(), target.GetSettings()) {
		return false
	}

	return true
}

// Helper functions for deep comparison
func stringPtrEqual(a, b *string) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return *a == *b
}

func dictionaryColumnsEqual(a, b []*parser.DictionaryColumn) bool {
	if len(a) != len(b) {
		return false
	}
	for i, colA := range a {
		colB := b[i]
		if colA.Name != colB.Name || colA.Type != colB.Type {
			return false
		}

		// Compare defaults
		if !dictionaryColumnDefaultEqual(colA.Default, colB.Default) {
			return false
		}

		// Compare attributes
		if !dictionaryColumnAttributesEqual(colA.Attributes, colB.Attributes) {
			return false
		}
	}
	return true
}

func dictionaryColumnDefaultEqual(a, b *parser.DictionaryColumnDefault) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Type == b.Type && a.Expression == b.Expression
}

func dictionaryColumnAttributesEqual(a, b []*parser.DictionaryColumnAttr) bool {
	if len(a) != len(b) {
		return false
	}
	// Create maps to compare attributes regardless of order
	aMap := make(map[string]bool)
	bMap := make(map[string]bool)

	for _, attr := range a {
		aMap[attr.Name] = true
	}
	for _, attr := range b {
		bMap[attr.Name] = true
	}

	// Compare maps
	for name := range aMap {
		if !bMap[name] {
			return false
		}
	}
	for name := range bMap {
		if !aMap[name] {
			return false
		}
	}

	return true
}

func dictionaryPrimaryKeyEqual(a, b *parser.DictionaryPrimaryKey) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.Keys) != len(b.Keys) {
		return false
	}
	for i, keyA := range a.Keys {
		if keyA != b.Keys[i] {
			return false
		}
	}
	return true
}

func dictionarySourceEqual(a, b *parser.DictionarySource) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Name == b.Name && dictionaryParametersEqual(a.Parameters, b.Parameters)
}

func dictionaryLayoutEqual(a, b *parser.DictionaryLayout) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	return a.Name == b.Name && dictionaryParametersEqual(a.Parameters, b.Parameters)
}

func dictionaryLifetimeEqual(a, b *parser.DictionaryLifetime) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare single values
	if !stringPtrEqual(a.Single, b.Single) {
		return false
	}

	// Compare MinMax values
	if a.MinMax == nil && b.MinMax == nil {
		return true
	}
	if a.MinMax == nil || b.MinMax == nil {
		return false
	}

	// Both MinMax structures should be equivalent regardless of order
	aMin, aMax := getMinMaxValues(a.MinMax)
	bMin, bMax := getMinMaxValues(b.MinMax)

	return aMin == bMin && aMax == bMax
}

// getMinMaxValues extracts min and max values from DictionaryLifetimeMinMax regardless of order
func getMinMaxValues(minMax *parser.DictionaryLifetimeMinMax) (string, string) {
	if minMax.MinFirst != nil {
		return minMax.MinFirst.MinValue, minMax.MinFirst.MaxValue
	}
	if minMax.MaxFirst != nil {
		return minMax.MaxFirst.MinValue, minMax.MaxFirst.MaxValue
	}
	return "", ""
}

func dictionarySettingsEqual(a, b *parser.DictionarySettings) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	if len(a.Settings) != len(b.Settings) {
		return false
	}
	// Simple comparison - could be enhanced with order-independent comparison
	for i, settingA := range a.Settings {
		settingB := b.Settings[i]
		if settingA.Name != settingB.Name || settingA.Value != settingB.Value {
			return false
		}
	}
	return true
}

func dictionaryParametersEqual(a, b []*parser.DictionaryParameter) bool {
	if len(a) != len(b) {
		return false
	}
	for i, paramA := range a {
		paramB := b[i]
		if paramA.Name != paramB.Name || paramA.Value != paramB.Value {
			return false
		}
	}
	return true
}

// generateCreateDictionarySQL generates CREATE DICTIONARY SQL from dictionary info
func generateCreateDictionarySQL(dict *DictionaryInfo) string {
	return reconstructDictionarySQL(dict.Statement, false)
}

// generateReplaceDictionarySQL generates CREATE OR REPLACE DICTIONARY SQL from dictionary info
func generateReplaceDictionarySQL(dict *DictionaryInfo) string {
	return reconstructDictionarySQL(dict.Statement, true)
}

// generateDropDictionarySQL generates DROP DICTIONARY SQL from dictionary info
func generateDropDictionarySQL(dict *DictionaryInfo) string {
	var parts []string
	parts = append(parts, "DROP DICTIONARY IF EXISTS")

	if dict.Database != "" {
		parts = append(parts, dict.Database+"."+dict.Name)
	} else {
		parts = append(parts, dict.Name)
	}

	if dict.OnCluster != "" {
		parts = append(parts, "ON CLUSTER", dict.OnCluster)
	}

	return strings.Join(parts, " ") + ";"
}

// reconstructDictionarySQL reconstructs CREATE DICTIONARY SQL from parsed statement
func reconstructDictionarySQL(stmt *parser.CreateDictionaryStmt, useOrReplace bool) string {
	var parts []string

	parts = buildDictionaryHeader(parts, stmt, useOrReplace)
	parts = buildDictionaryColumns(parts, stmt)
	parts = buildDictionaryOptions(parts, stmt)

	return strings.Join(parts, " ") + ";"
}

func buildDictionaryHeader(parts []string, stmt *parser.CreateDictionaryStmt, useOrReplace bool) []string {
	// CREATE [OR REPLACE] DICTIONARY
	if useOrReplace {
		parts = append(parts, "CREATE OR REPLACE DICTIONARY")
	} else {
		parts = append(parts, "CREATE DICTIONARY")
	}

	// IF NOT EXISTS (only if not using OR REPLACE)
	if !useOrReplace && stmt.IfNotExists != nil {
		parts = append(parts, "IF NOT EXISTS")
	}

	// [database.]name
	if stmt.Database != nil {
		parts = append(parts, *stmt.Database+"."+stmt.Name)
	} else {
		parts = append(parts, stmt.Name)
	}

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, "ON CLUSTER", *stmt.OnCluster)
	}

	return parts
}

func buildDictionaryColumns(parts []string, stmt *parser.CreateDictionaryStmt) []string {
	// Columns
	if len(stmt.Columns) > 0 {
		parts = append(parts, "(")
		var columnParts []string
		for _, col := range stmt.Columns {
			columnStr := col.Name + " " + col.Type

			// Add DEFAULT or EXPRESSION if present
			if col.Default != nil {
				columnStr += " " + col.Default.Type + " " + col.Default.Expression
			}

			// Add attributes (IS_OBJECT_ID, HIERARCHICAL, INJECTIVE)
			for _, attr := range col.Attributes {
				columnStr += " " + attr.Name
			}

			columnParts = append(columnParts, columnStr)
		}
		parts = append(parts, strings.Join(columnParts, ", "))
		parts = append(parts, ")")
	}

	// PRIMARY KEY
	if primaryKey := stmt.GetPrimaryKey(); primaryKey != nil {
		parts = append(parts, "PRIMARY KEY", strings.Join(primaryKey.Keys, ", "))
	}

	return parts
}

func buildDictionaryOptions(parts []string, stmt *parser.CreateDictionaryStmt) []string {
	// SOURCE
	if source := stmt.GetSource(); source != nil {
		parts = append(parts, buildSourceClause(source))
	}

	// LAYOUT
	if layout := stmt.GetLayout(); layout != nil {
		parts = append(parts, buildLayoutClause(layout))
	}

	// LIFETIME
	if lifetime := stmt.GetLifetime(); lifetime != nil {
		if lifetime.Single != nil {
			parts = append(parts, "LIFETIME("+*lifetime.Single+")")
		} else if lifetime.MinMax != nil {
			min, max := getMinMaxValues(lifetime.MinMax)
			parts = append(parts, "LIFETIME(MIN "+min+" MAX "+max+")")
		}
	}

	// SETTINGS
	if settings := stmt.GetSettings(); settings != nil && len(settings.Settings) > 0 {
		var settingStrs []string
		for _, setting := range settings.Settings {
			settingStrs = append(settingStrs, setting.Name+"="+setting.Value)
		}
		parts = append(parts, "SETTINGS("+strings.Join(settingStrs, ", ")+")")
	}

	// COMMENT
	if stmt.Comment != nil {
		parts = append(parts, "COMMENT", *stmt.Comment)
	}

	return parts
}

func buildSourceClause(source *parser.DictionarySource) string {
	sourceStr := "SOURCE(" + source.Name
	if len(source.Parameters) > 0 {
		var paramStrs []string
		for _, param := range source.Parameters {
			paramStrs = append(paramStrs, param.Name+" "+param.Value)
		}
		sourceStr += "(" + strings.Join(paramStrs, ", ") + ")"
	}
	return sourceStr + ")"
}

func buildLayoutClause(layout *parser.DictionaryLayout) string {
	layoutStr := "LAYOUT(" + layout.Name
	if len(layout.Parameters) > 0 {
		var paramStrs []string
		for _, param := range layout.Parameters {
			paramStrs = append(paramStrs, param.Name+" "+param.Value)
		}
		layoutStr += "(" + strings.Join(paramStrs, ", ") + ")"
	}
	return layoutStr + ")"
}
