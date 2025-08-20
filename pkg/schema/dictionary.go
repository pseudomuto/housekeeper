package schema

import (
	"fmt"
	"regexp"
	"sort"
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
		Cluster   string                       // Cluster name if specified (empty if not clustered)
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
func compareDictionaries(current, target *parser.SQL) ([]*DictionaryDiff, error) {
	// Extract dictionary information from both SQL structures
	currentDicts := extractDictionaryInfo(current)
	targetDicts := extractDictionaryInfo(target)

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*DictionaryDiff, 0, len(currentDicts)+len(targetDicts))

	// Detect renames first to avoid treating them as drop+create
	renameDiffs, processedCurrent, processedTarget := detectDictionaryRenames(currentDicts, targetDicts)
	diffs = append(diffs, renameDiffs...)

	// Find dictionaries to create or replace - sorted for deterministic order
	targetNames := make([]string, 0, len(processedTarget))
	for name := range processedTarget {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	for _, name := range targetNames {
		targetDict := processedTarget[name]
		currentDict, exists := processedCurrent[name]

		// Validate operation before proceeding
		if err := validateDictionaryOperation(currentDict, targetDict); err != nil {
			return nil, err
		}

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

	// Find dictionaries to drop - sorted for deterministic order
	currentNames := make([]string, 0, len(processedCurrent))
	for name := range processedCurrent {
		if _, exists := processedTarget[name]; !exists {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentDict := processedCurrent[name]
		// Validate drop operation
		if err := validateDictionaryOperation(currentDict, nil); err != nil {
			return nil, err
		}

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

	return diffs, nil
}

// extractDictionaryInfo extracts dictionary information from CREATE DICTIONARY statements in parsed SQL
func extractDictionaryInfo(sql *parser.SQL) map[string]*DictionaryInfo {
	dictionaries := make(map[string]*DictionaryInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateDictionary != nil {
			dict := stmt.CreateDictionary
			info := &DictionaryInfo{
				Name:      normalizeIdentifier(dict.Name),
				Statement: dict,
			}

			if dict.Database != nil {
				info.Database = normalizeIdentifier(*dict.Database)
			}

			if dict.OnCluster != nil {
				info.Cluster = *dict.OnCluster
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
					UpSQL:             generateRenameDictionarySQL(currentName, targetName, currentDict.Cluster),
					DownSQL:           generateRenameDictionarySQL(targetName, currentName, currentDict.Cluster),
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
	// Compare basic metadata (excluding name) with normalized comment comparison
	if !commentsEqual(dict1.Comment, dict2.Comment) ||
		dict1.Database != dict2.Database {
		return false
	}

	if dict1.Cluster != dict2.Cluster {
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
	// Basic metadata comparison with normalized comment comparison
	if !commentsEqual(current.Comment, target.Comment) ||
		current.Database != target.Database {
		return true
	}

	if current.Cluster != target.Cluster {
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
	// Compare type and expression using AST-based comparison
	return a.Type == b.Type && expressionsAreEqual(&a.Expression, &b.Expression)
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

	// Handle ClickHouse's automatic layout conversion:
	// HASHED is converted to COMPLEX_KEY_HASHED for string keys
	if layoutsEquivalent(a.Name, b.Name) {
		return dictionaryParametersEqual(a.Parameters, b.Parameters)
	}

	return false
}

// layoutsEquivalent checks if two layout names are semantically equivalent
// ClickHouse converts HASHED to COMPLEX_KEY_HASHED for string primary keys
func layoutsEquivalent(a, b string) bool {
	if a == b {
		return true
	}

	// Handle ClickHouse automatic conversion
	return (a == "HASHED" && b == "COMPLEX_KEY_HASHED") ||
		(a == "COMPLEX_KEY_HASHED" && b == "HASHED")
}

func dictionaryLifetimeEqual(a, b *parser.DictionaryLifetime) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Extract effective values from both formats
	aValue := getEffectiveLifetimeValue(a)
	bValue := getEffectiveLifetimeValue(b)

	return aValue == bValue
}

// getEffectiveLifetimeValue extracts the effective lifetime value from either single or MinMax format
// For MinMax with MIN 0, it returns the MAX value (equivalent to single value)
// For other MinMax formats, it returns a normalized "MIN x MAX y" string
func getEffectiveLifetimeValue(lifetime *parser.DictionaryLifetime) string {
	if lifetime.Single != nil {
		return *lifetime.Single
	}

	if lifetime.MinMax != nil {
		min, max := getMinMaxValues(lifetime.MinMax)
		// If MIN is 0, treat it as equivalent to single value of MAX
		if min == "0" {
			return max
		}
		// For non-zero MIN, return the full range representation
		return "MIN " + min + " MAX " + max
	}

	return ""
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

	// Create maps for order-independent comparison using parsed parameter structures
	aParams := make(map[string]*parser.DictionaryParameter)
	for _, param := range a {
		aParams[strings.ToLower(param.GetName())] = param
	}

	bParams := make(map[string]*parser.DictionaryParameter)
	for _, param := range b {
		bParams[strings.ToLower(param.GetName())] = param
	}

	// Compare the parsed parameter structures
	for name, paramA := range aParams {
		paramB, exists := bParams[name]
		if !exists {
			return false
		}

		if !dictionaryParameterEqual(paramA, paramB) {
			return false
		}
	}

	return true
}

// dictionaryParameterEqual compares two parsed dictionary parameters for semantic equality
// This handles the structural comparison of parameters regardless of formatting differences
func dictionaryParameterEqual(a, b *parser.DictionaryParameter) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Both parameters must have the same structure type (SimpleParam vs DSLFunction)
	if (a.SimpleParam != nil) != (b.SimpleParam != nil) {
		return false
	}
	if (a.DSLFunction != nil) != (b.DSLFunction != nil) {
		return false
	}

	// Compare simple parameters (name value pairs like: url 'http://...')
	if a.SimpleParam != nil && b.SimpleParam != nil {
		return simpleParameterEqual(a.SimpleParam, b.SimpleParam)
	}

	// Compare DSL function parameters (like: headers(header(name 'Content-Type' value 'application/json')))
	if a.DSLFunction != nil && b.DSLFunction != nil {
		return dslFunctionEqual(a.DSLFunction, b.DSLFunction)
	}

	return false
}

// simpleParameterEqual compares simple name-value parameters
func simpleParameterEqual(a, b *parser.SimpleParameter) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare parameter names (case-insensitive)
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}

	// Compare expression values semantically
	return expressionEqual(a.Expression, b.Expression)
}

// dslFunctionEqual compares DSL function parameters (like headers, credentials, etc.)
func dslFunctionEqual(a, b *parser.DictionaryDSLFunc) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare function names (case-insensitive)
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}

	// Compare function parameters
	return dslParamsEqual(a.Params, b.Params)
}

// dslParamsEqual compares DSL function parameter lists
func dslParamsEqual(a, b []*parser.DictionaryDSLParam) bool {
	if len(a) != len(b) {
		return false
	}

	// For small parameter lists, do order-independent comparison
	if len(a) <= 10 {
		return dslParamsEqualUnordered(a, b)
	}

	// For larger lists, assume order matters for performance
	for i, paramA := range a {
		if !dslParamEqual(paramA, b[i]) {
			return false
		}
	}

	return true
}

// dslParamsEqualUnordered compares DSL parameters in order-independent way
func dslParamsEqualUnordered(a, b []*parser.DictionaryDSLParam) bool {
	// Create a map of used indices to avoid double-matching
	used := make(map[int]bool)

	for _, paramA := range a {
		found := false
		for i, paramB := range b {
			if used[i] {
				continue
			}
			if dslParamEqual(paramA, paramB) {
				used[i] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

// dslParamEqual compares individual DSL parameters
func dslParamEqual(a, b *parser.DictionaryDSLParam) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Both parameters must have the same structure type (SimpleParam vs NestedFunc)
	if (a.SimpleParam != nil) != (b.SimpleParam != nil) {
		return false
	}
	if (a.NestedFunc != nil) != (b.NestedFunc != nil) {
		return false
	}

	// Compare simple DSL parameters
	if a.SimpleParam != nil && b.SimpleParam != nil {
		return simpleDSLParamEqual(a.SimpleParam, b.SimpleParam)
	}

	// Compare nested DSL functions
	if a.NestedFunc != nil && b.NestedFunc != nil {
		return dslFunctionEqual(a.NestedFunc, b.NestedFunc)
	}

	return false
}

// simpleDSLParamEqual compares simple DSL parameters (name-value pairs)
func simpleDSLParamEqual(a, b *parser.SimpleDSLParam) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}

	// Compare parameter names (case-insensitive)
	if !strings.EqualFold(a.Name, b.Name) {
		return false
	}

	// Compare parameter values semantically
	return expressionEqual(a.Value, b.Value)
}

// expressionEqual compares expressions for semantic equality
// This handles different formatting of the same logical value
func expressionEqual(a, b parser.Expression) bool {
	// Get the string representations
	aStr := a.String()
	bStr := b.String()

	// If they're exactly equal, we're done
	if aStr == bStr {
		return true
	}

	// Normalize whitespace and quotes for comparison
	aNorm := normalizeExpressionValue(aStr)
	bNorm := normalizeExpressionValue(bStr)

	return aNorm == bNorm
}

// normalizeExpressionValue normalizes expression strings for comparison
func normalizeExpressionValue(value string) string {
	// Trim and normalize whitespace
	normalized := strings.TrimSpace(value)
	normalized = regexp.MustCompile(`\s+`).ReplaceAllString(normalized, " ")

	// Normalize quotes - remove spaces around quotes
	normalized = regexp.MustCompile(`\s*'\s*`).ReplaceAllString(normalized, "'")
	normalized = regexp.MustCompile(`\s*"\s*`).ReplaceAllString(normalized, "\"")

	return normalized
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

	if dict.Cluster != "" {
		parts = append(parts, "ON CLUSTER", dict.Cluster)
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
				columnStr += " " + col.Default.Type + " " + col.Default.GetValue()
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
			settingStrs = append(settingStrs, setting.Name+" = "+setting.Value)
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
			if param.DSLFunction != nil {
				paramStrs = append(paramStrs, param.GetValue())
			} else {
				paramStrs = append(paramStrs, param.GetName()+" "+param.GetValue())
			}
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
			if param.DSLFunction != nil {
				paramStrs = append(paramStrs, param.GetValue())
			} else {
				paramStrs = append(paramStrs, param.GetName()+" "+param.GetValue())
			}
		}
		layoutStr += "(" + strings.Join(paramStrs, ", ") + ")"
	}
	return layoutStr + ")"
}

// commentsEqual compares two comments with normalization for SQL keywords
// ClickHouse normalizes keywords even within string literals, so we need to handle this
func commentsEqual(comment1, comment2 string) bool {
	return normalizeComment(comment1) == normalizeComment(comment2)
}

// normalizeComment normalizes SQL keywords in comments to handle ClickHouse keyword normalization
func normalizeComment(comment string) string {
	if comment == "" {
		return comment
	}

	// Common SQL keywords that ClickHouse might normalize in comments
	keywords := []string{"FROM", "TO", "AS", "WHERE", "BY", "WITH", "AND", "OR"}

	result := comment
	for _, keyword := range keywords {
		// Replace both cases with uppercase to normalize
		lowerPattern := "\\b" + strings.ToLower(keyword) + "\\b"
		upperPattern := "\\b" + strings.ToUpper(keyword) + "\\b"

		lowerRegex := regexp.MustCompile(lowerPattern)
		upperRegex := regexp.MustCompile(upperPattern)

		result = lowerRegex.ReplaceAllString(result, strings.ToUpper(keyword))
		result = upperRegex.ReplaceAllString(result, strings.ToUpper(keyword))
	}

	return result
}
