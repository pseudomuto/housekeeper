package schema

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
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
		DiffBase                 // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current  *DictionaryInfo // Current state (nil if dictionary doesn't exist)
		Target   *DictionaryInfo // Target state (nil if dictionary should be dropped)
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

// GetName implements SchemaObject interface.
// Returns the fully-qualified name (database.name or just name).
func (d *DictionaryInfo) GetName() string {
	if d.Database != "" {
		return d.Database + "." + d.Name
	}
	return d.Name
}

// GetCluster implements SchemaObject interface
func (d *DictionaryInfo) GetCluster() string {
	return d.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two dictionaries have identical properties (excluding name).
func (d *DictionaryInfo) PropertiesMatch(other SchemaObject) bool {
	otherDict, ok := other.(*DictionaryInfo)
	if !ok {
		return false
	}
	return dictionaryPropertiesMatch(d, otherDict)
}

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

	// Detect renames using generic algorithm
	renames, processedCurrent, processedTarget := DetectRenames(currentDicts, targetDicts)

	// Create rename diffs
	for _, rename := range renames {
		currentDict := currentDicts[rename.OldName]
		targetDict := targetDicts[rename.NewName]
		diff := &DictionaryDiff{
			DiffBase: DiffBase{
				Type:        string(DictionaryDiffRename),
				Name:        rename.OldName,
				NewName:     rename.NewName,
				Description: fmt.Sprintf("Rename dictionary '%s' to '%s'", rename.OldName, rename.NewName),
				UpSQL:       generateRenameDictionarySQL(rename.OldName, rename.NewName, currentDict.Cluster),
				DownSQL:     generateRenameDictionarySQL(rename.NewName, rename.OldName, currentDict.Cluster),
			},
			Current: currentDict,
			Target:  targetDict,
		}
		diffs = append(diffs, diff)
	}

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
				DiffBase: DiffBase{
					Type:        string(DictionaryDiffCreate),
					Name:        name,
					Description: fmt.Sprintf("Create dictionary '%s'", name),
					UpSQL:       generateCreateDictionarySQL(targetDict),
					DownSQL:     generateDropDictionarySQL(targetDict),
				},
				Target: targetDict,
			}
			diffs = append(diffs, diff)
		} else {
			// Dictionary exists, check for modifications
			if needsDictionaryModification(currentDict, targetDict) {
				// Since dictionaries can't be altered, use CREATE OR REPLACE
				diff := &DictionaryDiff{
					DiffBase: DiffBase{
						Type:        string(DictionaryDiffReplace),
						Name:        name,
						Description: fmt.Sprintf("Replace dictionary '%s'", name),
						UpSQL:       generateReplaceDictionarySQL(targetDict),
						DownSQL:     generateReplaceDictionarySQL(currentDict),
					},
					Current: currentDict,
					Target:  targetDict,
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
			DiffBase: DiffBase{
				Type:        string(DictionaryDiffDrop),
				Name:        name,
				Description: fmt.Sprintf("Drop dictionary '%s'", name),
				UpSQL:       generateDropDictionarySQL(currentDict),
				DownSQL:     generateCreateDictionarySQL(currentDict),
			},
			Current: currentDict,
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
	return utils.NewSQLBuilder().
		Rename("DICTIONARY").
		Name(oldName).
		To(newName).
		OnCluster(onCluster).
		String()
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

// expressionEqual compares expressions using AST-based structural equality
func expressionEqual(a, b parser.Expression) bool {
	return a.Equal(&b)
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
	var database *string
	if dict.Database != "" {
		database = &dict.Database
	}

	return utils.NewSQLBuilder().
		Drop("DICTIONARY").
		IfExists().
		QualifiedName(database, dict.Name).
		OnCluster(dict.Cluster).
		String()
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
			var columnStrSb807 strings.Builder
			for _, attr := range col.Attributes {
				columnStrSb807.WriteString(" " + attr.Name)
			}
			columnStr += columnStrSb807.String()

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
