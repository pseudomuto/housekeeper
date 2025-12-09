package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
)

const (
	// FunctionDiffCreate indicates a function needs to be created
	FunctionDiffCreate FunctionDiffType = "CREATE"
	// FunctionDiffDrop indicates a function needs to be dropped
	FunctionDiffDrop FunctionDiffType = "DROP"
	// FunctionDiffReplace indicates a function needs to be replaced (functions use DROP+CREATE for modifications)
	FunctionDiffReplace FunctionDiffType = "REPLACE"
	// FunctionDiffRename indicates a function needs to be renamed
	FunctionDiffRename FunctionDiffType = "RENAME"
)

type (
	// FunctionDiff represents a difference between current and target function states.
	// It contains all information needed to generate migration SQL statements for
	// function operations including CREATE, DROP, REPLACE, and RENAME.
	FunctionDiff struct {
		Type            FunctionDiffType // Type of operation (CREATE, DROP, REPLACE, RENAME)
		FunctionName    string           // Name of the function being modified
		Description     string           // Human-readable description of the change
		UpSQL           string           // SQL to apply the change (forward migration)
		DownSQL         string           // SQL to rollback the change (reverse migration)
		Current         *FunctionInfo    // Current state (nil if function doesn't exist)
		Target          *FunctionInfo    // Target state (nil if function should be dropped)
		NewFunctionName string           // For rename operations - the new name
	}

	// FunctionDiffType represents the type of function difference
	FunctionDiffType string

	// FunctionInfo represents parsed function information extracted from DDL statements.
	// This structure contains all the properties needed for function comparison and
	// migration generation, including parameters, expression, and cluster configuration.
	FunctionInfo struct {
		Name       string             // Function name
		Parameters []string           // Function parameter names
		Expression *parser.Expression // Function expression/body AST
		Cluster    string             // Cluster name if specified (empty if not clustered)
	}
)

// compareFunctions compares current and target function schemas and returns migration diffs.
// It analyzes both schemas to identify differences and generates appropriate migration operations.
//
// The function identifies:
//   - Functions that need to be created (exist in target but not current)
//   - Functions that need to be dropped (exist in current but not target)
//   - Functions that need to be replaced (exist in both but have differences)
//   - Functions that need to be renamed (same properties but different names)
//
// Note: ClickHouse functions use DROP+CREATE strategy for modifications since
// ALTER FUNCTION doesn't exist.
//
// Rename Detection:
// The function intelligently detects rename operations by comparing function properties
// (parameters, expression, cluster) excluding the name. If two functions have identical
// properties but different names, it generates a RENAME operation instead of DROP+CREATE.
func compareFunctions(current, target *parser.SQL) []*FunctionDiff {
	// Extract function information from both SQL structures
	currentFunctions := extractFunctionInfo(current)
	targetFunctions := extractFunctionInfo(target)

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*FunctionDiff, 0, len(currentFunctions)+len(targetFunctions))

	// Create maps for quick lookup
	currentMap := make(map[string]*FunctionInfo)
	targetMap := make(map[string]*FunctionInfo)

	for _, fn := range currentFunctions {
		currentMap[fn.Name] = fn
	}
	for _, fn := range targetFunctions {
		targetMap[fn.Name] = fn
	}

	// Track processed functions for rename detection
	processedCurrent := make(map[string]bool)
	processedTarget := make(map[string]bool)

	// Process functions in order: exact matches, renames, then creations/deletions
	exactMatchDiffs := processExactMatches(currentMap, targetMap, processedCurrent, processedTarget)
	diffs = append(diffs, exactMatchDiffs...)

	renameDiffs := detectFunctionRenames(currentMap, targetMap, processedCurrent, processedTarget)
	diffs = append(diffs, renameDiffs...)

	creationDeletionDiffs := processCreationsAndDeletions(currentMap, targetMap, processedCurrent, processedTarget)
	diffs = append(diffs, creationDeletionDiffs...)

	return diffs
}

// extractFunctionInfo extracts function information from parsed SQL statements
func extractFunctionInfo(sql *parser.SQL) []*FunctionInfo {
	var functions []*FunctionInfo

	for _, stmt := range sql.Statements {
		if stmt.CreateFunction != nil {
			fn := &FunctionInfo{
				Name:       stmt.CreateFunction.Name,
				Parameters: make([]string, len(stmt.CreateFunction.Parameters)),
				Expression: stmt.CreateFunction.Expression,
			}

			// Extract parameter names
			for i, param := range stmt.CreateFunction.Parameters {
				fn.Parameters[i] = param.Name
			}

			// Extract cluster if specified
			if stmt.CreateFunction.OnCluster != nil {
				fn.Cluster = *stmt.CreateFunction.OnCluster
			}

			functions = append(functions, fn)
		}
	}

	return functions
}

// functionsEqual compares two functions for equality including names
func functionsEqual(a, b *FunctionInfo) bool {
	if a.Name != b.Name {
		return false
	}
	return functionsEqualIgnoringName(a, b)
}

// functionsEqualIgnoringName compares two functions for equality excluding names
func functionsEqualIgnoringName(a, b *FunctionInfo) bool {
	if a.Cluster != b.Cluster {
		return false
	}

	if !expressionsAreEqual(a.Expression, b.Expression) {
		return false
	}

	if len(a.Parameters) != len(b.Parameters) {
		return false
	}

	// Compare parameters in order
	for i, param := range a.Parameters {
		if param != b.Parameters[i] {
			return false
		}
	}

	return true
}

// generateCreateFunctionSQL generates CREATE FUNCTION SQL statement
func generateCreateFunctionSQL(fn *FunctionInfo) string {
	// Build parameter string
	paramStr := "("
	var paramStrSb168 strings.Builder
	for i, param := range fn.Parameters {
		if i > 0 {
			paramStrSb168.WriteString(", ")
		}
		paramStrSb168.WriteString(fmt.Sprintf("`%s`", param))
	}
	paramStr += paramStrSb168.String()
	paramStr += ")"

	// Build full AS expression
	asExpression := fmt.Sprintf("%s -> %v", paramStr, fn.Expression)

	return utils.NewSQLBuilder().
		Create("FUNCTION").
		Name(fn.Name).
		OnCluster(fn.Cluster).
		As(asExpression).
		String()
}

// generateDropFunctionSQL generates DROP FUNCTION SQL statement
func generateDropFunctionSQL(fn *FunctionInfo) string {
	return utils.NewSQLBuilder().
		Drop("FUNCTION").
		IfExists().
		Name(fn.Name).
		OnCluster(fn.Cluster).
		String()
}

// detectFunctionRenames detects rename operations by comparing function properties
func detectFunctionRenames(currentMap, targetMap map[string]*FunctionInfo, processedCurrent, processedTarget map[string]bool) []*FunctionDiff {
	var diffs []*FunctionDiff

	// Sort names for deterministic ordering
	var currentNames []string
	for name := range currentMap {
		if !processedCurrent[name] {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, currentName := range currentNames {
		if processedCurrent[currentName] {
			continue
		}
		currentFn := currentMap[currentName]

		// Sort target names as well for deterministic ordering
		var targetNames []string
		for name := range targetMap {
			if !processedTarget[name] {
				targetNames = append(targetNames, name)
			}
		}
		sort.Strings(targetNames)

		for _, targetName := range targetNames {
			if processedTarget[targetName] {
				continue
			}
			targetFn := targetMap[targetName]

			// Check if functions have same properties but different names
			if functionsEqualIgnoringName(currentFn, targetFn) {
				processedCurrent[currentName] = true
				processedTarget[targetName] = true

				diff := &FunctionDiff{
					Type:            FunctionDiffRename,
					FunctionName:    currentName,
					NewFunctionName: targetName,
					Description:     fmt.Sprintf("Rename function %s to %s", currentName, targetName),
					Current:         currentFn,
					Target:          targetFn,
				}
				// Functions don't have RENAME support, use DROP+CREATE
				diff.UpSQL = generateDropFunctionSQL(currentFn) + "\n" + generateCreateFunctionSQL(targetFn)
				diff.DownSQL = generateDropFunctionSQL(targetFn) + "\n" + generateCreateFunctionSQL(currentFn)
				diffs = append(diffs, diff)
				break
			}
		}
	}

	return diffs
}

// processExactMatches processes functions with exact name matches for modifications
func processExactMatches(currentMap, targetMap map[string]*FunctionInfo, processedCurrent, processedTarget map[string]bool) []*FunctionDiff {
	var diffs []*FunctionDiff

	// Find exact matches first (same name, check for modifications)
	// Sort names for deterministic ordering
	var matchNames []string
	for name := range targetMap {
		if _, exists := currentMap[name]; exists {
			matchNames = append(matchNames, name)
		}
	}
	sort.Strings(matchNames)

	for _, name := range matchNames {
		targetFn := targetMap[name]
		currentFn := currentMap[name]
		processedCurrent[name] = true
		processedTarget[name] = true

		if !functionsEqual(currentFn, targetFn) {
			// Function exists but is different - needs replacement
			diff := &FunctionDiff{
				Type:         FunctionDiffReplace,
				FunctionName: name,
				Description:  "Replace function " + name,
				Current:      currentFn,
				Target:       targetFn,
			}
			diff.UpSQL = generateCreateFunctionSQL(targetFn)
			diff.DownSQL = generateCreateFunctionSQL(currentFn)
			diffs = append(diffs, diff)
		}
	}

	return diffs
}

// processCreationsAndDeletions processes functions that need to be created or dropped
func processCreationsAndDeletions(currentMap, targetMap map[string]*FunctionInfo, processedCurrent, processedTarget map[string]bool) []*FunctionDiff {
	diffs := make([]*FunctionDiff, 0, 10)

	// Find functions to create (exist in target but not current, and not processed as renames)
	// Sort names for deterministic ordering
	var createNames []string
	for name := range targetMap {
		if !processedTarget[name] {
			createNames = append(createNames, name)
		}
	}
	sort.Strings(createNames)

	for _, name := range createNames {
		targetFn := targetMap[name]
		diff := &FunctionDiff{
			Type:         FunctionDiffCreate,
			FunctionName: name,
			Description:  "Create function " + name,
			Target:       targetFn,
		}
		diff.UpSQL = generateCreateFunctionSQL(targetFn)
		diff.DownSQL = generateDropFunctionSQL(targetFn)
		diffs = append(diffs, diff)
	}

	// Find functions to drop (exist in current but not target, and not processed as renames)
	// Sort names for deterministic ordering
	var dropNames []string
	for name := range currentMap {
		if !processedCurrent[name] {
			dropNames = append(dropNames, name)
		}
	}
	sort.Strings(dropNames)

	for _, name := range dropNames {
		currentFn := currentMap[name]
		diff := &FunctionDiff{
			Type:         FunctionDiffDrop,
			FunctionName: name,
			Description:  "Drop function " + name,
			Current:      currentFn,
		}
		diff.UpSQL = generateDropFunctionSQL(currentFn)
		diff.DownSQL = generateCreateFunctionSQL(currentFn)
		diffs = append(diffs, diff)
	}

	return diffs
}

// GetDiffType returns the diff type for FunctionDiff (implements diffProcessor interface)
func (d *FunctionDiff) GetDiffType() string {
	return string(d.Type)
}

// GetUpSQL returns the up SQL for FunctionDiff (implements diffProcessor interface)
func (d *FunctionDiff) GetUpSQL() string {
	return d.UpSQL
}
