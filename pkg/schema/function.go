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
		DiffBase               // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current  *FunctionInfo // Current state (nil if function doesn't exist)
		Target   *FunctionInfo // Target state (nil if function should be dropped)
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

// GetName implements SchemaObject interface
func (f *FunctionInfo) GetName() string {
	return f.Name
}

// GetCluster implements SchemaObject interface
func (f *FunctionInfo) GetCluster() string {
	return f.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two functions have identical properties (excluding name).
func (f *FunctionInfo) PropertiesMatch(other SchemaObject) bool {
	otherFn, ok := other.(*FunctionInfo)
	if !ok {
		return false
	}
	return functionsEqualIgnoringName(f, otherFn)
}

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
	currentMap := extractFunctionInfoAsMap(current)
	targetMap := extractFunctionInfoAsMap(target)

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*FunctionDiff, 0, len(currentMap)+len(targetMap))

	// Detect renames using generic algorithm
	renames, processedCurrent, processedTarget := DetectRenames(currentMap, targetMap)

	// Create rename diffs
	for _, rename := range renames {
		currentFn := currentMap[rename.OldName]
		targetFn := targetMap[rename.NewName]
		diff := &FunctionDiff{
			DiffBase: DiffBase{
				Type:        string(FunctionDiffRename),
				Name:        rename.OldName,
				NewName:     rename.NewName,
				Description: fmt.Sprintf("Rename function %s to %s", rename.OldName, rename.NewName),
				// Functions don't have RENAME support, use DROP+CREATE
				UpSQL:   generateDropFunctionSQL(currentFn) + "\n" + generateCreateFunctionSQL(targetFn),
				DownSQL: generateDropFunctionSQL(targetFn) + "\n" + generateCreateFunctionSQL(currentFn),
			},
			Current: currentFn,
			Target:  targetFn,
		}
		diffs = append(diffs, diff)
	}

	// Find functions to create or modify (sorted for deterministic order)
	targetNames := make([]string, 0, len(processedTarget))
	for name := range processedTarget {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	for _, name := range targetNames {
		targetFn := processedTarget[name]
		currentFn, exists := processedCurrent[name]

		if !exists {
			// Function needs to be created
			diff := &FunctionDiff{
				DiffBase: DiffBase{
					Type:        string(FunctionDiffCreate),
					Name:        name,
					Description: "Create function " + name,
					UpSQL:       generateCreateFunctionSQL(targetFn),
					DownSQL:     generateDropFunctionSQL(targetFn),
				},
				Target: targetFn,
			}
			diffs = append(diffs, diff)
		} else if !functionsEqual(currentFn, targetFn) {
			// Function exists but is different - needs replacement
			diff := &FunctionDiff{
				DiffBase: DiffBase{
					Type:        string(FunctionDiffReplace),
					Name:        name,
					Description: "Replace function " + name,
					UpSQL:       generateCreateFunctionSQL(targetFn),
					DownSQL:     generateCreateFunctionSQL(currentFn),
				},
				Current: currentFn,
				Target:  targetFn,
			}
			diffs = append(diffs, diff)
		}
	}

	// Find functions to drop (sorted for deterministic order)
	currentNames := make([]string, 0, len(processedCurrent))
	for name := range processedCurrent {
		if _, exists := processedTarget[name]; !exists {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentFn := processedCurrent[name]
		diff := &FunctionDiff{
			DiffBase: DiffBase{
				Type:        string(FunctionDiffDrop),
				Name:        name,
				Description: "Drop function " + name,
				UpSQL:       generateDropFunctionSQL(currentFn),
				DownSQL:     generateCreateFunctionSQL(currentFn),
			},
			Current: currentFn,
		}
		diffs = append(diffs, diff)
	}

	return diffs
}

// extractFunctionInfoAsMap extracts function information from parsed SQL statements as a map
func extractFunctionInfoAsMap(sql *parser.SQL) map[string]*FunctionInfo {
	functions := make(map[string]*FunctionInfo)

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

			functions[fn.Name] = fn
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
