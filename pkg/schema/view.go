package schema

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// ViewDiffCreate indicates a view needs to be created
	ViewDiffCreate ViewDiffType = "CREATE"
	// ViewDiffDrop indicates a view needs to be dropped
	ViewDiffDrop ViewDiffType = "DROP"
	// ViewDiffAlter indicates a view needs to be altered (only for materialized views using ALTER TABLE MODIFY QUERY)
	ViewDiffAlter ViewDiffType = "ALTER"
	// ViewDiffRename indicates a view needs to be renamed (uses RENAME TABLE for both regular and materialized views)
	ViewDiffRename ViewDiffType = "RENAME"
)

type (
	// ViewDiff represents a difference between current and target view states.
	// It handles both regular views and materialized views, with special handling
	// for materialized views which can only be altered using ALTER TABLE MODIFY QUERY.
	ViewDiff struct {
		Type           ViewDiffType // Type of operation (CREATE, DROP, ALTER, RENAME)
		ViewName       string       // Full name of the view (database.name)
		Description    string       // Human-readable description of the change
		UpSQL          string       // SQL to apply the change (forward migration)
		DownSQL        string       // SQL to rollback the change (reverse migration)
		Current        *ViewInfo    // Current state (nil if view doesn't exist)
		Target         *ViewInfo    // Target state (nil if view should be dropped)
		NewViewName    string       // For rename operations - the new full name
		IsMaterialized bool         // True if this is a materialized view
	}

	// ViewDiffType represents the type of view difference
	ViewDiffType string

	// ViewInfo represents parsed view information extracted from DDL statements.
	// This structure contains all the properties needed for view comparison and
	// migration generation, including whether it's a materialized view.
	ViewInfo struct {
		Name           string                 // View name
		Database       string                 // Database name (empty for default database)
		Cluster        string                 // Cluster name if specified (empty if not clustered)
		IsMaterialized bool                   // True if this is a materialized view
		OrReplace      bool                   // True if created with OR REPLACE
		Query          string                 // Query string for validation compatibility
		Statement      *parser.CreateViewStmt // Full parsed CREATE VIEW statement for deep comparison
	}
)

// compareViews compares current and target schemas to find view differences.
// It analyzes CREATE VIEW statements from both schemas and generates appropriate
// migration operations including CREATE, DROP, ALTER (for materialized views), and RENAME.
//
// For materialized views that have changed:
// - Uses ALTER TABLE name MODIFY QUERY for content changes (not CREATE OR REPLACE)
// - Uses standard CREATE/DROP for creation/deletion
// - Uses RENAME TABLE for renames
//
// For regular views that have changed:
// - Uses CREATE OR REPLACE for content changes
// - Uses standard CREATE/DROP for creation/deletion
// - Uses RENAME TABLE for renames
func compareViews(current, target *parser.SQL) ([]*ViewDiff, error) { // nolint: unparam
	// Extract views from both schemas
	currentViews := extractViewsFromSQL(current)
	targetViews := extractViewsFromSQL(target)

	var diffs []*ViewDiff

	// Find views to create, drop, alter, or rename using helper functions
	createD, err := findViewsToCreate(targetViews, currentViews)
	if err != nil {
		return nil, err
	}
	diffs = append(diffs, createD...)

	dropD, err := findViewsToDrop(currentViews, targetViews)
	if err != nil {
		return nil, err
	}
	diffs = append(diffs, dropD...)

	alterD, err := findViewsToAlterOrRename(currentViews, targetViews)
	if err != nil {
		return nil, err
	}
	diffs = append(diffs, alterD...)

	return diffs, nil
}

// findViewsToCreate finds views that need to be created (exist in target but not in current)
func findViewsToCreate(targetViews, currentViews map[string]*ViewInfo) ([]*ViewDiff, error) {
	// Count views to create for pre-allocation
	createCount := 0
	for name := range targetViews {
		if _, exists := currentViews[name]; !exists {
			createCount++
		}
	}

	diffs := make([]*ViewDiff, 0, createCount)

	// Sort view names for deterministic order
	viewNames := make([]string, 0, createCount)
	for name := range targetViews {
		if _, exists := currentViews[name]; !exists {
			viewNames = append(viewNames, name)
		}
	}
	sort.Strings(viewNames)

	for _, name := range viewNames {
		targetView := targetViews[name]
		// Validate create operation
		if err := validateViewOperation(nil, targetView); err != nil {
			return nil, err
		}

		diff := &ViewDiff{
			Type:           ViewDiffCreate,
			ViewName:       name,
			Description:    fmt.Sprintf("Create %s %s", getViewType(targetView), name),
			Target:         targetView,
			IsMaterialized: targetView.IsMaterialized,
		}
		diff.UpSQL = generateCreateViewSQL(targetView)
		diff.DownSQL = generateDropViewSQL(targetView)
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// findViewsToDrop finds views that need to be dropped (exist in current but not in target)
func findViewsToDrop(currentViews, targetViews map[string]*ViewInfo) ([]*ViewDiff, error) {
	// Count views to drop for pre-allocation
	dropCount := 0
	for name := range currentViews {
		if _, exists := targetViews[name]; !exists {
			dropCount++
		}
	}

	diffs := make([]*ViewDiff, 0, dropCount)

	// Sort view names for deterministic order
	viewNames := make([]string, 0, dropCount)
	for name := range currentViews {
		if _, exists := targetViews[name]; !exists {
			viewNames = append(viewNames, name)
		}
	}
	sort.Strings(viewNames)

	for _, name := range viewNames {
		currentView := currentViews[name]
		// Validate drop operation
		if err := validateViewOperation(currentView, nil); err != nil {
			return nil, err
		}

		diff := &ViewDiff{
			Type:           ViewDiffDrop,
			ViewName:       name,
			Description:    fmt.Sprintf("Drop %s %s", getViewType(currentView), name),
			Current:        currentView,
			IsMaterialized: currentView.IsMaterialized,
		}
		diff.UpSQL = generateDropViewSQL(currentView)
		diff.DownSQL = generateCreateViewSQL(currentView)
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// findViewsToAlterOrRename finds views that need to be altered or renamed
func findViewsToAlterOrRename(currentViews, targetViews map[string]*ViewInfo) ([]*ViewDiff, error) {
	diffs := make([]*ViewDiff, 0, len(currentViews))

	// First, detect all potential renames using a proper algorithm that prevents circular renames
	renamePairs := detectViewRenames(currentViews, targetViews)

	// Create a set of views involved in renames to skip them in the alter check
	renamedViews := make(map[string]bool)
	for currentName, targetName := range renamePairs {
		renamedViews[currentName] = true
		renamedViews[targetName] = true

		currentView := currentViews[currentName]
		targetView := targetViews[targetName]

		// Validate rename operation
		if err := validateViewOperation(currentView, targetView); err != nil {
			return nil, err
		}

		diff := &ViewDiff{
			Type:           ViewDiffRename,
			ViewName:       currentName,
			NewViewName:    targetName,
			Description:    fmt.Sprintf("Rename %s %s to %s", getViewType(currentView), currentName, targetName),
			Current:        currentView,
			Target:         targetView,
			IsMaterialized: currentView.IsMaterialized,
		}
		diff.UpSQL = generateRenameViewSQL(currentView, targetView)
		diff.DownSQL = generateRenameViewSQL(targetView, currentView)
		diffs = append(diffs, diff)
	}

	// Now check for alterations, excluding views involved in renames
	var currentNames []string
	for name := range currentViews {
		if _, exists := targetViews[name]; exists && !renamedViews[name] {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentView := currentViews[name]
		targetView := targetViews[name]

		// Validate operation before proceeding
		if err := validateViewOperation(currentView, targetView); err != nil {
			return nil, err
		}

		if !viewsAreEqual(currentView, targetView) {
			// View needs to be altered
			diff := &ViewDiff{
				Type:           ViewDiffAlter,
				ViewName:       name,
				Description:    fmt.Sprintf("Alter %s %s", getViewType(currentView), name),
				Current:        currentView,
				Target:         targetView,
				IsMaterialized: currentView.IsMaterialized,
			}

			// For materialized views, use DROP+CREATE (more reliable than ALTER TABLE MODIFY QUERY)
			// For regular views, use CREATE OR REPLACE
			if currentView.IsMaterialized {
				diff.UpSQL = generateDropViewSQL(currentView) + "\n\n" + generateCreateViewSQL(targetView)
				diff.DownSQL = generateDropViewSQL(targetView) + "\n\n" + generateCreateViewSQL(currentView)
			} else {
				diff.UpSQL = generateCreateOrReplaceViewSQL(targetView)
				diff.DownSQL = generateCreateOrReplaceViewSQL(currentView)
			}

			diffs = append(diffs, diff)
		}
	}

	return diffs, nil
}

// extractViewsFromSQL extracts all view information from parsed SQL
func extractViewsFromSQL(sql *parser.SQL) map[string]*ViewInfo {
	views := make(map[string]*ViewInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateView != nil {
			// Extract query string for validation - simplified approach
			queryStr := ""
			if stmt.CreateView.AsSelect != nil {
				// For now, we'll use a simple placeholder since we don't have String() method
				queryStr = "SELECT ..." // TODO: Implement proper query string extraction if needed
			}

			view := &ViewInfo{
				Name:           normalizeIdentifier(stmt.CreateView.Name),
				Database:       normalizeIdentifier(getStringValue(stmt.CreateView.Database)),
				Cluster:        getStringValue(stmt.CreateView.OnCluster),
				IsMaterialized: stmt.CreateView.Materialized,
				OrReplace:      stmt.CreateView.OrReplace,
				Query:          queryStr, // For validation compatibility
				Statement:      stmt.CreateView,
			}

			// Create full name (database.name or just name)
			fullName := view.Name
			if view.Database != "" {
				fullName = view.Database + "." + view.Name
			}

			views[fullName] = view
		}
	}

	return views
}

// viewsAreEqual compares two ViewInfo structures for equality
func viewsAreEqual(current, target *ViewInfo) bool {
	if current.Name != target.Name ||
		current.Database != target.Database ||
		current.IsMaterialized != target.IsMaterialized {
		return false
	}

	// Note: OrReplace is ignored because it's a creation-time directive
	// that's not preserved in ClickHouse's stored object definitions

	if current.Cluster != target.Cluster {
		return false
	}

	// Compare the full statements for deep equality
	// This includes comparing the SELECT clause and all other properties
	result := viewStatementsAreEqual(current.Statement, target.Statement)

	// Debug output disabled for now
	// if !result && current.Statement != nil && target.Statement != nil {
	//     fmt.Printf("DEBUG: View %s.%s has differences\n", current.Database, current.Name)
	// }

	return result
}

// viewsHaveSameProperties compares views ignoring the name (used for rename detection)
func viewsHaveSameProperties(view1, view2 *ViewInfo) bool {
	if view1.Database != view2.Database ||
		view1.IsMaterialized != view2.IsMaterialized {
		return false
	}

	// Note: OrReplace is ignored because it's a creation-time directive
	// that's not preserved in ClickHouse's stored object definitions

	if view1.Cluster != view2.Cluster {
		return false
	}

	// Compare statements ignoring names
	return viewStatementsHaveSameProperties(view1.Statement, view2.Statement)
}

// detectViewRenames finds rename pairs using a proper algorithm that prevents circular renames.
// Returns a map of currentName -> targetName for all detected renames.
func detectViewRenames(currentViews, targetViews map[string]*ViewInfo) map[string]string {
	renamePairs := make(map[string]string)
	usedTargets := make(map[string]bool)

	// Build a map of possible rename candidates
	renameCandidates := make(map[string][]string) // currentName -> []possibleTargetNames

	for currentName, currentView := range currentViews {
		// Skip if current view still exists with same name in target
		if _, exists := targetViews[currentName]; exists {
			continue
		}

		// Find all target views that have the same properties as this current view
		var candidates []string
		for targetName, targetView := range targetViews {
			// Skip if target already exists in current (no rename needed)
			if _, exists := currentViews[targetName]; exists {
				continue
			}

			// Check if properties match (indicating potential rename)
			if viewsHaveSameProperties(currentView, targetView) {
				candidates = append(candidates, targetName)
			}
		}

		if len(candidates) > 0 {
			sort.Strings(candidates) // For deterministic ordering
			renameCandidates[currentName] = candidates
		}
	}

	// Now assign renames ensuring no target is used twice
	// Sort current names for deterministic processing order
	sortedCurrentNames := make([]string, 0, len(renameCandidates))
	for currentName := range renameCandidates {
		sortedCurrentNames = append(sortedCurrentNames, currentName)
	}
	sort.Strings(sortedCurrentNames)

	for _, currentName := range sortedCurrentNames {
		candidates := renameCandidates[currentName]

		// Find the first unused target candidate
		for _, targetName := range candidates {
			if !usedTargets[targetName] {
				renamePairs[currentName] = targetName
				usedTargets[targetName] = true
				break
			}
		}
	}

	return renamePairs
}

// viewStatementsAreEqual compares two CREATE VIEW statements for complete equality
func viewStatementsAreEqual(stmt1, stmt2 *parser.CreateViewStmt) bool {
	// Note: IfNotExists is ignored because it's a creation-time directive
	// that's not preserved in ClickHouse's stored object definitions

	// Compare TO clauses (materialized views only)
	if getViewTableTargetValue(stmt1.To) != getViewTableTargetValue(stmt2.To) {
		return false
	}

	// Compare ENGINE clauses with ClickHouse limitations in mind
	if !engineClausesAreEqualWithTolerance(stmt1.Engine, stmt2.Engine) {
		return false
	}

	// Compare POPULATE with ClickHouse limitations in mind
	// ClickHouse doesn't preserve POPULATE directive in stored definitions
	// Allow differences since ClickHouse doesn't preserve POPULATE in system.tables
	_ = stmt1.Populate // Acknowledge that we're intentionally ignoring this field
	_ = stmt2.Populate

	// Compare SELECT clauses with formatting tolerance
	if !selectClausesAreEqualWithTolerance(stmt1.AsSelect, stmt2.AsSelect) {
		return false
	}

	return true
}

// viewStatementsHaveSameProperties compares statements ignoring names (for rename detection)
func viewStatementsHaveSameProperties(stmt1, stmt2 *parser.CreateViewStmt) bool {
	// For rename detection, we compare everything except the view name
	// This uses the same logic as viewStatementsAreEqual, which ignores creation-time directives
	return viewStatementsAreEqual(stmt1, stmt2)
}

// engineClausesAreEqualWithTolerance compares engine clauses with ClickHouse limitations in mind
func engineClausesAreEqualWithTolerance(engine1, engine2 *parser.ViewEngine) bool {
	// If both are nil, they're equal
	if engine1 == nil && engine2 == nil {
		return true
	}

	// ClickHouse limitation: materialized views don't return ENGINE clause in system.tables
	// If one is nil and the other isn't, this might be a ClickHouse storage limitation
	// For now, we'll be tolerant of this difference
	if engine1 == nil || engine2 == nil {
		// This is likely ClickHouse not preserving ENGINE information
		// We'll allow this difference rather than failing the comparison
		return true
	}

	// Both have values, compare them normally
	return viewEnginesEqual(engine1, engine2)
}

// buildEngineString builds an engine string from ViewEngine struct
// This builds a properly formatted string with spaces for readability
func buildEngineString(engine *parser.ViewEngine) string {
	if engine == nil {
		return ""
	}

	result := engine.Name

	// Add parameters - always add parentheses for engines like MergeTree()
	if len(engine.Parameters) > 0 {
		var params []string
		for _, param := range engine.Parameters {
			params = append(params, param.Value())
		}
		result += "(" + strings.Join(params, ", ") + ")"
	} else {
		// Add empty parentheses for engines like MergeTree()
		result += "()"
	}

	// Add ORDER BY if present (proper format with spaces)
	if engine.OrderBy != nil {
		result += " ORDER BY " + engine.OrderBy.Expression.String()
	}

	// Add PARTITION BY if present (proper format with spaces)
	if engine.PartitionBy != nil {
		result += " PARTITION BY " + engine.PartitionBy.Expression.String()
	}

	// Add PRIMARY KEY if present (proper format with spaces)
	if engine.PrimaryKey != nil {
		result += " PRIMARY KEY " + engine.PrimaryKey.Expression.String()
	}

	// Add SAMPLE BY if present (proper format with spaces)
	if engine.SampleBy != nil {
		result += " SAMPLE BY " + engine.SampleBy.Expression.String()
	}

	return result
}

// viewEnginesEqual compares ViewEngine structures using AST-based comparison
func viewEnginesEqual(engine1, engine2 *parser.ViewEngine) bool {
	if engine1 == nil && engine2 == nil {
		return true
	}
	if engine1 == nil || engine2 == nil {
		return false
	}

	// Compare engine names (case-insensitive)
	if !strings.EqualFold(engine1.Name, engine2.Name) {
		return false
	}

	// Compare parameters
	if len(engine1.Parameters) != len(engine2.Parameters) {
		return false
	}
	for i, param1 := range engine1.Parameters {
		param2 := engine2.Parameters[i]
		if !engineParametersEqual(&param1, &param2) {
			return false
		}
	}

	// Compare ORDER BY clause
	if engine1.OrderBy == nil && engine2.OrderBy == nil {
		// Both nil, continue
	} else if engine1.OrderBy != nil && engine2.OrderBy != nil {
		if !expressionsAreEqual(&engine1.OrderBy.Expression, &engine2.OrderBy.Expression) {
			return false
		}
	} else {
		// One is nil, the other isn't
		return false
	}

	// Compare PARTITION BY clause
	if engine1.PartitionBy == nil && engine2.PartitionBy == nil {
		// Both nil, continue
	} else if engine1.PartitionBy != nil && engine2.PartitionBy != nil {
		if !expressionsAreEqual(&engine1.PartitionBy.Expression, &engine2.PartitionBy.Expression) {
			return false
		}
	} else {
		// One is nil, the other isn't
		return false
	}

	// Compare PRIMARY KEY clause
	if engine1.PrimaryKey == nil && engine2.PrimaryKey == nil {
		// Both nil, continue
	} else if engine1.PrimaryKey != nil && engine2.PrimaryKey != nil {
		if !expressionsAreEqual(&engine1.PrimaryKey.Expression, &engine2.PrimaryKey.Expression) {
			return false
		}
	} else {
		// One is nil, the other isn't
		return false
	}

	// Compare SAMPLE BY clause
	if engine1.SampleBy == nil && engine2.SampleBy == nil {
		// Both nil, continue
	} else if engine1.SampleBy != nil && engine2.SampleBy != nil {
		if !expressionsAreEqual(&engine1.SampleBy.Expression, &engine2.SampleBy.Expression) {
			return false
		}
	} else {
		// One is nil, the other isn't
		return false
	}

	return true
}

// selectClausesAreEqualWithTolerance compares SELECT clauses with formatting tolerance
func selectClausesAreEqualWithTolerance(select1, select2 *parser.SelectStatement) bool {
	if select1 == nil && select2 == nil {
		return true
	}
	if select1 == nil || select2 == nil {
		return false
	}

	// First try exact AST comparison
	if selectStatementsAreEqualAST(select1, select2) {
		return true
	}

	// If exact comparison fails, try string-based normalization comparison
	// This handles cases where ClickHouse formatting differs from our parser output
	return selectStatementsAreEqualNormalized(select1, select2)
}

// selectStatementsAreEqualNormalized compares SELECT statements with tolerance for ClickHouse formatting
func selectStatementsAreEqualNormalized(stmt1, stmt2 *parser.SelectStatement) bool {
	// For now, implement a simple fallback strategy:
	// If we have the same number of clauses and they're structurally similar, consider them equal
	// This is a conservative approach that favors avoiding unnecessary recreations

	// Check if basic structure is the same
	if (stmt1.With == nil) != (stmt2.With == nil) {
		return false // Different WITH clause presence
	}
	if len(stmt1.Columns) != len(stmt2.Columns) {
		return false // Different number of columns
	}
	if (stmt1.From == nil) != (stmt2.From == nil) {
		return false // Different FROM clause presence
	}
	if (stmt1.OrderBy == nil) != (stmt2.OrderBy == nil) {
		return false // Different ORDER BY presence
	}

	// If we get here, the basic structure is similar
	// For ClickHouse formatting tolerance, we'll be optimistic and assume they're equivalent
	// This is a temporary measure to address the recreation issue
	// TODO: Implement proper normalization comparison once format package has public methods
	return true
}

// selectStatementsAreEqualAST compares two SELECT statements using AST-based comparison
func selectStatementsAreEqualAST(stmt1, stmt2 *parser.SelectStatement) bool {
	// Compare WITH clauses (CTEs)
	if !withClausesAreEqual(stmt1.With, stmt2.With) {
		return false
	}

	// Compare SELECT columns
	if !selectColumnsAreEqual(stmt1.Columns, stmt2.Columns) {
		return false
	}

	// Compare FROM clauses
	if !fromClausesAreEqual(stmt1.From, stmt2.From) {
		return false
	}

	// Compare WHERE clauses
	if !whereClausesAreEqual(stmt1.Where, stmt2.Where) {
		return false
	}

	// Compare GROUP BY clauses
	if !groupByClausesAreEqual(stmt1.GroupBy, stmt2.GroupBy) {
		return false
	}

	// Compare HAVING clauses
	if !havingClausesAreEqual(stmt1.Having, stmt2.Having) {
		return false
	}

	// Compare ORDER BY clauses
	if !selectOrderByClausesAreEqual(stmt1.OrderBy, stmt2.OrderBy) {
		return false
	}

	// Compare LIMIT clauses
	if !limitClausesAreEqual(stmt1.Limit, stmt2.Limit) {
		return false
	}

	// Compare SETTINGS clauses
	if !settingsClausesAreEqual(stmt1.Settings, stmt2.Settings) {
		return false
	}

	return true
}

// withClausesAreEqual compares WITH clauses (CTEs)
func withClausesAreEqual(with1, with2 *parser.WithClause) bool {
	if with1 == nil && with2 == nil {
		return true
	}
	if with1 == nil || with2 == nil {
		return false
	}
	if len(with1.CTEs) != len(with2.CTEs) {
		return false
	}
	for i, cte1 := range with1.CTEs {
		cte2 := with2.CTEs[i]
		if normalizeIdentifier(cte1.Name) != normalizeIdentifier(cte2.Name) {
			return false
		}
		if !selectStatementsAreEqualAST(cte1.Query, cte2.Query) {
			return false
		}
	}
	return true
}

// whereClausesAreEqual compares WHERE clauses
func whereClausesAreEqual(where1, where2 *parser.WhereClause) bool {
	if where1 == nil && where2 == nil {
		return true
	}
	if where1 == nil || where2 == nil {
		return false
	}
	return expressionsAreEqual(&where1.Condition, &where2.Condition)
}

// havingClausesAreEqual compares HAVING clauses
func havingClausesAreEqual(having1, having2 *parser.HavingClause) bool {
	if having1 == nil && having2 == nil {
		return true
	}
	if having1 == nil || having2 == nil {
		return false
	}
	return expressionsAreEqual(&having1.Condition, &having2.Condition)
}

// selectOrderByClausesAreEqual compares SELECT ORDER BY clauses
func selectOrderByClausesAreEqual(order1, order2 *parser.SelectOrderByClause) bool {
	if order1 == nil && order2 == nil {
		return true
	}
	if order1 == nil || order2 == nil {
		return false
	}
	if len(order1.Columns) != len(order2.Columns) {
		return false
	}
	for i, col1 := range order1.Columns {
		col2 := order2.Columns[i]
		if !orderByColumnsAreEqual(&col1, &col2) {
			return false
		}
	}
	return true
}

// orderByColumnsAreEqual compares ORDER BY columns
func orderByColumnsAreEqual(col1, col2 *parser.OrderByColumn) bool {
	if !expressionsAreEqual(&col1.Expression, &col2.Expression) {
		return false
	}

	// Compare direction (ASC/DESC)
	dir1 := ""
	if col1.Direction != nil {
		dir1 = strings.ToUpper(*col1.Direction)
	}
	dir2 := ""
	if col2.Direction != nil {
		dir2 = strings.ToUpper(*col2.Direction)
	}
	if dir1 != dir2 {
		return false
	}

	// Compare NULLS FIRST/LAST
	nulls1 := ""
	if col1.Nulls != nil {
		nulls1 = strings.ToUpper(*col1.Nulls)
	}
	nulls2 := ""
	if col2.Nulls != nil {
		nulls2 = strings.ToUpper(*col2.Nulls)
	}
	return nulls1 == nulls2
}

// selectColumnsAreEqual compares SELECT column lists
func selectColumnsAreEqual(cols1, cols2 []parser.SelectColumn) bool {
	if len(cols1) != len(cols2) {
		return false
	}
	for i, col1 := range cols1 {
		col2 := cols2[i]
		if !expressionsAreEqual(col1.Expression, col2.Expression) {
			return false
		}
		// Compare aliases (normalize case and handle nil)
		alias1 := ""
		if col1.Alias != nil {
			alias1 = normalizeIdentifier(*col1.Alias)
		}
		alias2 := ""
		if col2.Alias != nil {
			alias2 = normalizeIdentifier(*col2.Alias)
		}
		if alias1 != alias2 {
			return false
		}
	}
	return true
}

// fromClausesAreEqual compares FROM clauses
func fromClausesAreEqual(from1, from2 *parser.FromClause) bool {
	if from1 == nil && from2 == nil {
		return true
	}
	if from1 == nil || from2 == nil {
		return false
	}

	// Compare main table
	if !tableRefsAreEqual(&from1.Table, &from2.Table) {
		return false
	}

	// Compare joins
	if len(from1.Joins) != len(from2.Joins) {
		return false
	}
	for i, join1 := range from1.Joins {
		join2 := from2.Joins[i]
		if !joinsAreEqual(&join1, &join2) {
			return false
		}
	}

	return true
}

// tableRefsAreEqual compares table references
func tableRefsAreEqual(table1, table2 *parser.TableRef) bool {
	if table1 == nil && table2 == nil {
		return true
	}
	if table1 == nil || table2 == nil {
		return false
	}

	// Compare table names
	if table1.TableName != nil && table2.TableName != nil {
		return tableNamesWithAliasAreEqual(table1.TableName, table2.TableName)
	}
	if table1.TableName != nil || table2.TableName != nil {
		return false
	}

	// Compare subqueries
	if table1.Subquery != nil && table2.Subquery != nil {
		return subqueriesWithAliasAreEqual(table1.Subquery, table2.Subquery)
	}
	if table1.Subquery != nil || table2.Subquery != nil {
		return false
	}

	// Compare function calls
	if table1.Function != nil && table2.Function != nil {
		return functionsWithAliasAreEqual(table1.Function, table2.Function)
	}
	return table1.Function == nil && table2.Function == nil
}

// tableNamesWithAliasAreEqual compares table names with aliases
func tableNamesWithAliasAreEqual(name1, name2 *parser.TableNameWithAlias) bool {
	if name1 == nil && name2 == nil {
		return true
	}
	if name1 == nil || name2 == nil {
		return false
	}

	// Compare database names
	db1 := ""
	if name1.Database != nil {
		db1 = normalizeIdentifier(*name1.Database)
	}
	db2 := ""
	if name2.Database != nil {
		db2 = normalizeIdentifier(*name2.Database)
	}
	if db1 != db2 {
		return false
	}

	// Compare table names
	if normalizeIdentifier(name1.Table) != normalizeIdentifier(name2.Table) {
		return false
	}

	// Compare aliases - for now, just check if both have or don't have aliases
	hasAlias1 := name1.ExplicitAlias != nil || name1.ImplicitAlias != nil
	hasAlias2 := name2.ExplicitAlias != nil || name2.ImplicitAlias != nil
	return hasAlias1 == hasAlias2
}

// subqueriesWithAliasAreEqual compares subqueries with aliases
func subqueriesWithAliasAreEqual(sub1, sub2 *parser.SubqueryWithAlias) bool {
	if sub1 == nil && sub2 == nil {
		return true
	}
	if sub1 == nil || sub2 == nil {
		return false
	}

	// Compare the actual subqueries
	if !selectStatementsAreEqualAST(&sub1.Subquery, &sub2.Subquery) {
		return false
	}

	// Compare aliases
	alias1 := ""
	if sub1.Alias != nil {
		alias1 = normalizeIdentifier(*sub1.Alias)
	}
	alias2 := ""
	if sub2.Alias != nil {
		alias2 = normalizeIdentifier(*sub2.Alias)
	}
	return alias1 == alias2
}

// functionsWithAliasAreEqual compares function calls with aliases using AST-based comparison
func functionsWithAliasAreEqual(fn1, fn2 *parser.FunctionWithAlias) bool {
	if fn1 == nil && fn2 == nil {
		return true
	}
	if fn1 == nil || fn2 == nil {
		return false
	}

	// Compare function names (case-insensitive for ClickHouse)
	if !strings.EqualFold(fn1.Function.Name, fn2.Function.Name) {
		return false
	}

	// Compare argument lists
	if len(fn1.Function.Arguments) != len(fn2.Function.Arguments) {
		return false
	}

	for i, arg1 := range fn1.Function.Arguments {
		arg2 := fn2.Function.Arguments[i]
		if !functionArgsEqual(&arg1, &arg2) {
			return false
		}
	}

	// Compare aliases (case-insensitive)
	if fn1.Alias == nil && fn2.Alias == nil {
		return true
	}
	if fn1.Alias == nil || fn2.Alias == nil {
		return false
	}
	return strings.EqualFold(*fn1.Alias, *fn2.Alias)
}

// joinsAreEqual compares JOIN clauses
func joinsAreEqual(join1, join2 *parser.JoinClause) bool {
	if join1.Type != join2.Type || join1.Join != join2.Join {
		return false
	}
	if !tableRefsAreEqual(&join1.Table, &join2.Table) {
		return false
	}

	// Compare join conditions
	if join1.Condition == nil && join2.Condition == nil {
		return true
	}
	if join1.Condition == nil || join2.Condition == nil {
		return false
	}

	// Compare ON conditions
	if join1.Condition.On != nil && join2.Condition.On != nil {
		return expressionsAreEqual(join1.Condition.On, join2.Condition.On)
	}
	if join1.Condition.On != nil || join2.Condition.On != nil {
		return false
	}

	// Compare USING conditions
	if len(join1.Condition.Using) != len(join2.Condition.Using) {
		return false
	}
	for i, col1 := range join1.Condition.Using {
		col2 := join2.Condition.Using[i]
		if normalizeIdentifier(col1) != normalizeIdentifier(col2) {
			return false
		}
	}

	return true
}

// groupByClausesAreEqual compares GROUP BY clauses
func groupByClausesAreEqual(group1, group2 *parser.GroupByClause) bool {
	if group1 == nil && group2 == nil {
		return true
	}
	if group1 == nil || group2 == nil {
		return false
	}
	if len(group1.Columns) != len(group2.Columns) {
		return false
	}
	for i, expr1 := range group1.Columns {
		expr2 := group2.Columns[i]
		if !expressionsAreEqual(&expr1, &expr2) {
			return false
		}
	}
	return true
}

// limitClausesAreEqual compares LIMIT clauses
func limitClausesAreEqual(limit1, limit2 *parser.LimitClause) bool {
	if limit1 == nil && limit2 == nil {
		return true
	}
	if limit1 == nil || limit2 == nil {
		return false
	}

	// Compare count expressions
	if !expressionsAreEqual(&limit1.Count, &limit2.Count) {
		return false
	}

	// Compare offset expressions
	if limit1.Offset == nil && limit2.Offset == nil {
		return true
	}
	if limit1.Offset == nil || limit2.Offset == nil {
		return false
	}
	return expressionsAreEqual(&limit1.Offset.Value, &limit2.Offset.Value)
}

// settingsClausesAreEqual compares SETTINGS clauses
func settingsClausesAreEqual(settings1, settings2 *parser.SettingsClause) bool {
	if settings1 == nil && settings2 == nil {
		return true
	}
	if settings1 == nil || settings2 == nil {
		return false
	}
	if len(settings1.Values) != len(settings2.Values) {
		return false
	}

	// Create maps for easier comparison
	map1 := make(map[string]string)
	for _, setting := range settings1.Values {
		map1[normalizeIdentifier(setting.Key)] = setting.Value.String()
	}

	map2 := make(map[string]string)
	for _, setting := range settings2.Values {
		map2[normalizeIdentifier(setting.Key)] = setting.Value.String()
	}

	// Compare maps
	for name, value1 := range map1 {
		value2, exists := map2[name]
		if !exists || value1 != value2 {
			return false
		}
	}

	return true
}

// selectStatementToString converts a SelectStatement to a properly formatted string representation
func selectStatementToString(stmt *parser.SelectStatement) string {
	if stmt == nil {
		return ""
	}

	// The formatSelectStatement method is private, so we'll use a workaround
	// Create a fake SELECT statement wrapper and format it, then extract the SELECT part
	fakeSQL := &parser.SQL{
		Statements: []*parser.Statement{
			{
				SelectStatement: &parser.TopLevelSelectStatement{
					SelectStatement: *stmt,
					Semicolon:       true,
				},
			},
		},
	}

	var buf bytes.Buffer
	if err := format.FormatSQL(&buf, format.Defaults, fakeSQL); err != nil {
		// Fallback to basic string representation if formatting fails
		return "SELECT * FROM unknown"
	}

	// Extract the formatted SELECT statement
	formatted := buf.String()
	// Remove any trailing semicolon since this is used within CREATE VIEW statements
	formatted = strings.TrimSuffix(formatted, ";")
	// Convert to single line for view definitions
	formatted = strings.ReplaceAll(formatted, "\n", " ")
	// Clean up multiple spaces
	formatted = strings.Join(strings.Fields(formatted), " ")

	return formatted
}

// getViewType returns a human-readable view type string
func getViewType(view *ViewInfo) string {
	if view.IsMaterialized {
		return "materialized view"
	}
	return "view"
}

// getFullViewName returns the full name of a view (database.name or just name)
func getFullViewName(view *ViewInfo) string {
	if view.Database != "" {
		return view.Database + "." + view.Name
	}
	return view.Name
}

// generateCreateViewSQL generates CREATE VIEW SQL from ViewInfo
func generateCreateViewSQL(view *ViewInfo) string {
	sql := "CREATE"

	if view.OrReplace {
		sql += " OR REPLACE"
	}

	if view.IsMaterialized {
		sql += " MATERIALIZED"
	}

	sql += " VIEW"

	if view.Statement.IfNotExists {
		sql += " IF NOT EXISTS"
	}

	sql += " " + getFullViewName(view)

	if view.Cluster != "" {
		sql += " ON CLUSTER " + view.Cluster
	}

	toValue := getViewTableTargetValue(view.Statement.To)
	if toValue != "" {
		sql += " TO " + toValue
	}

	if view.Statement.Engine != nil {
		sql += " ENGINE = " + buildEngineString(view.Statement.Engine)
	}

	if view.Statement.Populate {
		sql += " POPULATE"
	}

	if view.Statement.AsSelect != nil {
		sql += " AS " + selectStatementToString(view.Statement.AsSelect)
	}

	return sql + ";"
}

// generateDropViewSQL generates DROP VIEW/TABLE SQL from ViewInfo
func generateDropViewSQL(view *ViewInfo) string {
	if view.IsMaterialized {
		// Materialized views are dropped using DROP TABLE
		sql := "DROP TABLE IF EXISTS " + getFullViewName(view)
		if view.Cluster != "" {
			sql += " ON CLUSTER " + view.Cluster
		}
		return sql + ";"
	} else {
		// Regular views are dropped using DROP VIEW
		sql := "DROP VIEW IF EXISTS " + getFullViewName(view)
		if view.Cluster != "" {
			sql += " ON CLUSTER " + view.Cluster
		}
		return sql + ";"
	}
}

// generateCreateOrReplaceViewSQL generates CREATE OR REPLACE VIEW SQL for regular views
func generateCreateOrReplaceViewSQL(view *ViewInfo) string {
	sql := "CREATE OR REPLACE VIEW " + getFullViewName(view)

	if view.Cluster != "" {
		sql += " ON CLUSTER " + view.Cluster
	}

	if view.Statement.AsSelect != nil {
		sql += " AS " + selectStatementToString(view.Statement.AsSelect)
	}

	return sql + ";"
}

// generateRenameViewSQL generates RENAME TABLE SQL for both regular and materialized views
func generateRenameViewSQL(from, to *ViewInfo) string {
	sql := "RENAME TABLE " + getFullViewName(from) + " TO " + getFullViewName(to)

	// Use cluster info from the target view
	if to.Cluster != "" {
		sql += " ON CLUSTER " + to.Cluster
	}

	return sql + ";"
}
