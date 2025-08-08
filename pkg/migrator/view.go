package migrator

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
	var diffs []*ViewDiff

	// Sort view names for deterministic order
	var currentNames []string
	for name := range currentViews {
		if _, exists := targetViews[name]; exists {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentView := currentViews[name]
		targetView := targetViews[name]
		//nolint:nestif // Complex nested logic needed for view comparison and rename detection
		// Validate operation before proceeding
		if err := validateViewOperation(currentView, targetView); err != nil {
			return nil, err
		}

		// Check if it's a rename by comparing properties (excluding names)
		if isViewRename(currentView, targetViews) {
			// Find the renamed target - sort target names for deterministic order
			var targetNames []string
			for targetName := range targetViews {
				targetNames = append(targetNames, targetName)
			}
			sort.Strings(targetNames)

			for _, targetName := range targetNames {
				candidate := targetViews[targetName]
				if targetName != name && viewsHaveSameProperties(currentView, candidate) {
					diff := &ViewDiff{
						Type:           ViewDiffRename,
						ViewName:       name,
						NewViewName:    targetName,
						Description:    fmt.Sprintf("Rename %s %s to %s", getViewType(currentView), name, targetName),
						Current:        currentView,
						Target:         candidate,
						IsMaterialized: currentView.IsMaterialized,
					}
					diff.UpSQL = generateRenameViewSQL(currentView, candidate)
					diff.DownSQL = generateRenameViewSQL(candidate, currentView)
					diffs = append(diffs, diff)
					break
				}
			}
		} else if !viewsAreEqual(currentView, targetView) {
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
				Name:           stmt.CreateView.Name,
				Database:       getStringValue(stmt.CreateView.Database),
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
		current.Cluster != target.Cluster ||
		current.IsMaterialized != target.IsMaterialized ||
		current.OrReplace != target.OrReplace {
		return false
	}

	// Compare the full statements for deep equality
	// This includes comparing the SELECT clause and all other properties
	return viewStatementsAreEqual(current.Statement, target.Statement)
}

// viewsHaveSameProperties compares views ignoring the name (used for rename detection)
func viewsHaveSameProperties(view1, view2 *ViewInfo) bool {
	if view1.Database != view2.Database ||
		view1.Cluster != view2.Cluster ||
		view1.IsMaterialized != view2.IsMaterialized ||
		view1.OrReplace != view2.OrReplace {
		return false
	}

	// Compare statements ignoring names
	return viewStatementsHaveSameProperties(view1.Statement, view2.Statement)
}

// isViewRename determines if a view is being renamed by checking if its properties match another view
func isViewRename(currentView *ViewInfo, targetViews map[string]*ViewInfo) bool {
	for targetName, targetView := range targetViews {
		// Skip if it's the same name
		if getFullViewName(currentView) == targetName {
			continue
		}

		// Check if properties match (indicating a rename)
		if viewsHaveSameProperties(currentView, targetView) {
			return true
		}
	}
	return false
}

// viewStatementsAreEqual compares two CREATE VIEW statements for complete equality
func viewStatementsAreEqual(stmt1, stmt2 *parser.CreateViewStmt) bool {
	if stmt1.IfNotExists != stmt2.IfNotExists ||
		stmt1.Populate != stmt2.Populate ||
		getStringValue(stmt1.To) != getStringValue(stmt2.To) {
		return false
	}

	// Compare ENGINE clauses
	if !engineClausesAreEqual(stmt1.Engine, stmt2.Engine) {
		return false
	}

	// Compare SELECT clauses
	if !selectClausesAreEqual(stmt1.AsSelect, stmt2.AsSelect) {
		return false
	}

	return true
}

// viewStatementsHaveSameProperties compares statements ignoring names (for rename detection)
func viewStatementsHaveSameProperties(stmt1, stmt2 *parser.CreateViewStmt) bool {
	// For rename detection, we compare everything except the view name
	return viewStatementsAreEqual(stmt1, stmt2)
}

// engineClausesAreEqual compares two engine clauses
func engineClausesAreEqual(engine1, engine2 *parser.ViewEngine) bool {
	if engine1 == nil && engine2 == nil {
		return true
	}
	if engine1 == nil || engine2 == nil {
		return false
	}
	return strings.TrimSpace(buildEngineString(engine1)) == strings.TrimSpace(buildEngineString(engine2))
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

// selectClausesAreEqual compares two SELECT clauses
func selectClausesAreEqual(select1, select2 *parser.SelectStatement) bool {
	if select1 == nil && select2 == nil {
		return true
	}
	if select1 == nil || select2 == nil {
		return false
	}
	// For now, do a simple comparison by converting to string representation
	// A more sophisticated comparison would check individual fields
	return selectStatementToString(select1) == selectStatementToString(select2)
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

	if view.Statement.To != nil && *view.Statement.To != "" {
		sql += " TO " + *view.Statement.To
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
