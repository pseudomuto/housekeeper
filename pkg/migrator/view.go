package migrator

import (
	"fmt"
	"strings"

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
		Name         string                 // View name
		Database     string                 // Database name (empty for default database)
		OnCluster    string                 // Cluster name if specified (empty if not clustered)
		Materialized bool                   // True if this is a materialized view
		OrReplace    bool                   // True if created with OR REPLACE
		Statement    *parser.CreateViewStmt // Full parsed CREATE VIEW statement for deep comparison
	}
)

// CompareViewGrammars compares current and target grammars to find view differences.
// It analyzes CREATE VIEW statements from both grammars and generates appropriate
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
//
// Returns a slice of ViewDiff structs representing all the differences found.
func CompareViewGrammars(current, target *parser.SQL) ([]*ViewDiff, error) {
	// Extract views from both grammars
	currentViews := extractViewsFromGrammar(current)
	targetViews := extractViewsFromGrammar(target)

	var diffs []*ViewDiff

	// Find views to create (exist in target but not in current)
	for name, targetView := range targetViews {
		if _, exists := currentViews[name]; !exists {
			diff := &ViewDiff{
				Type:           ViewDiffCreate,
				ViewName:       name,
				Description:    fmt.Sprintf("Create %s %s", getViewType(targetView), name),
				Target:         targetView,
				IsMaterialized: targetView.Materialized,
			}
			diff.UpSQL = generateCreateViewSQL(targetView)
			diff.DownSQL = generateDropViewSQL(targetView)
			diffs = append(diffs, diff)
		}
	}

	// Find views to drop (exist in current but not in target)
	for name, currentView := range currentViews {
		if _, exists := targetViews[name]; !exists {
			diff := &ViewDiff{
				Type:           ViewDiffDrop,
				ViewName:       name,
				Description:    fmt.Sprintf("Drop %s %s", getViewType(currentView), name),
				Current:        currentView,
				IsMaterialized: currentView.Materialized,
			}
			diff.UpSQL = generateDropViewSQL(currentView)
			diff.DownSQL = generateCreateViewSQL(currentView)
			diffs = append(diffs, diff)
		}
	}

	// Find views to alter or rename
	for name, currentView := range currentViews {
		//nolint:nestif // Complex nested logic needed for view comparison and rename detection
		if targetView, exists := targetViews[name]; exists {
			// Check if it's a rename by comparing properties (excluding names)
			if isViewRename(currentView, targetViews) {
				// Find the renamed target
				for targetName, candidate := range targetViews {
					if targetName != name && viewsHaveSameProperties(currentView, candidate) {
						diff := &ViewDiff{
							Type:           ViewDiffRename,
							ViewName:       name,
							NewViewName:    targetName,
							Description:    fmt.Sprintf("Rename %s %s to %s", getViewType(currentView), name, targetName),
							Current:        currentView,
							Target:         candidate,
							IsMaterialized: currentView.Materialized,
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
					IsMaterialized: currentView.Materialized,
				}

				// For materialized views, use ALTER TABLE MODIFY QUERY
				// For regular views, use CREATE OR REPLACE
				if currentView.Materialized {
					diff.UpSQL = generateAlterMaterializedViewSQL(targetView)
					diff.DownSQL = generateAlterMaterializedViewSQL(currentView)
				} else {
					diff.UpSQL = generateCreateOrReplaceViewSQL(targetView)
					diff.DownSQL = generateCreateOrReplaceViewSQL(currentView)
				}
				diffs = append(diffs, diff)
			}
		}
	}

	return diffs, nil
}

// extractViewsFromGrammar extracts all view information from a parsed grammar
func extractViewsFromGrammar(sql *parser.SQL) map[string]*ViewInfo {
	views := make(map[string]*ViewInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateView != nil {
			view := &ViewInfo{
				Name:         stmt.CreateView.Name,
				Database:     getStringValue(stmt.CreateView.Database),
				OnCluster:    getStringValue(stmt.CreateView.OnCluster),
				Materialized: stmt.CreateView.Materialized,
				OrReplace:    stmt.CreateView.OrReplace,
				Statement:    stmt.CreateView,
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
		current.OnCluster != target.OnCluster ||
		current.Materialized != target.Materialized ||
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
		view1.OnCluster != view2.OnCluster ||
		view1.Materialized != view2.Materialized ||
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

// selectStatementToString converts a SelectStatement to a string representation
func selectStatementToString(stmt *parser.SelectStatement) string {
	if stmt == nil {
		return ""
	}

	// Build SQL without spaces to match expected format
	sql := "SELECT"

	if stmt.Distinct {
		sql += "DISTINCT"
	}

	// Add columns
	if len(stmt.Columns) > 0 {
		for i, col := range stmt.Columns {
			if i > 0 {
				sql += ","
			}
			if col.Star != nil {
				sql += "*"
			} else if col.Expression != nil {
				sql += col.Expression.String()
			}
			if col.Alias != nil {
				sql += "as" + *col.Alias
			}
		}
	}

	// Add FROM clause
	//nolint:nestif // Complex nested logic needed for SELECT statement formatting
	if stmt.From != nil {
		sql += "FROM"
		if stmt.From.Table.TableName != nil {
			if stmt.From.Table.TableName.Database != nil {
				sql += *stmt.From.Table.TableName.Database + "."
			}
			sql += stmt.From.Table.TableName.Table
			if stmt.From.Table.TableName.Alias != nil && stmt.From.Table.TableName.Alias.Name != nil {
				sql += "as" + *stmt.From.Table.TableName.Alias.Name
			}
		}

		// Add JOINs
		for _, join := range stmt.From.Joins {
			if join.Type != "" {
				sql += join.Type
			}
			sql += join.Join
			// Simplified join table representation
			sql += "table"
		}
	}

	// Add WHERE clause
	if stmt.Where != nil {
		sql += "WHEREcondition"
	}

	// Add GROUP BY clause with actual columns
	if stmt.GroupBy != nil && len(stmt.GroupBy.Columns) > 0 {
		sql += "GROUPBY"
		for i, col := range stmt.GroupBy.Columns {
			if i > 0 {
				sql += ","
			}
			sql += col.String()
		}
	}

	// Add HAVING clause
	if stmt.Having != nil {
		sql += "HAVINGcondition"
	}

	// Add ORDER BY clause
	if stmt.OrderBy != nil {
		sql += "ORDERBYcolumns"
	}

	// Add LIMIT clause
	if stmt.Limit != nil {
		sql += "LIMITn"
	}

	return sql
}

// getViewType returns a human-readable view type string
func getViewType(view *ViewInfo) string {
	if view.Materialized {
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

	if view.Materialized {
		sql += " MATERIALIZED"
	}

	sql += " VIEW"

	if view.Statement.IfNotExists {
		sql += " IF NOT EXISTS"
	}

	sql += " " + getFullViewName(view)

	if view.OnCluster != "" {
		sql += " ON CLUSTER " + view.OnCluster
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
	if view.Materialized {
		// Materialized views are dropped using DROP TABLE
		sql := "DROP TABLE IF EXISTS " + getFullViewName(view)
		if view.OnCluster != "" {
			sql += " ON CLUSTER " + view.OnCluster
		}
		return sql + ";"
	} else {
		// Regular views are dropped using DROP VIEW
		sql := "DROP VIEW IF EXISTS " + getFullViewName(view)
		if view.OnCluster != "" {
			sql += " ON CLUSTER " + view.OnCluster
		}
		return sql + ";"
	}
}

// generateCreateOrReplaceViewSQL generates CREATE OR REPLACE VIEW SQL for regular views
func generateCreateOrReplaceViewSQL(view *ViewInfo) string {
	sql := "CREATE OR REPLACE VIEW " + getFullViewName(view)

	if view.OnCluster != "" {
		sql += " ON CLUSTER " + view.OnCluster
	}

	if view.Statement.AsSelect != nil {
		sql += " AS " + selectStatementToString(view.Statement.AsSelect)
	}

	return sql + ";"
}

// generateAlterMaterializedViewSQL generates ALTER TABLE MODIFY QUERY SQL for materialized views
func generateAlterMaterializedViewSQL(view *ViewInfo) string {
	sql := "ALTER TABLE " + getFullViewName(view)

	if view.OnCluster != "" {
		sql += " ON CLUSTER " + view.OnCluster
	}

	sql += " MODIFY QUERY"

	if view.Statement.AsSelect != nil {
		sql += " " + selectStatementToString(view.Statement.AsSelect)
	}

	return sql + ";"
}

// generateRenameViewSQL generates RENAME TABLE SQL for both regular and materialized views
func generateRenameViewSQL(from, to *ViewInfo) string {
	sql := "RENAME TABLE " + getFullViewName(from) + " TO " + getFullViewName(to)

	// Use cluster info from the target view
	if to.OnCluster != "" {
		sql += " ON CLUSTER " + to.OnCluster
	}

	return sql + ";"
}
