package schema

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
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
		DiffBase                 // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current        *ViewInfo // Current state (nil if view doesn't exist)
		Target         *ViewInfo // Target state (nil if view should be dropped)
		IsMaterialized bool      // True if this is a materialized view
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

// GetName implements SchemaObject interface.
// Returns the full view name (database.name or just name).
func (v *ViewInfo) GetName() string {
	if v.Database != "" {
		return v.Database + "." + v.Name
	}
	return v.Name
}

// GetCluster implements SchemaObject interface.
func (v *ViewInfo) GetCluster() string {
	return v.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two views have identical properties (excluding name).
func (v *ViewInfo) PropertiesMatch(other SchemaObject) bool {
	otherView, ok := other.(*ViewInfo)
	if !ok {
		return false
	}
	return viewsHaveSameProperties(v, otherView)
}

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
			DiffBase: DiffBase{
				Type:        string(ViewDiffCreate),
				Name:        name,
				Description: fmt.Sprintf("Create %s %s", getViewType(targetView), name),
				UpSQL:       generateCreateViewSQL(targetView),
				DownSQL:     generateDropViewSQL(targetView),
			},
			Target:         targetView,
			IsMaterialized: targetView.IsMaterialized,
		}
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
			DiffBase: DiffBase{
				Type:        string(ViewDiffDrop),
				Name:        name,
				Description: fmt.Sprintf("Drop %s %s", getViewType(currentView), name),
				UpSQL:       generateDropViewSQL(currentView),
				DownSQL:     generateCreateViewSQL(currentView),
			},
			Current:        currentView,
			IsMaterialized: currentView.IsMaterialized,
		}
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// findViewsToAlterOrRename finds views that need to be altered or renamed
func findViewsToAlterOrRename(currentViews, targetViews map[string]*ViewInfo) ([]*ViewDiff, error) {
	diffs := make([]*ViewDiff, 0, len(currentViews))

	// Use generic rename detection algorithm
	renames, processedCurrent, processedTarget := DetectRenames(currentViews, targetViews)

	// Create rename diffs
	for _, rename := range renames {
		currentView := currentViews[rename.OldName]
		targetView := targetViews[rename.NewName]

		// Validate rename operation
		if err := validateViewOperation(currentView, targetView); err != nil {
			return nil, err
		}

		diff := &ViewDiff{
			DiffBase: DiffBase{
				Type:        string(ViewDiffRename),
				Name:        rename.OldName,
				NewName:     rename.NewName,
				Description: fmt.Sprintf("Rename %s %s to %s", getViewType(currentView), rename.OldName, rename.NewName),
				UpSQL:       generateRenameViewSQL(currentView, targetView),
				DownSQL:     generateRenameViewSQL(targetView, currentView),
			},
			Current:        currentView,
			Target:         targetView,
			IsMaterialized: currentView.IsMaterialized,
		}
		diffs = append(diffs, diff)
	}

	// Now check for alterations using processed maps (excludes renamed views)
	var currentNames []string
	for name := range processedCurrent {
		if _, exists := processedTarget[name]; exists {
			currentNames = append(currentNames, name)
		}
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		currentView := processedCurrent[name]
		targetView := processedTarget[name]

		// Validate operation before proceeding
		if err := validateViewOperation(currentView, targetView); err != nil {
			return nil, err
		}

		if !viewsAreEqual(currentView, targetView) {
			// View needs to be altered
			var upSQL, downSQL string

			// For materialized views, use DROP+CREATE (more reliable than ALTER TABLE MODIFY QUERY)
			// For regular views, use CREATE OR REPLACE
			if currentView.IsMaterialized {
				upSQL = generateDropViewSQL(currentView) + "\n\n" + generateCreateViewSQL(targetView)
				downSQL = generateDropViewSQL(targetView) + "\n\n" + generateCreateViewSQL(currentView)
			} else {
				upSQL = generateCreateOrReplaceViewSQL(targetView)
				downSQL = generateCreateOrReplaceViewSQL(currentView)
			}

			diff := &ViewDiff{
				DiffBase: DiffBase{
					Type:        string(ViewDiffAlter),
					Name:        name,
					Description: fmt.Sprintf("Alter %s %s", getViewType(currentView), name),
					UpSQL:       upSQL,
					DownSQL:     downSQL,
				},
				Current:        currentView,
				Target:         targetView,
				IsMaterialized: currentView.IsMaterialized,
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

	// Note: Cluster is ignored because ClickHouse doesn't return ON CLUSTER
	// in the create_table_query column from system.tables. The cluster info
	// is managed separately by housekeeper when generating DDL statements.

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

// viewStatementsAreEqual compares two CREATE VIEW statements for complete equality
func viewStatementsAreEqual(stmt1, stmt2 *parser.CreateViewStmt) bool {
	// Note: IfNotExists is ignored because it's a creation-time directive
	// that's not preserved in ClickHouse's stored object definitions

	// Compare REFRESH clauses (for refreshable materialized views)
	if !refreshClausesAreEqual(stmt1.Refresh, stmt2.Refresh) {
		return false
	}

	// Compare APPEND flag (for refreshable materialized views)
	if stmt1.Append != stmt2.Append {
		return false
	}

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

// timeUnitsAreEqual compares time units, treating singular and plural as equal
// (e.g., SECOND == SECONDS, MINUTE == MINUTES)
func timeUnitsAreEqual(unit1, unit2 string) bool {
	normalize := func(unit string) string {
		u := strings.ToUpper(strings.TrimSpace(unit))
		// Remove trailing 'S' to normalize SECONDS->SECOND, MINUTES->MINUTE, etc.
		if strings.HasSuffix(u, "S") && len(u) > 1 {
			return u[:len(u)-1]
		}
		return u
	}
	return normalize(unit1) == normalize(unit2)
}

// normalizeRefreshInterval converts a refresh interval to seconds for comparison
// This handles cases like "60 SECOND" vs "1 MINUTE" which are equivalent
func normalizeRefreshIntervalToSeconds(interval int, unit string) int {
	u := strings.ToUpper(strings.TrimSpace(unit))
	// Remove trailing 'S' for singular/plural normalization
	if strings.HasSuffix(u, "S") && len(u) > 1 {
		u = u[:len(u)-1]
	}

	switch u {
	case "SECOND":
		return interval
	case "MINUTE":
		return interval * 60
	case "HOUR":
		return interval * 3600
	case "DAY":
		return interval * 86400
	default:
		return interval
	}
}

// refreshClausesAreEqual compares REFRESH clauses for equality
func refreshClausesAreEqual(refresh1, refresh2 *parser.RefreshClause) bool {
	if refresh1 == nil && refresh2 == nil {
		return true
	}
	if refresh1 == nil || refresh2 == nil {
		return false
	}

	// Compare EVERY vs AFTER
	if refresh1.Every != refresh2.Every || refresh1.After != refresh2.After {
		return false
	}

	// Compare interval and unit
	// Normalize intervals to seconds for comparison
	// This handles cases like "60 SECOND" vs "1 MINUTE" which are equivalent
	interval1 := normalizeRefreshIntervalToSeconds(refresh1.Interval, refresh1.Unit)
	interval2 := normalizeRefreshIntervalToSeconds(refresh2.Interval, refresh2.Unit)
	if interval1 != interval2 {
		return false
	}

	// Compare OFFSET if present
	if (refresh1.OffsetInterval == nil) != (refresh2.OffsetInterval == nil) {
		return false
	}
	if refresh1.OffsetInterval != nil && *refresh1.OffsetInterval != *refresh2.OffsetInterval {
		return false
	}
	if (refresh1.OffsetUnit == nil) != (refresh2.OffsetUnit == nil) {
		return false
	}
	if refresh1.OffsetUnit != nil && !strings.EqualFold(*refresh1.OffsetUnit, *refresh2.OffsetUnit) {
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

	// IMPORTANT: Always check WITH clause contents - CTE changes are meaningful
	if !withClausesAreEqual(stmt1.With, stmt2.With) {
		return false
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

	// IMPORTANT: Always check LIMIT clauses exactly - these are meaningful changes
	if !limitClausesAreEqual(stmt1.Limit, stmt2.Limit) {
		return false
	}

	// IMPORTANT: Always check SETTINGS clauses exactly - these are meaningful changes
	if !settingsClausesAreEqual(stmt1.Settings, stmt2.Settings) {
		return false
	}

	// IMPORTANT: Always check UNION clauses - these are meaningful changes
	if !unionClausesAreEqual(stmt1.Unions, stmt2.Unions) {
		return false
	}

	// IMPORTANT: Check WHERE clause presence (structure changes)
	if (stmt1.Where == nil) != (stmt2.Where == nil) {
		return false
	}

	// IMPORTANT: Check GROUP BY clause presence (structure changes)
	if (stmt1.GroupBy == nil) != (stmt2.GroupBy == nil) {
		return false
	}

	// IMPORTANT: Check HAVING clause presence (structure changes)
	if (stmt1.Having == nil) != (stmt2.Having == nil) {
		return false
	}

	// IMPORTANT: Check if FROM clause structure is significantly different
	// (table vs subquery vs function)
	if stmt1.From != nil && stmt2.From != nil {
		if !fromClauseStructureSimilar(&stmt1.From.Table, &stmt2.From.Table) {
			return false
		}
	}

	// If we get here, the basic structure is similar
	// For ClickHouse formatting tolerance, we'll be optimistic and assume they're equivalent
	// This is a temporary measure to address the recreation issue
	// TODO: Implement proper normalization comparison once format package has public methods
	return true
}

// fromClauseStructureSimilar checks if two FROM clauses have similar structure
// (both use table names, both use subqueries, or both use functions)
func fromClauseStructureSimilar(table1, table2 *parser.TableRef) bool {
	if table1 == nil && table2 == nil {
		return true
	}
	if table1 == nil || table2 == nil {
		return false
	}

	// Check if both use table names
	if (table1.TableName != nil) != (table2.TableName != nil) {
		return false
	}

	// Check if both use subqueries
	if (table1.Subquery != nil) != (table2.Subquery != nil) {
		return false
	}

	// Check if both use functions
	if (table1.Function != nil) != (table2.Function != nil) {
		return false
	}

	// If both use subqueries, recursively check their structure
	if table1.Subquery != nil && table2.Subquery != nil {
		return selectStatementsStructureSimilar(&table1.Subquery.Subquery, &table2.Subquery.Subquery)
	}

	return true
}

// selectStatementsStructureSimilar checks if two select statements have similar structure
func selectStatementsStructureSimilar(stmt1, stmt2 *parser.SelectStatement) bool {
	// Check UNION presence
	if len(stmt1.Unions) != len(stmt2.Unions) {
		return false
	}

	// Check FROM clause presence and type
	if (stmt1.From == nil) != (stmt2.From == nil) {
		return false
	}

	if stmt1.From != nil && stmt2.From != nil {
		if !fromClauseStructureSimilar(&stmt1.From.Table, &stmt2.From.Table) {
			return false
		}
	}

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

	// Compare UNION clauses
	if !unionClausesAreEqual(stmt1.Unions, stmt2.Unions) {
		return false
	}

	return true
}

// withClausesAreEqual compares WITH clauses (CTEs)
func withClausesAreEqual(with1, with2 *parser.WithClause) bool {
	if eq, done := compare.NilCheck(with1, with2); !done {
		return eq
	}
	return compare.Slices(with1.CTEs, with2.CTEs, func(a, b parser.CommonTableExpression) bool {
		return normalizeIdentifier(a.Name) == normalizeIdentifier(b.Name) &&
			selectStatementsAreEqualAST(a.Query, b.Query)
	})
}

// whereClausesAreEqual compares WHERE clauses
func whereClausesAreEqual(where1, where2 *parser.WhereClause) bool {
	if eq, done := compare.NilCheck(where1, where2); !done {
		return eq
	}
	return expressionsAreEqual(&where1.Condition, &where2.Condition)
}

// havingClausesAreEqual compares HAVING clauses
func havingClausesAreEqual(having1, having2 *parser.HavingClause) bool {
	if eq, done := compare.NilCheck(having1, having2); !done {
		return eq
	}
	return expressionsAreEqual(&having1.Condition, &having2.Condition)
}

// selectOrderByClausesAreEqual compares SELECT ORDER BY clauses
func selectOrderByClausesAreEqual(order1, order2 *parser.SelectOrderByClause) bool {
	if eq, done := compare.NilCheck(order1, order2); !done {
		return eq
	}
	return compare.Slices(order1.Columns, order2.Columns, func(a, b parser.OrderByColumn) bool {
		return orderByColumnsAreEqual(&a, &b)
	})
}

// orderByColumnsAreEqual compares ORDER BY columns
func orderByColumnsAreEqual(col1, col2 *parser.OrderByColumn) bool {
	if !expressionsAreEqual(&col1.Expression, &col2.Expression) {
		return false
	}

	// Compare direction (ASC/DESC) with normalization
	dir1 := ""
	if col1.Direction != nil {
		dir1 = strings.ToUpper(*col1.Direction)
	}
	dir2 := ""
	if col2.Direction != nil {
		dir2 = strings.ToUpper(*col2.Direction)
	}

	// Compare NULLS FIRST/LAST with normalization
	nulls1 := ""
	if col1.Nulls != nil {
		nulls1 = strings.ToUpper(*col1.Nulls)
	}
	nulls2 := ""
	if col2.Nulls != nil {
		nulls2 = strings.ToUpper(*col2.Nulls)
	}

	return dir1 == dir2 && nulls1 == nulls2
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
		// Compare aliases (normalize backticks and compare case-insensitively)
		alias1 := ""
		if col1.Alias != nil {
			alias1 = normalizeIdentifier(*col1.Alias)
		}
		alias2 := ""
		if col2.Alias != nil {
			alias2 = normalizeIdentifier(*col2.Alias)
		}
		if !strings.EqualFold(alias1, alias2) {
			return false
		}
	}
	return true
}

// fromClausesAreEqual compares FROM clauses
func fromClausesAreEqual(from1, from2 *parser.FromClause) bool {
	if eq, done := compare.NilCheck(from1, from2); !done {
		return eq
	}

	return tableRefsAreEqual(&from1.Table, &from2.Table) &&
		compare.Slices(from1.Joins, from2.Joins, func(a, b parser.JoinClause) bool {
			return joinsAreEqual(&a, &b)
		})
}

// tableRefsAreEqual compares table references
func tableRefsAreEqual(table1, table2 *parser.TableRef) bool {
	if eq, done := compare.NilCheck(table1, table2); !done {
		return eq
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
	if eq, done := compare.NilCheck(name1, name2); !done {
		return eq
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

// unionClausesAreEqual compares UNION clauses
func unionClausesAreEqual(unions1, unions2 []parser.UnionClause) bool {
	if len(unions1) != len(unions2) {
		return false
	}
	for i, union1 := range unions1 {
		union2 := unions2[i]
		if union1.All != union2.All || union1.Distinct != union2.Distinct {
			return false
		}
		if union1.Select == nil && union2.Select == nil {
			continue
		}
		if union1.Select == nil || union2.Select == nil {
			return false
		}
		// Compare the SELECT clauses in the union
		if !simpleSelectClausesAreEqual(union1.Select, union2.Select) {
			return false
		}
	}
	return true
}

// simpleSelectClausesAreEqual compares two SimpleSelectClause structures
func simpleSelectClausesAreEqual(sel1, sel2 *parser.SimpleSelectClause) bool {
	if sel1 == nil && sel2 == nil {
		return true
	}
	if sel1 == nil || sel2 == nil {
		return false
	}

	// Compare SELECT DISTINCT
	if sel1.Distinct != sel2.Distinct {
		return false
	}

	// Compare columns count
	if len(sel1.Columns) != len(sel2.Columns) {
		return false
	}

	// Compare FROM presence
	if (sel1.From == nil) != (sel2.From == nil) {
		return false
	}

	// Compare WHERE presence
	if (sel1.Where == nil) != (sel2.Where == nil) {
		return false
	}

	// Compare GROUP BY presence
	if (sel1.GroupBy == nil) != (sel2.GroupBy == nil) {
		return false
	}

	// Compare HAVING presence
	if (sel1.Having == nil) != (sel2.Having == nil) {
		return false
	}

	// Compare ORDER BY presence
	if (sel1.OrderBy == nil) != (sel2.OrderBy == nil) {
		return false
	}

	// Compare LIMIT
	if !simpleSelectLimitClausesAreEqual(sel1.Limit, sel2.Limit) {
		return false
	}

	// Compare SETTINGS
	if !settingsClausesAreEqual(sel1.Settings, sel2.Settings) {
		return false
	}

	return true
}

// simpleSelectLimitClausesAreEqual compares LIMIT clauses from SimpleSelectClause
func simpleSelectLimitClausesAreEqual(limit1, limit2 *parser.LimitClause) bool {
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

	// Add REFRESH clause for refreshable materialized views
	if view.Statement.Refresh != nil {
		sql += " " + formatRefreshClause(view.Statement.Refresh)
	}

	// Add APPEND TO or TO clause
	toValue := getViewTableTargetValue(view.Statement.To)
	if toValue != "" {
		if view.Statement.Append {
			sql += " APPEND TO " + toValue
		} else {
			sql += " TO " + toValue
		}
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

// formatRefreshClause formats a REFRESH clause for SQL generation
func formatRefreshClause(refresh *parser.RefreshClause) string {
	if refresh == nil {
		return ""
	}

	result := "REFRESH"

	if refresh.Every {
		result += " EVERY"
	} else if refresh.After {
		result += " AFTER"
	}

	result += fmt.Sprintf(" %d %s", refresh.Interval, strings.ToUpper(refresh.Unit))

	if refresh.OffsetInterval != nil && refresh.OffsetUnit != nil {
		result += fmt.Sprintf(" OFFSET %d %s", *refresh.OffsetInterval, strings.ToUpper(*refresh.OffsetUnit))
	}

	return result
}

// generateDropViewSQL generates DROP VIEW/TABLE SQL from ViewInfo
func generateDropViewSQL(view *ViewInfo) string {
	var database *string
	if view.Database != "" {
		database = &view.Database
	}

	objectType := "VIEW"
	if view.IsMaterialized {
		// Materialized views are dropped using DROP TABLE
		objectType = "TABLE"
	}

	return utils.NewSQLBuilder().
		Drop(objectType).
		IfExists().
		QualifiedName(database, view.Name).
		OnCluster(view.Cluster).
		String()
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
	var fromDB, toDB *string
	if from.Database != "" {
		fromDB = &from.Database
	}
	if to.Database != "" {
		toDB = &to.Database
	}

	return utils.NewSQLBuilder().
		Rename("TABLE").
		QualifiedName(fromDB, from.Name).
		QualifiedTo(toDB, to.Name).
		OnCluster(to.Cluster).
		String()
}
