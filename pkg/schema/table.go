package schema

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
)

const (
	// TableDiffCreate indicates a table needs to be created
	TableDiffCreate TableDiffType = "CREATE"
	// TableDiffDrop indicates a table needs to be dropped
	TableDiffDrop TableDiffType = "DROP"
	// TableDiffAlter indicates a table needs to be altered
	TableDiffAlter TableDiffType = "ALTER"
	// TableDiffRename indicates a table needs to be renamed
	TableDiffRename TableDiffType = "RENAME"
)

type (
	// TableDiff represents a difference between current and target table states.
	// It contains all information needed to generate migration SQL statements for
	// table operations including CREATE, ALTER, DROP, and RENAME.
	TableDiff struct {
		DiffBase                   // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current       *TableInfo   // Current state (nil if table doesn't exist)
		Target        *TableInfo   // Target state (nil if table should be dropped)
		ColumnChanges []ColumnDiff // For ALTER operations - specific column changes
	}

	// TableDiffType represents the type of table difference
	TableDiffType string

	// TableInfo represents parsed table information extracted from DDL statements.
	// This structure contains all the properties needed for table comparison and
	// migration generation, including columns, engine, and other table options.
	TableInfo struct {
		Name          string                 // Table name (without database prefix)
		Database      string                 // Database name (empty if not specified)
		Engine        *parser.TableEngine    // Engine AST
		Cluster       string                 // Cluster name for distributed tables
		Comment       string                 // Table comment
		OrderBy       *parser.Expression     // ORDER BY expression AST
		PartitionBy   *parser.Expression     // PARTITION BY expression AST
		PrimaryKey    *parser.Expression     // PRIMARY KEY expression AST
		SampleBy      *parser.Expression     // SAMPLE BY expression AST
		TTL           *parser.TableTTLClause // Table-level TTL clause AST (includes DELETE keyword)
		Settings      map[string]string      // Table settings
		Columns       []ColumnInfo           // Column definitions
		OrReplace     bool                   // Whether CREATE OR REPLACE was used
		IfNotExists   bool                   // Whether IF NOT EXISTS was used
		AsSourceTable *string                // If this table uses AS, the source table name (qualified)
		AsDependents  map[string]bool        // Tables that use AS to reference this table
	}

	// ColumnInfo represents a single column definition
	ColumnInfo struct {
		Name        string              // Column name
		DataType    *parser.DataType    // Data type AST
		DefaultType string              // Default type: DEFAULT, MATERIALIZED, EPHEMERAL, ALIAS
		Default     *parser.Expression  // Default expression AST
		Codec       *parser.CodecClause // Codec AST
		TTL         *parser.TTLClause   // TTL AST
		Comment     string              // Column comment
	}

	// ColumnDiff represents a difference in column definitions
	ColumnDiff struct {
		Type        ColumnDiffType // Type of column operation
		ColumnName  string         // Name of the column
		Current     *ColumnInfo    // Current column definition (nil for ADD)
		Target      *ColumnInfo    // Target column definition (nil for DROP)
		Description string         // Human-readable description
	}

	// ColumnDiffType represents the type of column difference
	ColumnDiffType string
)

const (
	// ColumnDiffAdd indicates a column needs to be added
	ColumnDiffAdd ColumnDiffType = "ADD"
	// ColumnDiffDrop indicates a column needs to be dropped
	ColumnDiffDrop ColumnDiffType = "DROP"
	// ColumnDiffModify indicates a column needs to be modified
	ColumnDiffModify ColumnDiffType = "MODIFY"
	// ColumnDiffRename indicates a column needs to be renamed
	ColumnDiffRename ColumnDiffType = "RENAME"
)

// GetName implements SchemaObject interface.
// Returns the full table name (database.name or just name).
func (t *TableInfo) GetName() string {
	if t.Database != "" {
		return t.Database + "." + t.Name
	}
	return t.Name
}

// GetCluster implements SchemaObject interface.
func (t *TableInfo) GetCluster() string {
	return t.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two tables have identical properties (excluding name).
func (t *TableInfo) PropertiesMatch(other SchemaObject) bool {
	otherTable, ok := other.(*TableInfo)
	if !ok {
		return false
	}
	return tablesEqualIgnoringName(t, otherTable)
}

// Equal compares two TableInfo instances for equality using AST comparison
func (t *TableInfo) Equal(other *TableInfo) bool {
	if eq, done := compare.NilCheck(t, other); !done {
		return eq
	}

	// Compare basic fields
	if t.Name != other.Name || t.Database != other.Database || t.Cluster != other.Cluster ||
		!strings.EqualFold(t.Comment, other.Comment) {
		return false
	}

	// Compare AST fields
	if !enginesEqual(t.Engine, other.Engine) ||
		!equalAST(t.OrderBy, other.OrderBy) ||
		!equalAST(t.PartitionBy, other.PartitionBy) ||
		!equalAST(t.PrimaryKey, other.PrimaryKey) ||
		!equalAST(t.SampleBy, other.SampleBy) ||
		!ttlClausesEqual(t.TTL, other.TTL) {
		return false
	}

	// Compare settings and columns
	return compare.Maps(t.Settings, other.Settings) &&
		compare.Slices(t.Columns, other.Columns, func(a, b ColumnInfo) bool {
			return a.Equal(b)
		})
}

// ttlClausesEqual compares two TTL clauses for semantic equality.
// This handles:
// 1. The equivalence between INTERVAL X UNIT and toIntervalUnit(X) syntax
// 2. The DELETE keyword being the default action (TTL expr DELETE == TTL expr)
func ttlClausesEqual(a, b *parser.TableTTLClause) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// Compare expressions with interval normalization
	if !expressionsEqualWithIntervalNormalization(&a.Expression, &b.Expression) {
		return false
	}
	// Compare Delete clauses with special handling for default DELETE action
	// DELETE without WHERE is the default action, so:
	// - nil Delete == Delete{Where: nil}
	// - Both are considered equal
	return ttlDeleteClausesEqual(a.Delete, b.Delete)
}

// ttlDeleteClausesEqual compares TTL Delete clauses, treating DELETE without WHERE as the default
func ttlDeleteClausesEqual(a, b *parser.TTLDelete) bool {
	// DELETE without WHERE is the default action
	// So nil and &TTLDelete{Where: nil} are equivalent
	aIsDefault := a == nil || a.Where == nil
	bIsDefault := b == nil || b.Where == nil

	if aIsDefault && bIsDefault {
		return true
	}
	if aIsDefault != bIsDefault {
		return false
	}
	// Both have WHERE clauses - compare them
	return a.Where.Equal(b.Where)
}

// expressionsEqualWithIntervalNormalization compares two expressions for equality,
// treating INTERVAL X UNIT and toIntervalUnit(X) as equivalent.
func expressionsEqualWithIntervalNormalization(a, b *parser.Expression) bool {
	if a == nil && b == nil {
		return true
	}
	if a == nil || b == nil {
		return false
	}
	// First try standard equality
	if a.Equal(b) {
		return true
	}
	// If not equal, try normalizing interval expressions
	// Normalize both to string representation and compare
	aNorm := normalizeIntervalExpr(a.String())
	bNorm := normalizeIntervalExpr(b.String())
	return aNorm == bNorm
}

// normalizeIntervalExpr converts INTERVAL X UNIT to toIntervalUnit(X) format
// for consistent comparison. It handles the common interval units.
func normalizeIntervalExpr(expr string) string {
	// Replace INTERVAL X UNIT patterns with toIntervalUnit(X)
	// Handle: INTERVAL 7 DAY -> toIntervalDay(7)
	intervalUnits := map[string]string{
		"SECOND":  "Second",
		"MINUTE":  "Minute",
		"HOUR":    "Hour",
		"DAY":     "Day",
		"WEEK":    "Week",
		"MONTH":   "Month",
		"QUARTER": "Quarter",
		"YEAR":    "Year",
	}

	result := expr
	for unit, funcSuffix := range intervalUnits {
		// Match "INTERVAL <number> <UNIT>" pattern (case insensitive)
		// Replace with toInterval<Unit>(<number>)
		pattern := fmt.Sprintf("(?i)INTERVAL\\s+(\\d+)\\s+%s", unit)
		re := regexp.MustCompile(pattern)
		result = re.ReplaceAllString(result, fmt.Sprintf("toInterval%s($1)", funcSuffix))
	}
	return result
}

// Equal compares two ColumnInfo instances for equality using AST comparison
func (c ColumnInfo) Equal(other ColumnInfo) bool {
	if c.Name != other.Name || c.DefaultType != other.DefaultType ||
		!strings.EqualFold(c.Comment, other.Comment) {
		return false
	}

	return equalAST(c.DataType, other.DataType) &&
		equalAST(c.Default, other.Default) &&
		equalAST(c.Codec, other.Codec) &&
		equalAST(c.TTL, other.TTL)
}

// enginesEqual compares two table engines with special handling for parameter normalization.
// This handles cases where:
// 1. ReplicatedMergeTree() is auto-expanded by ClickHouse to include paths
// 2. Kafka vs Kafka() are semantically equivalent (empty params vs no params)
func enginesEqual(target, current *parser.TableEngine) bool {
	// Use standard equalAST for nil checks
	if target == nil || current == nil {
		return equalAST(target, current)
	}

	// Engine names must match
	if target.Name != current.Name {
		return false
	}

	// Special handling for ReplicatedMergeTree when target has no parameters
	// ClickHouse auto-expands ReplicatedMergeTree() to include paths like
	// ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')
	if target.Name == "ReplicatedMergeTree" && len(target.Parameters) == 0 {
		return true // Names match and target has no params, consider equal
	}

	// For engines where empty params are equivalent to no params (Kafka, etc.)
	// Treat nil and empty slice as equivalent
	targetEmpty := len(target.Parameters) == 0
	currentEmpty := len(current.Parameters) == 0
	if targetEmpty && currentEmpty {
		return true
	}

	// For all other cases, use standard AST comparison
	return equalAST(target, current)
}

// compareTables compares current and target parsed schemas to find table differences.
// It identifies tables that need to be created, altered, dropped, or renamed.
//
// The function performs comprehensive table comparison including:
// - Table structure (engine, settings, comments)
// - Column definitions and modifications
// - Rename detection based on content similarity
// - Proper ordering for migration generation
func compareTables(current, target *parser.SQL) ([]*TableDiff, error) {
	currentTables, err := extractTablesFromSQL(current)
	if err != nil {
		return nil, err
	}

	targetTables, err := extractTablesFromSQL(target)
	if err != nil {
		return nil, err
	}

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*TableDiff, 0, len(currentTables)+len(targetTables))

	// Pre-identify tables that will be handled via propagation to avoid duplicate diffs.
	// When a source table has column changes, its AS dependents should be handled via propagation
	// (which uses DROP+CREATE for view-like engines like Distributed). We skip these tables in
	// the main loop to prevent generating both an ALTER and a DROP+CREATE for the same table.
	// This must be done before the main loop because dependent tables may be processed
	// before their source tables in alphabetical order.
	propagatedTables := identifyPropagatedTables(currentTables, targetTables)

	// Find tables to create or modify (exist in target but not in current) - sorted for deterministic order
	for _, tableName := range SortedKeys(targetTables) {
		targetTable := targetTables[tableName]
		currentTable, exists := currentTables[tableName]

		// Skip tables that will be handled via propagation from a source table
		if propagatedTables[tableName] {
			continue
		}

		diff, err := createTableDiff(tableName, currentTable, targetTable, currentTables, targetTables, exists)
		if err != nil {
			return nil, err
		}
		if diff != nil {
			diffs = append(diffs, diff)
			// Remove from currentTables if this was a rename to avoid processing as a drop
			if diff.Type == string(TableDiffRename) {
				delete(currentTables, diff.Name)
			}

			// Propagate column changes to AS dependents (only for ALTER operations)
			if diff.Type == string(TableDiffAlter) && targetTable.AsDependents != nil && len(diff.ColumnChanges) > 0 {
				propagatedDiffs := propagateColumnChangesToDependents(
					diff,
					targetTable.AsDependents,
					currentTables,
					targetTables,
				)
				diffs = append(diffs, propagatedDiffs...)
			}
		}
	}

	// Find tables to drop (exist in current but not in target) - sorted for deterministic order
	for _, tableName := range FilterExcluding(currentTables, targetTables) {
		currentTable := currentTables[tableName]
		diff := &TableDiff{
			DiffBase: DiffBase{
				Type:        string(TableDiffDrop),
				Name:        tableName,
				Description: "Drop table " + tableName,
				UpSQL:       generateDropTableSQL(currentTable),
				DownSQL:     generateCreateTableSQL(currentTable),
			},
			Current: currentTable,
		}
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// identifyPropagatedTables pre-identifies tables that should be handled via propagation
// rather than the main comparison loop. This is necessary because dependent tables may be
// processed before their source tables in alphabetical order.
//
// A table should be handled via propagation if:
// 1. It uses AS to reference another table (is an AS dependent)
// 2. The source table has column changes (will generate an ALTER with column changes)
// 3. Both the source and dependent exist in current and target
func identifyPropagatedTables(currentTables, targetTables map[string]*TableInfo) map[string]bool {
	propagatedTables := make(map[string]bool)

	for tableName, targetTable := range targetTables {
		// Skip tables without AS dependents
		if targetTable.AsDependents == nil || len(targetTable.AsDependents) == 0 {
			continue
		}

		// Check if this table exists in current (required for column comparison)
		currentTable, existsInCurrent := currentTables[tableName]
		if !existsInCurrent {
			continue
		}

		// Check if this table has column changes
		// Conditionally flatten based on whether current has Nested columns
		comparisonTargetTable := MaybeFlattenNestedColumns(currentTable, targetTable)
		columnChanges := compareColumns(currentTable.Columns, comparisonTargetTable.Columns)
		if len(columnChanges) == 0 {
			continue
		}

		// This table has column changes - mark all its AS dependents for propagation
		for dependentName := range targetTable.AsDependents {
			// Only mark if the dependent exists in both current and target
			if _, existsInTarget := targetTables[dependentName]; existsInTarget {
				if _, existsInCurrent := currentTables[dependentName]; existsInCurrent {
					propagatedTables[dependentName] = true
				}
			}
		}
	}

	return propagatedTables
}

// propagateColumnChangesToDependents creates ALTER or DROP+CREATE diffs for tables
// that use AS to reference a source table when that source table has column changes
func propagateColumnChangesToDependents(
	sourceDiff *TableDiff,
	dependents map[string]bool,
	currentTables, targetTables map[string]*TableInfo,
) []*TableDiff {
	// Only process if there are column changes to propagate
	if len(sourceDiff.ColumnChanges) == 0 {
		return nil
	}

	propagatedDiffs := make([]*TableDiff, 0, len(dependents))

	for dependentName := range dependents {
		targetDep := targetTables[dependentName]
		currentDep := currentTables[dependentName]

		// Skip if dependent doesn't exist in both current and target
		if currentDep == nil || targetDep == nil {
			continue
		}

		// Create a propagated diff with column changes
		propDiff := &TableDiff{
			DiffBase: DiffBase{
				Type:        string(TableDiffAlter),
				Name:        dependentName,
				Description: fmt.Sprintf("Propagated column changes from %s (AS dependency)", sourceDiff.Name),
			},
			Current:       currentDep,
			Target:        targetDep,
			ColumnChanges: sourceDiff.ColumnChanges, // Same column changes
		}

		// Generate SQL based on engine type
		if isViewLikeEngine(targetDep.Engine) {
			// For Distributed, Memory, etc.: DROP + CREATE is safe and necessary
			// Note: generateDropTableSQL already includes semicolon from SQLBuilder
			propDiff.UpSQL = fmt.Sprintf("-- Recreate to match schema changes from %s\n", sourceDiff.Name) +
				generateDropTableSQL(currentDep) + "\n" +
				generateCreateTableSQL(targetDep)
			propDiff.DownSQL = generateDropTableSQL(targetDep) + "\n" +
				generateCreateTableSQL(currentDep)
		} else {
			// For MergeTree, etc.: Use ALTER to preserve data
			// Note: For propagated changes from AS dependencies, we only propagate column changes (no TTL changes)
			propDiff.UpSQL = fmt.Sprintf("-- Propagated from %s (AS dependency)\n", sourceDiff.Name) +
				generateAlterTableSQL(currentDep, targetDep, sourceDiff.ColumnChanges)
			propDiff.DownSQL = generateAlterTableSQL(targetDep, currentDep, reverseColumnChanges(sourceDiff.ColumnChanges))
		}

		propagatedDiffs = append(propagatedDiffs, propDiff)
	}

	return propagatedDiffs
}

// isViewLikeEngine determines if an engine is view-like (doesn't store data locally)
// and can be safely recreated with DROP+CREATE without data loss
func isViewLikeEngine(engine *parser.TableEngine) bool {
	if engine == nil {
		return false
	}

	viewLikeEngines := map[string]bool{
		"Distributed": true,
		"Merge":       true,
		"Buffer":      true,
		"Dictionary":  true,
		"View":        true,
		"LiveView":    true,
		"Memory":      true, // Temporary data, safe to recreate
	}

	return viewLikeEngines[engine.Name]
}

// requiresDropCreate determines if changing from current engine to target engine
// requires DROP+CREATE rather than ALTER TABLE operations
func requiresDropCreate(current, target *parser.TableEngine) bool {
	if current == nil || target == nil {
		return false
	}

	// If engine names are different, this should be caught by validation
	// This function only handles parameter changes within the same engine type
	if current.Name != target.Name {
		return false
	}

	// ReplicatedMergeTree parameter changes require DROP+CREATE
	// because you cannot ALTER the replication path or replica name
	if current.Name == "ReplicatedMergeTree" {
		// Use our special enginesEqual logic that handles the no-parameters case
		// If engines are not equal (respecting the no-parameters special case), DROP+CREATE is required
		return !enginesEqual(target, current)
	}

	// Other engines with parameter changes might be added here in the future
	// For now, most other parameter changes can be handled with ALTER TABLE

	return false
}

// shouldCopyClause determines if a specific clause type should be copied from source to target table
// based on the target table's engine restrictions
func shouldCopyClause(targetEngine *parser.TableEngine, clauseType string) bool {
	if targetEngine == nil {
		return true // If no engine specified, allow all clauses
	}

	restrictedClauses, hasRestrictions := engineClauseRestrictions[targetEngine.Name]
	if !hasRestrictions {
		return true // Engine has no clause restrictions
	}

	// Check if this clause type is restricted for this engine
	return !slices.Contains(restrictedClauses, clauseType) // Clause is allowed
}

// resolveASReferences resolves AS table references to copy schema from source tables
// It also tracks dependency relationships for migration propagation
func resolveASReferences(tables map[string]*TableInfo) error {
	for tableName, table := range tables {
		if table.AsSourceTable == nil {
			continue
		}

		// Skip table function markers - these don't reference actual tables in the schema
		if strings.HasPrefix(*table.AsSourceTable, consts.TableFunctionPrefix) {
			continue
		}

		// Find the source table
		sourceTable, exists := tables[*table.AsSourceTable]
		if !exists {
			return fmt.Errorf("table %s references non-existent table %s via AS clause",
				tableName, *table.AsSourceTable)
		}

		// Copy schema from source table (but keep explicitly specified properties)
		// Only copy columns if no columns were explicitly defined
		if len(table.Columns) == 0 {
			table.Columns = make([]ColumnInfo, len(sourceTable.Columns))
			copy(table.Columns, sourceTable.Columns)
		}

		// Copy clauses only if not explicitly specified AND supported by target engine
		if table.OrderBy == nil && sourceTable.OrderBy != nil && shouldCopyClause(table.Engine, "ORDER BY") {
			orderByCopy := *sourceTable.OrderBy
			table.OrderBy = &orderByCopy
		}
		if table.PartitionBy == nil && sourceTable.PartitionBy != nil && shouldCopyClause(table.Engine, "PARTITION BY") {
			partitionByCopy := *sourceTable.PartitionBy
			table.PartitionBy = &partitionByCopy
		}
		if table.PrimaryKey == nil && sourceTable.PrimaryKey != nil && shouldCopyClause(table.Engine, "PRIMARY KEY") {
			primaryKeyCopy := *sourceTable.PrimaryKey
			table.PrimaryKey = &primaryKeyCopy
		}
		if table.SampleBy == nil && sourceTable.SampleBy != nil && shouldCopyClause(table.Engine, "SAMPLE BY") {
			sampleByCopy := *sourceTable.SampleBy
			table.SampleBy = &sampleByCopy
		}

		// Track dependency in source table
		if sourceTable.AsDependents == nil {
			sourceTable.AsDependents = make(map[string]bool)
		}
		sourceTable.AsDependents[tableName] = true
	}

	return nil
}

// extractTablesFromSQL extracts table information from parsed SQL statements
//
//nolint:gocognit,funlen // Complex function needed for comprehensive table parsing
func extractTablesFromSQL(sql *parser.SQL) (map[string]*TableInfo, error) {
	tables := make(map[string]*TableInfo)

	for _, stmt := range sql.Statements {
		//nolint:nestif // Complex nested logic needed for comprehensive table extraction
		if stmt.CreateTable != nil {
			table := stmt.CreateTable
			tableName := normalizeIdentifier(table.Name)
			if table.Database != nil {
				tableName = normalizeIdentifier(*table.Database) + "." + normalizeIdentifier(table.Name)
			}

			tableInfo := &TableInfo{
				Name:        normalizeIdentifier(table.Name),
				OrReplace:   table.OrReplace,
				IfNotExists: table.IfNotExists,
			}

			// Track AS source table if present
			if table.AsTable != nil {
				// Handle both table functions and table references
				if table.AsTable.Function != nil {
					// For table functions, store the function name as a marker
					// This helps identify that the table was created from a table function
					functionMarker := consts.TableFunctionPrefix + table.AsTable.Function.Name
					tableInfo.AsSourceTable = &functionMarker
				} else if table.AsTable.TableRef != nil {
					asTableName := normalizeIdentifier(table.AsTable.TableRef.Table)
					if table.AsTable.TableRef.Database != nil {
						asTableName = normalizeIdentifier(*table.AsTable.TableRef.Database) + "." + asTableName
					}
					tableInfo.AsSourceTable = &asTableName
				}
			}

			if table.Database != nil {
				tableInfo.Database = normalizeIdentifier(*table.Database)
			}
			if table.OnCluster != nil {
				tableInfo.Cluster = *table.OnCluster
			}
			if table.Engine != nil {
				tableInfo.Engine = table.Engine
			}
			if table.Comment != nil {
				tableInfo.Comment = removeQuotes(*table.Comment)
			}
			if orderBy := table.GetOrderBy(); orderBy != nil {
				tableInfo.OrderBy = &orderBy.Expression
			}
			if partitionBy := table.GetPartitionBy(); partitionBy != nil {
				tableInfo.PartitionBy = &partitionBy.Expression
			}
			if primaryKey := table.GetPrimaryKey(); primaryKey != nil {
				tableInfo.PrimaryKey = &primaryKey.Expression
			}
			if sampleBy := table.GetSampleBy(); sampleBy != nil {
				tableInfo.SampleBy = &sampleBy.Expression
			}
			if ttl := table.GetTTL(); ttl != nil {
				tableInfo.TTL = ttl
			}
			if settings := table.GetSettings(); settings != nil {
				settingMap := make(map[string]string)
				for _, setting := range settings.Settings {
					settingMap[setting.Name] = setting.Value
				}
				tableInfo.Settings = settingMap
			}

			// Process columns from table elements
			var columns []ColumnInfo
			for _, element := range table.Elements {
				if element.Column == nil {
					continue // Skip indexes and constraints for now
				}
				col := element.Column
				columnInfo := ColumnInfo{
					Name:     normalizeIdentifier(col.Name),
					DataType: col.DataType,
				}
				if defaultClause := col.GetDefault(); defaultClause != nil {
					columnInfo.DefaultType = defaultClause.Type
					columnInfo.Default = &defaultClause.Expression
				}
				if codecClause := col.GetCodec(); codecClause != nil {
					columnInfo.Codec = codecClause
				}
				if ttlClause := col.GetTTL(); ttlClause != nil {
					columnInfo.TTL = ttlClause
				}
				if comment := col.GetComment(); comment != nil {
					columnInfo.Comment = removeQuotes(*comment)
				}
				columns = append(columns, columnInfo)
			}
			tableInfo.Columns = columns

			tables[tableName] = tableInfo
		}
	}

	// Resolve AS references after all tables are extracted
	if err := resolveASReferences(tables); err != nil {
		return tables, err
	}

	return tables, nil
}

// findRenamedTable attempts to find if a target table is actually a renamed version of a current table
func findRenamedTable(targetTable *TableInfo, currentTables, targetTables map[string]*TableInfo) string {
	// Look for a table in current that has the same structure but different name
	for currentName, currentTable := range currentTables {
		// Skip if this current table has a corresponding target table (not renamed)
		targetName := currentName
		if currentTable.Database != "" {
			targetName = currentTable.Database + "." + currentTable.Name
		}
		if _, exists := targetTables[targetName]; exists {
			continue
		}

		// Compare table properties (excluding name and database)
		// Conditionally flatten target table based on whether current has Nested columns
		comparisonTargetTable := MaybeFlattenNestedColumns(currentTable, targetTable)
		if tablesEqualIgnoringName(currentTable, comparisonTargetTable) {
			return currentName
		}
	}
	return ""
}

// tablesEqual compares two tables for equality
func tablesEqual(a, b *TableInfo) bool {
	return a.Equal(b)
}

// tablesEqualIgnoringName compares two tables for equality ignoring name and database
func tablesEqualIgnoringName(a, b *TableInfo) bool {
	// Create copies with normalized names to use Equal()
	aCopy := *a
	bCopy := *b
	aCopy.Name = ""
	aCopy.Database = ""
	bCopy.Name = ""
	bCopy.Database = ""
	return aCopy.Equal(&bCopy)
}

// compareColumns compares column definitions and returns differences
func compareColumns(current, target []ColumnInfo) []ColumnDiff {
	var diffs []ColumnDiff

	// Create maps for easier lookup
	currentCols := make(map[string]ColumnInfo)
	targetCols := make(map[string]ColumnInfo)
	// Also create position maps for rename detection
	currentPositions := make(map[string]int)
	targetPositions := make(map[string]int)

	for i, col := range current {
		currentCols[col.Name] = col
		currentPositions[col.Name] = i
	}
	for i, col := range target {
		targetCols[col.Name] = col
		targetPositions[col.Name] = i
	}

	// Find columns to add or modify
	for _, targetCol := range target {
		if currentCol, exists := currentCols[targetCol.Name]; exists {
			// Column exists - check for changes using Equal() method
			if !currentCol.Equal(targetCol) {
				// Check if this is an incompatible AggregateFunction type change
				// ClickHouse doesn't support MODIFY for AggregateFunction type changes
				// We need to use DROP + ADD instead
				if isIncompatibleAggregateFunctionChange(currentCol.DataType, targetCol.DataType) {
					// Convert to DROP + ADD instead of MODIFY
					currentColCopy := currentCol
					targetColCopy := targetCol
					diffs = append(diffs, ColumnDiff{
						Type:        ColumnDiffDrop,
						ColumnName:  currentCol.Name,
						Current:     &currentColCopy,
						Description: "Drop column " + currentCol.Name + " (incompatible AggregateFunction change)",
					})
					diffs = append(diffs, ColumnDiff{
						Type:        ColumnDiffAdd,
						ColumnName:  targetCol.Name,
						Target:      &targetColCopy,
						Description: "Add column " + targetCol.Name + " (replacing incompatible AggregateFunction)",
					})
				} else {
					// Fix: Create copies to avoid loop variable pointer issues
					currentColCopy := currentCol
					targetColCopy := targetCol
					diffs = append(diffs, ColumnDiff{
						Type:        ColumnDiffModify,
						ColumnName:  targetCol.Name,
						Current:     &currentColCopy,
						Target:      &targetColCopy,
						Description: "Modify column " + targetCol.Name,
					})
				}
			}
		} else {
			// Column needs to be added
			// Fix: Create copy to avoid loop variable pointer issues
			targetColCopy := targetCol
			diffs = append(diffs, ColumnDiff{
				Type:        ColumnDiffAdd,
				ColumnName:  targetCol.Name,
				Target:      &targetColCopy,
				Description: "Add column " + targetCol.Name,
			})
		}
	}

	// Find columns to drop
	for _, currentCol := range current {
		if _, exists := targetCols[currentCol.Name]; !exists {
			// Fix: Create copy to avoid loop variable pointer issues
			currentColCopy := currentCol
			diffs = append(diffs, ColumnDiff{
				Type:        ColumnDiffDrop,
				ColumnName:  currentCol.Name,
				Current:     &currentColCopy,
				Description: "Drop column " + currentCol.Name,
			})
		}
	}

	// Detect renames: match DROP and ADD columns with matching types
	// Use position as primary match, but use name similarity as tiebreaker
	type posDiff struct {
		diff *ColumnDiff
		idx  int
		pos  int
	}
	dropDiffs := make([]posDiff, 0)
	addDiffs := make([]posDiff, 0)

	// Collect DROP and ADD diffs with their positions
	for i := range diffs {
		diff := &diffs[i]
		if diff.Type == ColumnDiffDrop {
			if pos, exists := currentPositions[diff.ColumnName]; exists {
				dropDiffs = append(dropDiffs, posDiff{diff: diff, idx: i, pos: pos})
			}
		} else if diff.Type == ColumnDiffAdd {
			if pos, exists := targetPositions[diff.ColumnName]; exists {
				addDiffs = append(addDiffs, posDiff{diff: diff, idx: i, pos: pos})
			}
		}
	}

	// Match DROP and ADD columns
	// Strategy: For each DROP, find the best matching ADD using:
	// 1. Same position + same type → always treat as rename (no name similarity required)
	// 2. Otherwise: matching type + name similarity >= 0.53
	//
	// Position+type match takes precedence so that e.g. min_event_received_at -> session_start_time
	// at the same ordinal is detected as a rename even when names are lexically unrelated.
	type matchCandidate struct {
		dropIdx    int
		addIdx     int
		score      float64
		similarity float64
		posMatch   bool
	}

	var candidates []matchCandidate
	for i, dropPosDiff := range dropDiffs {
		dropCol := dropPosDiff.diff.Current

		for j, addPosDiff := range addDiffs {
			addCol := addPosDiff.diff.Target

			// Check if types match (ignoring name)
			typeMatch := columnsEqualIgnoringName(*dropCol, *addCol)
			if !typeMatch {
				continue
			}

			posMatch := dropPosDiff.pos == addPosDiff.pos
			similarity := nameSimilarity(dropPosDiff.diff.ColumnName, addPosDiff.diff.ColumnName)

			// Same position + same type → treat as rename regardless of name similarity
			if posMatch {
				// No name similarity required
			} else if similarity < 0.53 {
				continue
			}

			// Score: position+type match first (highest), then by name similarity
			score := similarity * 100.0
			if posMatch {
				score = 1000.0 + similarity
			}

			candidates = append(candidates, matchCandidate{
				dropIdx:    i,
				addIdx:     j,
				score:      score,
				similarity: similarity,
				posMatch:   posMatch,
			})
		}
	}

	// Sort candidates by score (best first) to match unambiguous pairs first
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].score != candidates[j].score {
			return candidates[i].score > candidates[j].score
		}
		// Tiebreaker: prefer position matches
		if candidates[i].posMatch != candidates[j].posMatch {
			return candidates[i].posMatch
		}
		return candidates[i].similarity > candidates[j].similarity
	})

	var renameDiffs []ColumnDiff
	indicesToRemove := make(map[int]bool)
	matchedAdds := make(map[int]bool)  // Track which ADD diffs have been matched
	matchedDrops := make(map[int]bool) // Track which DROP diffs have been matched

	// Process candidates in order (best matches first)
	for _, candidate := range candidates {
		if matchedDrops[candidate.dropIdx] || matchedAdds[candidate.addIdx] {
			continue // Already matched
		}

		dropPosDiff := dropDiffs[candidate.dropIdx]
		addPosDiff := addDiffs[candidate.addIdx]

		// Position+type match: accept as rename without name similarity. Otherwise require similarity >= 0.53.
		accept := candidate.posMatch
		if !accept {
			finalSimilarity := nameSimilarity(dropPosDiff.diff.ColumnName, addPosDiff.diff.ColumnName)
			accept = finalSimilarity >= 0.53
		}

		if accept {
			currentCopy := *dropPosDiff.diff.Current
			targetCopy := *addPosDiff.diff.Target
			renameDiffs = append(renameDiffs, ColumnDiff{
				Type:        ColumnDiffRename,
				ColumnName:  dropPosDiff.diff.ColumnName, // old name
				Current:     &currentCopy,
				Target:      &targetCopy,
				Description: fmt.Sprintf("Rename column %s to %s", dropPosDiff.diff.ColumnName, addPosDiff.diff.ColumnName),
			})

			// Mark original diffs for removal
			indicesToRemove[dropPosDiff.idx] = true
			indicesToRemove[addPosDiff.idx] = true
			matchedAdds[candidate.addIdx] = true
			matchedDrops[candidate.dropIdx] = true
		}
	}

	// Remove matched ADD/DROP diffs and add RENAME diffs
	if len(indicesToRemove) > 0 {
		// Build sorted list of indices to remove (descending order)
		sortedIndices := make([]int, 0, len(indicesToRemove))
		for idx := range indicesToRemove {
			sortedIndices = append(sortedIndices, idx)
		}
		sort.Sort(sort.Reverse(sort.IntSlice(sortedIndices)))

		// Remove indices from highest to lowest to avoid index shifting issues
		for _, idx := range sortedIndices {
			diffs = append(diffs[:idx], diffs[idx+1:]...)
		}

		// Add RENAME diffs
		diffs = append(diffs, renameDiffs...)
	}

	return diffs
}

// columnsEqualIgnoringName compares two columns for equality, ignoring the name
// For rename detection, we only care about the DataType matching - other attributes
// like Default, Codec, TTL, Comment, DefaultType might differ but shouldn't prevent renames
func columnsEqualIgnoringName(a, b ColumnInfo) bool {
	// Only compare DataType - ignore name, Default, Codec, TTL, Comment, DefaultType
	// This ensures that columns with the same type but different metadata can still be renamed
	return equalAST(a.DataType, b.DataType)
}

// nameSimilarity calculates a simple similarity score between two column names
// Returns a value between 0.0 and 1.0, where 1.0 is identical
// Uses word-based matching and longest common subsequence
func nameSimilarity(name1, name2 string) float64 {
	if name1 == name2 {
		return 1.0
	}
	if len(name1) == 0 || len(name2) == 0 {
		return 0.0
	}

	// Split names by underscores to get words
	words1 := strings.Split(name1, "_")
	words2 := strings.Split(name2, "_")

	// Calculate word overlap (more important for column names)
	wordOverlap := 0.0
	matchedWords := 0
	totalWords := len(words1)
	if len(words2) > totalWords {
		totalWords = len(words2)
	}

	// Count matching words (order-independent)
	words2Map := make(map[string]int)
	for _, w := range words2 {
		words2Map[w]++
	}

	for _, w1 := range words1 {
		if count, exists := words2Map[w1]; exists && count > 0 {
			matchedWords++
			words2Map[w1]--
		}
	}

	if totalWords > 0 {
		wordOverlap = float64(matchedWords) / float64(totalWords)
	}

	// Also calculate LCS for partial matches
	lcsLen := longestCommonSubsequence(name1, name2)
	maxLen := len(name1)
	if len(name2) > maxLen {
		maxLen = len(name2)
	}
	lcsRatio := float64(lcsLen) / float64(maxLen)

	// Weight word overlap much more heavily (80%) since column names are word-based
	// LCS helps with partial word matches (20%)
	return 0.8*wordOverlap + 0.2*lcsRatio
}

// longestCommonSubsequence calculates the length of the longest common subsequence
func longestCommonSubsequence(s1, s2 string) int {
	m, n := len(s1), len(s2)
	dp := make([][]int, m+1)
	for i := range dp {
		dp[i] = make([]int, n+1)
	}

	for i := 1; i <= m; i++ {
		for j := 1; j <= n; j++ {
			if s1[i-1] == s2[j-1] {
				dp[i][j] = dp[i-1][j-1] + 1
			} else {
				if dp[i-1][j] > dp[i][j-1] {
					dp[i][j] = dp[i-1][j]
				} else {
					dp[i][j] = dp[i][j-1]
				}
			}
		}
	}
	return dp[m][n]
}

// isIncompatibleAggregateFunctionChange checks if changing from currentType to targetType
// involves an incompatible AggregateFunction type change that ClickHouse doesn't support via MODIFY.
// ClickHouse cannot MODIFY AggregateFunction columns when the aggregate function itself changes
// (e.g., from groupUniqArrayArray to argMaxIf). Such changes require DROP + ADD instead.
func isIncompatibleAggregateFunctionChange(currentType, targetType *parser.DataType) bool {
	if currentType == nil || targetType == nil {
		return false
	}

	currentStr := currentType.String()
	targetStr := targetType.String()

	// Check if both are AggregateFunction or SimpleAggregateFunction types
	isCurrentAggregate := strings.Contains(currentStr, "AggregateFunction") || strings.Contains(currentStr, "SimpleAggregateFunction")
	isTargetAggregate := strings.Contains(targetStr, "AggregateFunction") || strings.Contains(targetStr, "SimpleAggregateFunction")

	if !isCurrentAggregate || !isTargetAggregate {
		return false // Not both aggregate functions, MODIFY should work
	}

	// Extract the aggregate function name (first parameter)
	// AggregateFunction(funcName, ...) or SimpleAggregateFunction(funcName, ...)
	currentFunc := extractAggregateFunctionName(currentStr)
	targetFunc := extractAggregateFunctionName(targetStr)

	// If the function names are different, it's an incompatible change
	if currentFunc != "" && targetFunc != "" && currentFunc != targetFunc {
		return true
	}

	// Also check if switching between AggregateFunction and SimpleAggregateFunction
	if strings.HasPrefix(currentStr, "AggregateFunction") && strings.HasPrefix(targetStr, "SimpleAggregateFunction") {
		return true
	}
	if strings.HasPrefix(currentStr, "SimpleAggregateFunction") && strings.HasPrefix(targetStr, "AggregateFunction") {
		return true
	}

	return false
}

// extractAggregateFunctionName extracts the aggregate function name from a type string
// e.g., "AggregateFunction(groupUniqArrayArray, Array(String))" -> "groupUniqArrayArray"
// e.g., "SimpleAggregateFunction(max, Nullable(Decimal(18, 2)))" -> "max"
func extractAggregateFunctionName(typeStr string) string {
	// Find the opening parenthesis after AggregateFunction or SimpleAggregateFunction
	openParen := strings.Index(typeStr, "(")
	if openParen == -1 {
		return ""
	}

	// Extract the function name (first parameter before the first comma or closing paren)
	funcPart := typeStr[openParen+1:]
	// Find the first comma or closing paren
	commaIdx := strings.Index(funcPart, ",")
	closeParenIdx := strings.Index(funcPart, ")")

	endIdx := len(funcPart)
	if commaIdx != -1 && commaIdx < endIdx {
		endIdx = commaIdx
	}
	if closeParenIdx != -1 && closeParenIdx < endIdx {
		endIdx = closeParenIdx
	}

	funcName := strings.TrimSpace(funcPart[:endIdx])
	return funcName
}

// reverseColumnChanges reverses column changes for down migration
func reverseColumnChanges(changes []ColumnDiff) []ColumnDiff {
	var reversed []ColumnDiff
	for _, change := range changes {
		switch change.Type {
		case ColumnDiffAdd:
			// Fix: Create copy to avoid pointer corruption
			currentCopy := *change.Target
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffDrop,
				ColumnName:  change.ColumnName,
				Current:     &currentCopy,
				Description: "Drop column " + change.ColumnName,
			})
		case ColumnDiffDrop:
			// Fix: Create copy to avoid pointer corruption
			targetCopy := *change.Current
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffAdd,
				ColumnName:  change.ColumnName,
				Target:      &targetCopy,
				Description: "Add column " + change.ColumnName,
			})
		case ColumnDiffModify:
			// Fix: Create copies to avoid pointer corruption between UP and DOWN SQL generation
			currentCopy := *change.Target
			targetCopy := *change.Current
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffModify,
				ColumnName:  change.ColumnName,
				Current:     &currentCopy,
				Target:      &targetCopy,
				Description: "Modify column " + change.ColumnName,
			})
		case ColumnDiffRename:
			// Reverse rename: new -> old becomes old -> new
			currentCopy := *change.Target
			targetCopy := *change.Current
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffRename,
				ColumnName:  change.Target.Name, // new name becomes old
				Current:     &currentCopy,
				Target:      &targetCopy,
				Description: fmt.Sprintf("Rename column %s to %s", change.Target.Name, change.ColumnName),
			})
		}
	}
	return reversed
}

// SQL generation helper functions

// formatQualifiedTableName returns a qualified table name with optional database prefix
func formatQualifiedTableName(database, name string) string {
	if database != "" {
		return database + "." + name
	}
	return name
}

// writeOnClusterClause writes an ON CLUSTER clause if cluster is specified
func writeOnClusterClause(sql *strings.Builder, cluster string) {
	if cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(cluster)
	}
}

// formatColumnDefinition formats a complete column definition for DDL statements
func formatColumnDefinition(col ColumnInfo) string {
	var sql strings.Builder
	// Always be backticking
	sql.WriteString("`")
	sql.WriteString(col.Name)
	sql.WriteString("` ")
	sql.WriteString(col.DataType.String())

	if col.DefaultType != "" && col.Default != nil {
		sql.WriteString(" ")
		sql.WriteString(col.DefaultType)
		sql.WriteString(" ")
		sql.WriteString(col.Default.String())
	}
	if col.Codec != nil {
		sql.WriteString(" ")
		sql.WriteString(col.Codec.String())
	}
	if col.TTL != nil {
		sql.WriteString(" TTL ")
		sql.WriteString(col.TTL.Expression.String())
	}
	if col.Comment != "" {
		sql.WriteString(" COMMENT '")
		sql.WriteString(col.Comment)
		sql.WriteString("'")
	}
	return sql.String()
}

// SQL generation functions

func generateCreateTableSQL(table *TableInfo) string {
	var sql strings.Builder

	writeTableHeader(&sql, table)
	writeTableColumns(&sql, table)
	writeTableOptions(&sql, table)

	return sql.String()
}

func writeTableHeader(sql *strings.Builder, table *TableInfo) {
	sql.WriteString("CREATE ")
	if table.OrReplace {
		sql.WriteString("OR REPLACE ")
	}
	sql.WriteString("TABLE ")
	if table.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(formatQualifiedTableName(table.Database, table.Name))
	writeOnClusterClause(sql, table.Cluster)
}

func writeTableColumns(sql *strings.Builder, table *TableInfo) {
	sql.WriteString(" (\n")
	for i, col := range table.Columns {
		if i > 0 {
			sql.WriteString(",\n")
		}
		sql.WriteString("    ")
		sql.WriteString(formatColumnDefinition(col))
	}
	sql.WriteString("\n)")
}

func writeTableOptions(sql *strings.Builder, table *TableInfo) {
	// Engine
	if table.Engine != nil {
		sql.WriteString("\nENGINE = ")
		sql.WriteString(table.Engine.String())
	}

	// Table options
	if table.OrderBy != nil {
		sql.WriteString("\nORDER BY ")
		sql.WriteString(table.OrderBy.String())
	}
	if table.PartitionBy != nil {
		sql.WriteString("\nPARTITION BY ")
		sql.WriteString(table.PartitionBy.String())
	}
	if table.PrimaryKey != nil {
		sql.WriteString("\nPRIMARY KEY ")
		sql.WriteString(table.PrimaryKey.String())
	}
	if table.SampleBy != nil {
		sql.WriteString("\nSAMPLE BY ")
		sql.WriteString(table.SampleBy.String())
	}
	if table.TTL != nil {
		sql.WriteString("\nTTL ")
		sql.WriteString(table.TTL.Expression.String())
		if table.TTL.Delete != nil {
			sql.WriteString(" DELETE")
			if table.TTL.Delete.Where != nil {
				sql.WriteString(" WHERE ")
				sql.WriteString(table.TTL.Delete.Where.String())
			}
		}
	}

	// Settings
	if len(table.Settings) > 0 {
		sql.WriteString("\nSETTINGS ")
		first := true
		for key, value := range table.Settings {
			if !first {
				sql.WriteString(", ")
			}
			sql.WriteString(key)
			sql.WriteString(" = ")
			sql.WriteString(value)
			first = false
		}
	}

	// Comment
	if table.Comment != "" {
		sql.WriteString("\nCOMMENT '")
		sql.WriteString(table.Comment)
		sql.WriteString("'")
	}
}

func generateDropTableSQL(table *TableInfo) string {
	var database *string
	if table.Database != "" {
		database = &table.Database
	}

	return utils.NewSQLBuilder().
		Drop("TABLE").
		QualifiedName(database, table.Name).
		OnCluster(table.Cluster).
		String()
}

func generateRenameTableSQL(from, to *TableInfo, fromName, toName string) string {
	// Use cluster from either table (they should match after validation)
	cluster := from.Cluster
	if cluster == "" {
		cluster = to.Cluster
	}

	return utils.NewSQLBuilder().
		Rename("TABLE").
		Raw(fromName).
		Raw("TO").
		Raw(toName).
		OnCluster(cluster).
		String()
}

func generateAlterTableSQL(current, target *TableInfo, columnChanges []ColumnDiff) string {
	// Check if there are TTL changes (using ttlClausesEqual for interval normalization)
	ttlChanged := !ttlClausesEqual(current.TTL, target.TTL)

	if len(columnChanges) == 0 && !ttlChanged {
		return ""
	}

	var sql strings.Builder
	sql.WriteString("ALTER TABLE ")
	sql.WriteString(formatQualifiedTableName(target.Database, target.Name))
	writeOnClusterClause(&sql, target.Cluster)

	needsComma := false

	// Generate column modifications
	for _, change := range columnChanges {
		if needsComma {
			sql.WriteString(",")
		}
		sql.WriteString("\n    ")
		needsComma = true

		switch change.Type {
		case ColumnDiffAdd:
			sql.WriteString("ADD COLUMN ")
			sql.WriteString(formatColumnDefinition(*change.Target))
		case ColumnDiffDrop:
			sql.WriteString("DROP COLUMN ")
			// Always backtick column names for consistency
			sql.WriteString("`")
			sql.WriteString(change.ColumnName)
			sql.WriteString("`")
		case ColumnDiffModify:
			sql.WriteString("MODIFY COLUMN ")
			sql.WriteString(formatColumnDefinition(*change.Target))
		case ColumnDiffRename:
			sql.WriteString("RENAME COLUMN `")
			sql.WriteString(change.ColumnName) // old name
			sql.WriteString("` TO `")
			sql.WriteString(change.Target.Name) // new name
			sql.WriteString("`")
		}
	}

	// Generate TTL modification if changed
	if ttlChanged {
		if needsComma {
			sql.WriteString(",")
		}
		sql.WriteString("\n    ")
		if target.TTL == nil {
			sql.WriteString("REMOVE TTL")
		} else {
			sql.WriteString("MODIFY TTL ")
			sql.WriteString(format.FormatTTLClause(target.TTL))
		}
	}

	return sql.String()
}

func createTableDiff(tableName string, currentTable, targetTable *TableInfo, currentTables, targetTables map[string]*TableInfo, exists bool) (*TableDiff, error) {
	// Validate operation before proceeding
	if err := validateTableOperation(currentTable, targetTable); err != nil {
		return nil, err
	}

	if !exists {
		return handleTableNotExists(tableName, targetTable, currentTables, targetTables)
	}

	return handleTableExists(tableName, currentTable, targetTable)
}

// handleTableNotExists handles the case when a table doesn't exist in the current schema
func handleTableNotExists(tableName string, targetTable *TableInfo, currentTables, targetTables map[string]*TableInfo) (*TableDiff, error) {
	// Check if this might be a renamed table
	renamedFrom := findRenamedTable(targetTable, currentTables, targetTables)
	if renamedFrom != "" {
		return createRenameDiff(renamedFrom, tableName, currentTables[renamedFrom], targetTable), nil
	}

	// This is a create operation
	return createCreateDiff(tableName, targetTable), nil
}

// handleTableExists determines the appropriate action when a table exists in both the current and target schemas.
// It compares the current and target table definitions to decide whether no changes are needed (no-op),
// an ALTER operation is required, or a DROP+CREATE strategy should be used.
// The function first flattens nested columns in the target table to match ClickHouse's internal representation.
// If the tables are equal after flattening, no changes are needed.
// If significant differences are detected (e.g., engine or partition changes), a DROP+CREATE is performed.
// Otherwise, column-level differences are computed and an ALTER operation is generated.
func handleTableExists(tableName string, currentTable, targetTable *TableInfo) (*TableDiff, error) {
	// Table exists in both - check for changes
	// Conditionally flatten target table based on whether ClickHouse has flatten_nested=0
	// If current table has Nested columns, keep target as-is; otherwise flatten to match
	comparisonTargetTable := MaybeFlattenNestedColumns(currentTable, targetTable)
	if tablesEqual(currentTable, comparisonTargetTable) {
		return nil, nil
	}

	// Check if we need DROP+CREATE strategy
	if shouldUseDropCreate(currentTable, targetTable) {
		return createDropCreateDiff(tableName, currentTable, targetTable), nil
	}

	// Generate column diffs for regular tables
	// Use comparison target table (may be flattened or not depending on ClickHouse setting)
	columnChanges := compareColumns(currentTable.Columns, comparisonTargetTable.Columns)

	return createAlterDiff(tableName, currentTable, targetTable, columnChanges), nil
}

// shouldUseDropCreate determines if a table modification requires DROP+CREATE strategy.
//
// DROP+CREATE is required instead of ALTER in the following cases:
//   - Integration engines (e.g., Kafka, RabbitMQ, MySQL, ODBC, JDBC, etc.) are read-only from ClickHouse's perspective.
//     Any modification to tables using these engines cannot be performed via ALTER and requires dropping and recreating the table.
//   - Certain engine changes, such as ReplicatedMergeTree parameter changes (e.g., changing the replica path, zookeeper path, or other engine settings),
//     cannot be altered in-place and require the table to be dropped and recreated.
//
// This function checks for these conditions and returns true if DROP+CREATE is necessary.
func shouldUseDropCreate(currentTable, targetTable *TableInfo) bool {
	// For integration engines or engine changes that require DROP+CREATE, use DROP+CREATE strategy
	// Integration engines are read-only from ClickHouse perspective and modifications require recreating the table
	// ReplicatedMergeTree parameter changes also require DROP+CREATE as they cannot be altered
	return isIntegrationEngine(currentTable.Engine) ||
		isIntegrationEngine(targetTable.Engine) ||
		requiresDropCreate(currentTable.Engine, targetTable.Engine)
}

// createRenameDiff creates a TableDiff for rename operation
func createRenameDiff(oldName, newName string, currentTable, targetTable *TableInfo) *TableDiff {
	return &TableDiff{
		DiffBase: DiffBase{
			Type:        string(TableDiffRename),
			Name:        oldName,
			NewName:     newName,
			Description: fmt.Sprintf("Rename table %s to %s", oldName, newName),
			UpSQL:       generateRenameTableSQL(currentTable, targetTable, oldName, newName),
			DownSQL:     generateRenameTableSQL(targetTable, currentTable, newName, oldName),
		},
		Current: currentTable,
		Target:  targetTable,
	}
}

// createCreateDiff creates a TableDiff for create operation
func createCreateDiff(tableName string, targetTable *TableInfo) *TableDiff {
	return &TableDiff{
		DiffBase: DiffBase{
			Type:        string(TableDiffCreate),
			Name:        tableName,
			Description: "Create table " + tableName,
			UpSQL:       generateCreateTableSQL(targetTable),
			DownSQL:     generateDropTableSQL(targetTable),
		},
		Target: targetTable,
	}
}

// createDropCreateDiff creates a TableDiff for DROP+CREATE operation
func createDropCreateDiff(tableName string, currentTable, targetTable *TableInfo) *TableDiff {
	reason := "integration engine"
	if requiresDropCreate(currentTable.Engine, targetTable.Engine) {
		reason = "engine parameter change"
	}

	return &TableDiff{
		DiffBase: DiffBase{
			Type:        string(TableDiffAlter),
			Name:        tableName,
			Description: fmt.Sprintf("Alter table %s (DROP+CREATE for %s)", tableName, reason),
			UpSQL:       generateDropTableSQL(currentTable) + "\n\n" + generateCreateTableSQL(targetTable),
			DownSQL:     generateDropTableSQL(targetTable) + "\n\n" + generateCreateTableSQL(currentTable),
		},
		Current: currentTable,
		Target:  targetTable,
	}
}

// createAlterDiff creates a TableDiff for alter operation
func createAlterDiff(tableName string, currentTable, targetTable *TableInfo, columnChanges []ColumnDiff) *TableDiff {
	return &TableDiff{
		DiffBase: DiffBase{
			Type:        string(TableDiffAlter),
			Name:        tableName,
			Description: "Alter table " + tableName,
			UpSQL:       generateAlterTableSQL(currentTable, targetTable, columnChanges),
			DownSQL:     generateAlterTableSQL(targetTable, currentTable, reverseColumnChanges(columnChanges)),
		},
		Current:       currentTable,
		Target:        targetTable,
		ColumnChanges: columnChanges,
	}
}
