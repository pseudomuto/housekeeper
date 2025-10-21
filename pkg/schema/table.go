package schema

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
	"github.com/pseudomuto/housekeeper/pkg/consts"
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
		Type          TableDiffType // Type of operation (CREATE, ALTER, DROP, RENAME)
		TableName     string        // Name of the table being modified (with database prefix if needed)
		Description   string        // Human-readable description of the change
		UpSQL         string        // SQL to apply the change (forward migration)
		DownSQL       string        // SQL to rollback the change (reverse migration)
		Current       *TableInfo    // Current state (nil if table doesn't exist)
		Target        *TableInfo    // Target state (nil if table should be dropped)
		NewTableName  string        // For rename operations - the new name
		ColumnChanges []ColumnDiff  // For ALTER operations - specific column changes
	}

	// TableDiffType represents the type of table difference
	TableDiffType string

	// TableInfo represents parsed table information extracted from DDL statements.
	// This structure contains all the properties needed for table comparison and
	// migration generation, including columns, engine, and other table options.
	TableInfo struct {
		Name          string              // Table name (without database prefix)
		Database      string              // Database name (empty if not specified)
		Engine        *parser.TableEngine // Engine AST
		Cluster       string              // Cluster name for distributed tables
		Comment       string              // Table comment
		OrderBy       *parser.Expression  // ORDER BY expression AST
		PartitionBy   *parser.Expression  // PARTITION BY expression AST
		PrimaryKey    *parser.Expression  // PRIMARY KEY expression AST
		SampleBy      *parser.Expression  // SAMPLE BY expression AST
		TTL           *parser.Expression  // Table-level TTL expression AST
		Settings      map[string]string   // Table settings
		Columns       []ColumnInfo        // Column definitions
		OrReplace     bool                // Whether CREATE OR REPLACE was used
		IfNotExists   bool                // Whether IF NOT EXISTS was used
		AsSourceTable *string             // If this table uses AS, the source table name (qualified)
		AsDependents  map[string]bool     // Tables that use AS to reference this table
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
)

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
		!equalAST(t.TTL, other.TTL) {
		return false
	}

	// Compare settings and columns
	return compare.Maps(t.Settings, other.Settings) &&
		compare.Slices(t.Columns, other.Columns, func(a, b ColumnInfo) bool {
			return a.Equal(b)
		})
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

// enginesEqual compares two table engines with special handling for ReplicatedMergeTree.
// When the target engine is ReplicatedMergeTree with no parameters, parameters are ignored
// in the comparison. This handles the case where ClickHouse auto-expands ReplicatedMergeTree()
// to ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}') internally.
func enginesEqual(target, current *parser.TableEngine) bool {
	// Use standard equalAST for nil checks
	if target == nil || current == nil {
		return equalAST(target, current)
	}

	// Special handling for ReplicatedMergeTree when target has no parameters
	if target.Name == "ReplicatedMergeTree" && len(target.Parameters) == 0 {
		// If target has no parameters, only compare engine names (ignore current parameters)
		return current.Name == "ReplicatedMergeTree"
	}

	// For all other cases (including ReplicatedMergeTree with explicit parameters), use standard AST comparison
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

	// Find tables to create or modify (exist in target but not in current) - sorted for deterministic order
	targetTableNames := make([]string, 0, len(targetTables))
	for tableName := range targetTables {
		targetTableNames = append(targetTableNames, tableName)
	}
	sort.Strings(targetTableNames)

	for _, tableName := range targetTableNames {
		targetTable := targetTables[tableName]
		currentTable, exists := currentTables[tableName]
		diff, err := createTableDiff(tableName, currentTable, targetTable, currentTables, targetTables, exists)
		if err != nil {
			return nil, err
		}
		if diff != nil {
			diffs = append(diffs, diff)
			// Remove from currentTables if this was a rename to avoid processing as a drop
			if diff.Type == TableDiffRename {
				delete(currentTables, diff.TableName)
			}

			// Propagate column changes to AS dependents (only for ALTER operations)
			if diff.Type == TableDiffAlter && targetTable.AsDependents != nil && len(diff.ColumnChanges) > 0 {
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
	currentTableNames := make([]string, 0, len(currentTables))
	for tableName := range currentTables {
		if _, exists := targetTables[tableName]; !exists {
			currentTableNames = append(currentTableNames, tableName)
		}
	}
	sort.Strings(currentTableNames)

	for _, tableName := range currentTableNames {
		currentTable := currentTables[tableName]
		diff := &TableDiff{
			Type:        TableDiffDrop,
			TableName:   tableName,
			Description: "Drop table " + tableName,
			Current:     currentTable,
		}
		diff.UpSQL = generateDropTableSQL(currentTable)
		diff.DownSQL = generateCreateTableSQL(currentTable)
		diffs = append(diffs, diff)
	}

	return diffs, nil
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
			Type:          TableDiffAlter,
			TableName:     dependentName,
			Description:   fmt.Sprintf("Propagated column changes from %s (AS dependency)", sourceDiff.TableName),
			Current:       currentDep,
			Target:        targetDep,
			ColumnChanges: sourceDiff.ColumnChanges, // Same column changes
		}

		// Generate SQL based on engine type
		if isViewLikeEngine(targetDep.Engine) {
			// For Distributed, Memory, etc.: DROP + CREATE is safe and necessary
			propDiff.UpSQL = fmt.Sprintf("-- Recreate to match schema changes from %s\n", sourceDiff.TableName) +
				generateDropTableSQL(currentDep) + ";\n" +
				generateCreateTableSQL(targetDep)
			propDiff.DownSQL = generateDropTableSQL(targetDep) + ";\n" +
				generateCreateTableSQL(currentDep)
		} else {
			// For MergeTree, etc.: Use ALTER to preserve data
			propDiff.UpSQL = fmt.Sprintf("-- Propagated from %s (AS dependency)\n", sourceDiff.TableName) +
				generateAlterTableSQL(targetDep, sourceDiff.ColumnChanges)
			propDiff.DownSQL = generateAlterTableSQL(currentDep, reverseColumnChanges(sourceDiff.ColumnChanges))
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
				tableInfo.TTL = &ttl.Expression
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
		// Use flattened target table for comparison
		flattenedTargetTable := FlattenNestedColumns(targetTable)
		if tablesEqualIgnoringName(currentTable, flattenedTargetTable) {
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

	for _, col := range current {
		currentCols[col.Name] = col
	}
	for _, col := range target {
		targetCols[col.Name] = col
	}

	// Find columns to add or modify
	for _, targetCol := range target {
		if currentCol, exists := currentCols[targetCol.Name]; exists {
			// Column exists - check for changes using Equal() method
			if !currentCol.Equal(targetCol) {
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

	return diffs
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
	sql.WriteString(formatColumnDataType(col.DataType))

	if col.DefaultType != "" && col.Default != nil {
		sql.WriteString(" ")
		sql.WriteString(col.DefaultType)
		sql.WriteString(" ")
		sql.WriteString(col.Default.String())
	}
	if col.Codec != nil {
		sql.WriteString(" ")
		sql.WriteString(formatColumnCodec(col.Codec))
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
		sql.WriteString(formatTableEngine(table.Engine))
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
		sql.WriteString(table.TTL.String())
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

func generateAlterTableSQL(target *TableInfo, columnChanges []ColumnDiff) string {
	if len(columnChanges) == 0 {
		return ""
	}

	var sql strings.Builder
	sql.WriteString("ALTER TABLE ")
	sql.WriteString(formatQualifiedTableName(target.Database, target.Name))
	writeOnClusterClause(&sql, target.Cluster)

	// Generate column modifications
	for i, change := range columnChanges {
		if i > 0 {
			sql.WriteString(",")
		}
		sql.WriteString("\n    ")

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
		}
	}

	return sql.String()
}

// Helper functions for formatting

func formatTableEngine(engine *parser.TableEngine) string {
	if engine == nil {
		return ""
	}

	result := engine.Name
	if len(engine.Parameters) > 0 {
		result += "("
		for i, param := range engine.Parameters {
			if i > 0 {
				result += ", "
			}
			result += param.Value()
		}
		result += ")"
	}
	return result
}

func formatColumnDataType(dataType *parser.DataType) string {
	if dataType == nil {
		return ""
	}

	if dataType.Nullable != nil {
		return "Nullable(" + formatColumnDataType(dataType.Nullable.Type) + ")"
	}
	if dataType.Array != nil {
		return "Array(" + formatColumnDataType(dataType.Array.Type) + ")"
	}
	if dataType.Tuple != nil {
		result := "Tuple("
		for i, element := range dataType.Tuple.Elements {
			if i > 0 {
				result += ", "
			}
			if element.Name != nil {
				result += *element.Name + " " + formatColumnDataType(element.Type)
			} else {
				result += formatColumnDataType(element.UnnamedType)
			}
		}
		return result + ")"
	}
	if dataType.Nested != nil {
		result := "Nested("
		for i, col := range dataType.Nested.Columns {
			if i > 0 {
				result += ", "
			}
			result += col.Name + " " + formatColumnDataType(col.Type)
		}
		return result + ")"
	}
	if dataType.Map != nil {
		return "Map(" + formatColumnDataType(dataType.Map.KeyType) + ", " + formatColumnDataType(dataType.Map.ValueType) + ")"
	}
	if dataType.LowCardinality != nil {
		return "LowCardinality(" + formatColumnDataType(dataType.LowCardinality.Type) + ")"
	}
	//nolint:nestif // Complex nested logic needed for data type formatting
	if dataType.Simple != nil {
		result := dataType.Simple.Name
		if len(dataType.Simple.Parameters) > 0 {
			result += "("
			for i, param := range dataType.Simple.Parameters {
				if i > 0 {
					result += ", "
				}
				if param.String != nil {
					result += *param.String
				} else if param.Number != nil {
					result += *param.Number
				} else if param.Ident != nil {
					result += *param.Ident
				}
			}
			result += ")"
		}
		return result
	}
	return ""
}

func formatColumnCodec(codec *parser.CodecClause) string {
	if codec == nil {
		return ""
	}

	result := "CODEC("
	for i, codecSpec := range codec.Codecs {
		if i > 0 {
			result += ", "
		}
		result += codecSpec.Name
		if len(codecSpec.Parameters) > 0 {
			result += "("
			for j, param := range codecSpec.Parameters {
				if j > 0 {
					result += ", "
				}
				if param.String != nil {
					result += *param.String
				} else if param.Number != nil {
					result += *param.Number
				} else if param.Ident != nil {
					result += *param.Ident
				}
			}
			result += ")"
		}
	}
	return result + ")"
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
	// For comparison purposes, flatten the target table to match ClickHouse's internal representation
	// Current table is already flattened by ClickHouse, but target table may have Nested syntax
	flattenedTargetTable := FlattenNestedColumns(targetTable)
	if tablesEqual(currentTable, flattenedTargetTable) {
		return nil, nil
	}

	// Check if we need DROP+CREATE strategy
	if shouldUseDropCreate(currentTable, targetTable) {
		return createDropCreateDiff(tableName, currentTable, targetTable), nil
	}

	// Generate column diffs for regular tables
	// Use flattened target table for comparison but preserve original for SQL generation
	columnChanges := compareColumns(currentTable.Columns, flattenedTargetTable.Columns)

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
	diff := &TableDiff{
		Type:         TableDiffRename,
		TableName:    oldName,
		NewTableName: newName,
		Description:  fmt.Sprintf("Rename table %s to %s", oldName, newName),
		Current:      currentTable,
		Target:       targetTable,
	}
	diff.UpSQL = generateRenameTableSQL(diff.Current, diff.Target, oldName, newName)
	diff.DownSQL = generateRenameTableSQL(diff.Target, diff.Current, newName, oldName)
	return diff
}

// createCreateDiff creates a TableDiff for create operation
func createCreateDiff(tableName string, targetTable *TableInfo) *TableDiff {
	diff := &TableDiff{
		Type:        TableDiffCreate,
		TableName:   tableName,
		Description: "Create table " + tableName,
		Target:      targetTable,
	}
	diff.UpSQL = generateCreateTableSQL(targetTable)
	diff.DownSQL = generateDropTableSQL(targetTable)
	return diff
}

// createDropCreateDiff creates a TableDiff for DROP+CREATE operation
func createDropCreateDiff(tableName string, currentTable, targetTable *TableInfo) *TableDiff {
	reason := "integration engine"
	if requiresDropCreate(currentTable.Engine, targetTable.Engine) {
		reason = "engine parameter change"
	}

	diff := &TableDiff{
		Type:        TableDiffAlter,
		TableName:   tableName,
		Description: fmt.Sprintf("Alter table %s (DROP+CREATE for %s)", tableName, reason),
		Current:     currentTable,
		Target:      targetTable,
	}
	diff.UpSQL = generateDropTableSQL(currentTable) + "\n\n" + generateCreateTableSQL(targetTable)
	diff.DownSQL = generateDropTableSQL(targetTable) + "\n\n" + generateCreateTableSQL(currentTable)
	return diff
}

// createAlterDiff creates a TableDiff for alter operation
func createAlterDiff(tableName string, currentTable, targetTable *TableInfo, columnChanges []ColumnDiff) *TableDiff {
	diff := &TableDiff{
		Type:          TableDiffAlter,
		TableName:     tableName,
		Description:   "Alter table " + tableName,
		Current:       currentTable,
		Target:        targetTable,
		ColumnChanges: columnChanges,
	}
	diff.UpSQL = generateAlterTableSQL(targetTable, columnChanges)
	diff.DownSQL = generateAlterTableSQL(currentTable, reverseColumnChanges(columnChanges))
	return diff
}

// GetDiffType returns the diff type for TableDiff (implements diffProcessor interface)
func (d *TableDiff) GetDiffType() string {
	return string(d.Type)
}

// GetUpSQL returns the up SQL for TableDiff (implements diffProcessor interface)
func (d *TableDiff) GetUpSQL() string {
	return d.UpSQL
}
