package schema

import (
	"fmt"
	"reflect"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
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
		Name        string            // Table name (without database prefix)
		Database    string            // Database name (empty if not specified)
		Engine      string            // Engine type and parameters (e.g., "MergeTree()", "ReplicatedMergeTree('/path', 'replica')")
		Cluster     string            // Cluster name for distributed tables
		Comment     string            // Table comment
		OrderBy     string            // ORDER BY expression
		PartitionBy string            // PARTITION BY expression
		PrimaryKey  string            // PRIMARY KEY expression
		SampleBy    string            // SAMPLE BY expression
		TTL         string            // Table-level TTL expression
		Settings    map[string]string // Table settings
		Columns     []ColumnInfo      // Column definitions
		OrReplace   bool              // Whether CREATE OR REPLACE was used
		IfNotExists bool              // Whether IF NOT EXISTS was used
	}

	// ColumnInfo represents a single column definition
	ColumnInfo struct {
		Name     string // Column name
		DataType string // Full data type specification (e.g., "Nullable(String)", "Array(UInt64)")
		Default  string // Default value specification (e.g., "DEFAULT 'value'", "MATERIALIZED expr")
		Codec    string // Compression codec (e.g., "CODEC(ZSTD)")
		TTL      string // Column-level TTL
		Comment  string // Column comment
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

// compareTables compares current and target parsed schemas to find table differences.
// It identifies tables that need to be created, altered, dropped, or renamed.
//
// The function performs comprehensive table comparison including:
// - Table structure (engine, settings, comments)
// - Column definitions and modifications
// - Rename detection based on content similarity
// - Proper ordering for migration generation
func compareTables(current, target *parser.SQL) ([]*TableDiff, error) {
	currentTables := extractTablesFromSQL(current)
	targetTables := extractTablesFromSQL(target)

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

// extractTablesFromSQL extracts table information from parsed SQL statements
//
//nolint:gocognit,funlen // Complex function needed for comprehensive table parsing
func extractTablesFromSQL(sql *parser.SQL) map[string]*TableInfo {
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

			if table.Database != nil {
				tableInfo.Database = normalizeIdentifier(*table.Database)
			}
			if table.OnCluster != nil {
				tableInfo.Cluster = *table.OnCluster
			}
			if table.Engine != nil {
				tableInfo.Engine = formatTableEngine(table.Engine)
			}
			if table.Comment != nil {
				tableInfo.Comment = removeQuotes(*table.Comment)
			}
			if orderBy := table.GetOrderBy(); orderBy != nil {
				tableInfo.OrderBy = orderBy.Expression.String()
			}
			if partitionBy := table.GetPartitionBy(); partitionBy != nil {
				tableInfo.PartitionBy = partitionBy.Expression.String()
			}
			if primaryKey := table.GetPrimaryKey(); primaryKey != nil {
				tableInfo.PrimaryKey = primaryKey.Expression.String()
			}
			if sampleBy := table.GetSampleBy(); sampleBy != nil {
				tableInfo.SampleBy = sampleBy.Expression.String()
			}
			if ttl := table.GetTTL(); ttl != nil {
				tableInfo.TTL = ttl.Expression.String()
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
					DataType: formatColumnDataType(col.DataType),
				}
				if defaultClause := col.GetDefault(); defaultClause != nil {
					columnInfo.Default = defaultClause.Type + " " + defaultClause.Expression.String()
				}
				if codecClause := col.GetCodec(); codecClause != nil {
					columnInfo.Codec = formatColumnCodec(codecClause)
				}
				if ttlClause := col.GetTTL(); ttlClause != nil {
					columnInfo.TTL = ttlClause.Expression.String()
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

	return tables
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
		if tablesEqualIgnoringName(currentTable, targetTable) {
			return currentName
		}
	}
	return ""
}

// tablesEqual compares two tables for equality
func tablesEqual(a, b *TableInfo) bool {
	// For housekeeper tables, ignore cluster differences
	clusterMatch := a.Cluster == b.Cluster
	if a.Database == "housekeeper" || b.Database == "housekeeper" {
		clusterMatch = true // Ignore cluster differences for housekeeper objects
	}

	return a.Engine == b.Engine &&
		clusterMatch &&
		a.Comment == b.Comment &&
		a.OrderBy == b.OrderBy &&
		a.PartitionBy == b.PartitionBy &&
		a.PrimaryKey == b.PrimaryKey &&
		a.SampleBy == b.SampleBy &&
		a.TTL == b.TTL &&
		reflect.DeepEqual(a.Settings, b.Settings) &&
		columnsEqual(a.Columns, b.Columns)
}

// tablesEqualIgnoringName compares two tables for equality ignoring name and database
func tablesEqualIgnoringName(a, b *TableInfo) bool {
	// For housekeeper tables, ignore cluster differences
	clusterMatch := a.Cluster == b.Cluster
	if a.Database == "housekeeper" || b.Database == "housekeeper" {
		clusterMatch = true // Ignore cluster differences for housekeeper objects
	}

	return a.Engine == b.Engine &&
		clusterMatch &&
		a.Comment == b.Comment &&
		a.OrderBy == b.OrderBy &&
		a.PartitionBy == b.PartitionBy &&
		a.PrimaryKey == b.PrimaryKey &&
		a.SampleBy == b.SampleBy &&
		a.TTL == b.TTL &&
		reflect.DeepEqual(a.Settings, b.Settings) &&
		columnsEqual(a.Columns, b.Columns)
}

// columnsEqual compares two column slices for equality using case-insensitive comment comparison
func columnsEqual(a, b []ColumnInfo) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if !columnInfoEqual(a[i], b[i]) {
			return false
		}
	}
	return true
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
			// Column exists - check for changes using case-insensitive comment comparison
			if !columnInfoEqual(currentCol, targetCol) {
				diffs = append(diffs, ColumnDiff{
					Type:        ColumnDiffModify,
					ColumnName:  targetCol.Name,
					Current:     &currentCol,
					Target:      &targetCol,
					Description: "Modify column " + targetCol.Name,
				})
			}
		} else {
			// Column needs to be added
			diffs = append(diffs, ColumnDiff{
				Type:        ColumnDiffAdd,
				ColumnName:  targetCol.Name,
				Target:      &targetCol,
				Description: "Add column " + targetCol.Name,
			})
		}
	}

	// Find columns to drop
	for _, currentCol := range current {
		if _, exists := targetCols[currentCol.Name]; !exists {
			diffs = append(diffs, ColumnDiff{
				Type:        ColumnDiffDrop,
				ColumnName:  currentCol.Name,
				Current:     &currentCol,
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
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffDrop,
				ColumnName:  change.ColumnName,
				Current:     change.Target,
				Description: "Drop column " + change.ColumnName,
			})
		case ColumnDiffDrop:
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffAdd,
				ColumnName:  change.ColumnName,
				Target:      change.Current,
				Description: "Add column " + change.ColumnName,
			})
		case ColumnDiffModify:
			reversed = append(reversed, ColumnDiff{
				Type:        ColumnDiffModify,
				ColumnName:  change.ColumnName,
				Current:     change.Target,
				Target:      change.Current,
				Description: "Modify column " + change.ColumnName,
			})
		}
	}
	return reversed
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

	// Table name with database prefix
	if table.Database != "" {
		sql.WriteString(table.Database)
		sql.WriteString(".")
	}
	sql.WriteString(table.Name)

	if table.Cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(table.Cluster)
	}
}

func writeTableColumns(sql *strings.Builder, table *TableInfo) {
	// Columns
	sql.WriteString(" (\n")
	for i, col := range table.Columns {
		if i > 0 {
			sql.WriteString(",\n")
		}
		sql.WriteString("    ")
		sql.WriteString(col.Name)
		sql.WriteString(" ")
		sql.WriteString(col.DataType)
		if col.Default != "" {
			sql.WriteString(" ")
			sql.WriteString(col.Default)
		}
		if col.Codec != "" {
			sql.WriteString(" ")
			sql.WriteString(col.Codec)
		}
		if col.TTL != "" {
			sql.WriteString(" TTL ")
			sql.WriteString(col.TTL)
		}
		if col.Comment != "" {
			sql.WriteString(" COMMENT '")
			sql.WriteString(col.Comment)
			sql.WriteString("'")
		}
	}
	sql.WriteString("\n)")
}

func writeTableOptions(sql *strings.Builder, table *TableInfo) {
	// Engine
	if table.Engine != "" {
		sql.WriteString("\nENGINE = ")
		sql.WriteString(table.Engine)
	}

	// Table options
	if table.OrderBy != "" {
		sql.WriteString("\nORDER BY ")
		sql.WriteString(table.OrderBy)
	}
	if table.PartitionBy != "" {
		sql.WriteString("\nPARTITION BY ")
		sql.WriteString(table.PartitionBy)
	}
	if table.PrimaryKey != "" {
		sql.WriteString("\nPRIMARY KEY ")
		sql.WriteString(table.PrimaryKey)
	}
	if table.SampleBy != "" {
		sql.WriteString("\nSAMPLE BY ")
		sql.WriteString(table.SampleBy)
	}
	if table.TTL != "" {
		sql.WriteString("\nTTL ")
		sql.WriteString(table.TTL)
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
	var sql strings.Builder
	sql.WriteString("DROP TABLE ")
	if table.Database != "" {
		sql.WriteString(table.Database)
		sql.WriteString(".")
	}
	sql.WriteString(table.Name)
	if table.Cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(table.Cluster)
	}
	return sql.String()
}

func generateRenameTableSQL(from, to *TableInfo, fromName, toName string) string {
	var sql strings.Builder
	sql.WriteString("RENAME TABLE ")
	sql.WriteString(fromName)
	sql.WriteString(" TO ")
	sql.WriteString(toName)
	if from.Cluster != "" || to.Cluster != "" {
		cluster := from.Cluster
		if cluster == "" {
			cluster = to.Cluster
		}
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(cluster)
	}
	return sql.String()
}

func generateAlterTableSQL(target *TableInfo, columnChanges []ColumnDiff) string {
	if len(columnChanges) == 0 {
		return ""
	}

	var sql strings.Builder
	sql.WriteString("ALTER TABLE ")
	if target.Database != "" {
		sql.WriteString(target.Database)
		sql.WriteString(".")
	}
	sql.WriteString(target.Name)

	if target.Cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(target.Cluster)
	}

	// Generate column modifications
	for i, change := range columnChanges {
		if i > 0 {
			sql.WriteString(",")
		}
		sql.WriteString("\n    ")

		switch change.Type {
		case ColumnDiffAdd:
			sql.WriteString("ADD COLUMN ")
			sql.WriteString(change.Target.Name)
			sql.WriteString(" ")
			sql.WriteString(change.Target.DataType)
			if change.Target.Default != "" {
				sql.WriteString(" ")
				sql.WriteString(change.Target.Default)
			}
			if change.Target.Codec != "" {
				sql.WriteString(" ")
				sql.WriteString(change.Target.Codec)
			}
			if change.Target.TTL != "" {
				sql.WriteString(" TTL ")
				sql.WriteString(change.Target.TTL)
			}
			if change.Target.Comment != "" {
				sql.WriteString(" COMMENT '")
				sql.WriteString(change.Target.Comment)
				sql.WriteString("'")
			}
		case ColumnDiffDrop:
			sql.WriteString("DROP COLUMN ")
			sql.WriteString(change.ColumnName)
		case ColumnDiffModify:
			sql.WriteString("MODIFY COLUMN ")
			sql.WriteString(change.Target.Name)
			sql.WriteString(" ")
			sql.WriteString(change.Target.DataType)
			if change.Target.Default != "" {
				sql.WriteString(" ")
				sql.WriteString(change.Target.Default)
			}
			if change.Target.Codec != "" {
				sql.WriteString(" ")
				sql.WriteString(change.Target.Codec)
			}
			if change.Target.TTL != "" {
				sql.WriteString(" TTL ")
				sql.WriteString(change.Target.TTL)
			}
			if change.Target.Comment != "" {
				sql.WriteString(" COMMENT '")
				sql.WriteString(change.Target.Comment)
				sql.WriteString("'")
			}
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
		// Check if this might be a renamed table
		renamedFrom := findRenamedTable(targetTable, currentTables, targetTables)
		if renamedFrom != "" {
			// This is a rename operation
			diff := &TableDiff{
				Type:         TableDiffRename,
				TableName:    renamedFrom,
				NewTableName: tableName,
				Description:  fmt.Sprintf("Rename table %s to %s", renamedFrom, tableName),
				Current:      currentTables[renamedFrom],
				Target:       targetTable,
			}
			diff.UpSQL = generateRenameTableSQL(diff.Current, diff.Target, renamedFrom, tableName)
			diff.DownSQL = generateRenameTableSQL(diff.Target, diff.Current, tableName, renamedFrom)
			return diff, nil
		}

		// This is a create operation
		diff := &TableDiff{
			Type:        TableDiffCreate,
			TableName:   tableName,
			Description: "Create table " + tableName,
			Target:      targetTable,
		}
		diff.UpSQL = generateCreateTableSQL(targetTable)
		diff.DownSQL = generateDropTableSQL(targetTable)
		return diff, nil
	}

	// Table exists in both - check for changes
	if tablesEqual(currentTable, targetTable) {
		return nil, nil
	}

	// For integration engines, use DROP+CREATE (similar to materialized views)
	// Integration engines are read-only from ClickHouse perspective and modifications
	// require recreating the table anyway
	if isIntegrationEngine(currentTable.Engine) || isIntegrationEngine(targetTable.Engine) {
		diff := &TableDiff{
			Type:        TableDiffAlter,
			TableName:   tableName,
			Description: "Alter table " + tableName + " (DROP+CREATE for integration engine)",
			Current:     currentTable,
			Target:      targetTable,
		}
		diff.UpSQL = generateDropTableSQL(currentTable) + "\n\n" + generateCreateTableSQL(targetTable)
		diff.DownSQL = generateDropTableSQL(targetTable) + "\n\n" + generateCreateTableSQL(currentTable)
		return diff, nil
	}

	// Generate column diffs for regular tables
	columnChanges := compareColumns(currentTable.Columns, targetTable.Columns)

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
	return diff, nil
}
