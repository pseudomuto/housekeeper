package parser_test

import (
	"embed"
	"flag"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

//go:embed testdata/*.sql
var testdataFS embed.FS

// TestCase represents expected results for a SQL file
type TestCase struct {
	Databases    []ExpectedDatabase   `yaml:"databases,omitempty"`
	Dictionaries []ExpectedDictionary `yaml:"dictionaries,omitempty"`
	Views        []ExpectedView       `yaml:"views,omitempty"`
	Tables       []ExpectedTable      `yaml:"tables,omitempty"`
	Queries      []ExpectedQuery      `yaml:"queries,omitempty"`
}

// ExpectedDatabase represents expected database properties
type ExpectedDatabase struct {
	Name    string `yaml:"name"`
	Cluster string `yaml:"cluster,omitempty"`
	Engine  string `yaml:"engine,omitempty"`
	Comment string `yaml:"comment,omitempty"`
}

// ExpectedDictionary represents expected dictionary properties
type ExpectedDictionary struct {
	Name        string `yaml:"name"`
	Database    string `yaml:"database,omitempty"`
	Cluster     string `yaml:"cluster,omitempty"`
	OrReplace   bool   `yaml:"or_replace,omitempty"`
	Comment     string `yaml:"comment,omitempty"`
	Operation   string `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP
	IfNotExists bool   `yaml:"if_not_exists,omitempty"`
	IfExists    bool   `yaml:"if_exists,omitempty"`
	Permanently bool   `yaml:"permanently,omitempty"`
	Sync        bool   `yaml:"sync,omitempty"`
}

// ExpectedView represents expected view properties
type ExpectedView struct {
	Name         string `yaml:"name"`
	Database     string `yaml:"database,omitempty"`
	Cluster      string `yaml:"cluster,omitempty"`
	OrReplace    bool   `yaml:"or_replace,omitempty"`
	Materialized bool   `yaml:"materialized,omitempty"`
	Operation    string `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP, RENAME
	IfNotExists  bool   `yaml:"if_not_exists,omitempty"`
	IfExists     bool   `yaml:"if_exists,omitempty"`
	Permanently  bool   `yaml:"permanently,omitempty"`
	Sync         bool   `yaml:"sync,omitempty"`
	To           string `yaml:"to,omitempty"`
	Engine       string `yaml:"engine,omitempty"`
	Populate     bool   `yaml:"populate,omitempty"`
}

// ExpectedTable represents expected table properties
type ExpectedTable struct {
	Name            string            `yaml:"name"`
	Database        string            `yaml:"database,omitempty"`
	Cluster         string            `yaml:"cluster,omitempty"`
	Operation       string            `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP, RENAME
	OrReplace       bool              `yaml:"or_replace,omitempty"`
	IfNotExists     bool              `yaml:"if_not_exists,omitempty"`
	IfExists        bool              `yaml:"if_exists,omitempty"`
	Permanently     bool              `yaml:"permanently,omitempty"`
	Sync            bool              `yaml:"sync,omitempty"`
	Engine          string            `yaml:"engine,omitempty"`
	Comment         string            `yaml:"comment,omitempty"`
	Columns         []ExpectedColumn  `yaml:"columns,omitempty"`
	OrderBy         string            `yaml:"order_by,omitempty"`
	PartitionBy     string            `yaml:"partition_by,omitempty"`
	PrimaryKey      string            `yaml:"primary_key,omitempty"`
	SampleBy        string            `yaml:"sample_by,omitempty"`
	TTL             string            `yaml:"ttl,omitempty"`
	Settings        map[string]string `yaml:"settings,omitempty"`
	AlterOperations map[string]int    `yaml:"alter_operations,omitempty"`
}

// ExpectedColumn represents expected column properties
type ExpectedColumn struct {
	Name     string `yaml:"name"`
	DataType string `yaml:"data_type"`
	Default  string `yaml:"default,omitempty"`
	Codec    string `yaml:"codec,omitempty"`
	TTL      string `yaml:"ttl,omitempty"`
	Comment  string `yaml:"comment,omitempty"`
}

// ExpectedQuery represents expected SELECT query properties
type ExpectedQuery struct {
	Distinct    bool     `yaml:"distinct,omitempty"`
	Columns     []string `yaml:"columns,omitempty"`
	From        string   `yaml:"from,omitempty"`
	HasWhere    bool     `yaml:"has_where,omitempty"`
	HasGroupBy  bool     `yaml:"has_group_by,omitempty"`
	HasHaving   bool     `yaml:"has_having,omitempty"`
	HasOrderBy  bool     `yaml:"has_order_by,omitempty"`
	HasLimit    bool     `yaml:"has_limit,omitempty"`
	JoinCount   int      `yaml:"join_count,omitempty"`
	WindowFuncs bool     `yaml:"window_functions,omitempty"`
}

var updateFlag = flag.Bool("update", false, "update YAML test files")

// formatEngine formats a database engine with optional parameters
func formatEngine(engine *DatabaseEngine) string {
	if len(engine.Parameters) == 0 {
		return engine.Name
	}

	params := make([]string, len(engine.Parameters))
	for i, param := range engine.Parameters {
		params[i] = param.Value
	}
	return engine.Name + "(" + strings.Join(params, ", ") + ")"
}

// removeQuotes removes surrounding single quotes from a string
func removeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

func TestParserWithTestdata(t *testing.T) {
	// Find all SQL files in embedded testdata
	sqlFiles, err := fs.Glob(testdataFS, "testdata/*.sql")
	require.NoError(t, err, "Failed to find SQL files")

	// Run each test case
	for _, sqlPath := range sqlFiles {
		sqlFile := filepath.Base(sqlPath)
		yamlFile := sqlFile[:len(sqlFile)-4] + ".yaml" // Replace .sql with .yaml

		t.Run(sqlFile, func(t *testing.T) {
			// Read SQL file from embedded FS
			sqlData, err := testdataFS.ReadFile(sqlPath)
			require.NoError(t, err, "Failed to read SQL file: %s", sqlFile)

			// Parse SQL
			grammar, err := ParseSQL(string(sqlData))
			require.NoError(t, err, "Failed to parse SQL from %s", sqlFile)

			yamlPath := filepath.Join("testdata", yamlFile)

			if *updateFlag {
				// Generate and save YAML file
				expectedResult := generateTestCaseFromGrammar(grammar)
				yamlData, err := yaml.Marshal(&expectedResult)
				require.NoError(t, err, "Failed to marshal YAML for %s", yamlFile)

				err = os.WriteFile(yamlPath, yamlData, 0o600)
				require.NoError(t, err, "Failed to write YAML file: %s", yamlFile)

				t.Logf("Updated %s", yamlFile)
				return
			}

			// Load expected results from corresponding YAML file
			yamlData, err := os.ReadFile(yamlPath)
			require.NoError(t, err, "Failed to read YAML file: %s (run with -update to generate)", yamlFile)

			var expectedResult TestCase
			err = yaml.Unmarshal(yamlData, &expectedResult)
			require.NoError(t, err, "Failed to parse YAML file: %s", yamlFile)

			// Verify results
			verifyGrammar(t, grammar, expectedResult, sqlFile)
		})
	}
}

// generateTestCaseFromGrammar converts a parsed grammar into a TestCase for YAML generation
// Processing statements in order to preserve the sequence from the SQL file
//
//nolint:gocognit,gocyclo,maintidx // Complex test case generation function handles all DDL statement types
func generateTestCaseFromGrammar(grammar *SQL) TestCase {
	var expectedDatabases []ExpectedDatabase
	var expectedDictionaries []ExpectedDictionary
	var expectedViews []ExpectedView
	var expectedTables []ExpectedTable
	var expectedQueries []ExpectedQuery

	// Keep track of accumulated ALTER operations by table name
	alterOperations := make(map[string]map[string]int)

	// Process statements in order
	for _, stmt := range grammar.Statements {
		//nolint:nestif // Complex nested logic needed for comprehensive test case generation
		if stmt.CreateDatabase != nil {
			db := stmt.CreateDatabase
			expectedDB := ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
			if db.Engine != nil {
				expectedDB.Engine = formatEngine(db.Engine)
			}
			if db.Comment != nil {
				expectedDB.Comment = removeQuotes(*db.Comment)
			}
			expectedDatabases = append(expectedDatabases, expectedDB)

		} else if stmt.AlterDatabase != nil {
			db := stmt.AlterDatabase
			expectedDB := ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
			if db.Action != nil && db.Action.ModifyComment != nil {
				expectedDB.Comment = removeQuotes(*db.Action.ModifyComment)
			}
			expectedDatabases = append(expectedDatabases, expectedDB)

		} else if stmt.AttachDatabase != nil {
			db := stmt.AttachDatabase
			expectedDB := ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
			if db.Engine != nil {
				expectedDB.Engine = formatEngine(db.Engine)
			}
			expectedDatabases = append(expectedDatabases, expectedDB)

		} else if stmt.DetachDatabase != nil {
			db := stmt.DetachDatabase
			// Skip IF EXISTS operations since they may not refer to existing databases
			if !db.IfExists {
				expectedDB := ExpectedDatabase{
					Name: db.Name,
				}
				if db.OnCluster != nil {
					expectedDB.Cluster = *db.OnCluster
				}
				expectedDatabases = append(expectedDatabases, expectedDB)
			}

		} else if stmt.DropDatabase != nil {
			db := stmt.DropDatabase
			// Skip IF EXISTS operations since they may not refer to existing databases
			if !db.IfExists {
				expectedDB := ExpectedDatabase{
					Name: db.Name,
				}
				if db.OnCluster != nil {
					expectedDB.Cluster = *db.OnCluster
				}
				expectedDatabases = append(expectedDatabases, expectedDB)
			}

		} else if stmt.RenameDatabase != nil {
			for _, rename := range stmt.RenameDatabase.Renames {
				expectedDB := ExpectedDatabase{
					Name: rename.From,
				}
				if stmt.RenameDatabase.OnCluster != nil {
					expectedDB.Cluster = *stmt.RenameDatabase.OnCluster
				}
				expectedDatabases = append(expectedDatabases, expectedDB)
			}
		} else if stmt.CreateDictionary != nil {
			dict := stmt.CreateDictionary
			expectedDict := ExpectedDictionary{
				Name:        dict.Name,
				Operation:   "CREATE",
				OrReplace:   dict.OrReplace,
				IfNotExists: dict.IfNotExists != nil,
			}
			if dict.Database != nil {
				expectedDict.Database = *dict.Database
			}
			if dict.OnCluster != nil {
				expectedDict.Cluster = *dict.OnCluster
			}
			if dict.Comment != nil {
				expectedDict.Comment = removeQuotes(*dict.Comment)
			}
			expectedDictionaries = append(expectedDictionaries, expectedDict)

		} else if stmt.AttachDictionary != nil {
			dict := stmt.AttachDictionary
			expectedDict := ExpectedDictionary{
				Name:        dict.Name,
				Operation:   "ATTACH",
				IfNotExists: dict.IfNotExists != nil,
			}
			if dict.Database != nil {
				expectedDict.Database = *dict.Database
			}
			if dict.OnCluster != nil {
				expectedDict.Cluster = *dict.OnCluster
			}
			expectedDictionaries = append(expectedDictionaries, expectedDict)

		} else if stmt.DetachDictionary != nil {
			dict := stmt.DetachDictionary
			expectedDict := ExpectedDictionary{
				Name:        dict.Name,
				Operation:   "DETACH",
				IfExists:    dict.IfExists != nil,
				Permanently: dict.Permanently != nil,
				Sync:        dict.Sync != nil,
			}
			if dict.Database != nil {
				expectedDict.Database = *dict.Database
			}
			if dict.OnCluster != nil {
				expectedDict.Cluster = *dict.OnCluster
			}
			expectedDictionaries = append(expectedDictionaries, expectedDict)

		} else if stmt.DropDictionary != nil {
			dict := stmt.DropDictionary
			expectedDict := ExpectedDictionary{
				Name:      dict.Name,
				Operation: "DROP",
				IfExists:  dict.IfExists != nil,
				Sync:      dict.Sync != nil,
			}
			if dict.Database != nil {
				expectedDict.Database = *dict.Database
			}
			if dict.OnCluster != nil {
				expectedDict.Cluster = *dict.OnCluster
			}
			expectedDictionaries = append(expectedDictionaries, expectedDict)

		} else if stmt.RenameDictionary != nil {
			for _, rename := range stmt.RenameDictionary.Renames {
				expectedDict := ExpectedDictionary{
					Name:      rename.FromName,
					Operation: "RENAME",
				}
				if rename.FromDatabase != nil {
					expectedDict.Database = *rename.FromDatabase
				}
				if stmt.RenameDictionary.OnCluster != nil {
					expectedDict.Cluster = *stmt.RenameDictionary.OnCluster
				}
				expectedDictionaries = append(expectedDictionaries, expectedDict)
			}
		} else if stmt.CreateView != nil {
			view := stmt.CreateView
			expectedView := ExpectedView{
				Name:         view.Name,
				Operation:    "CREATE",
				OrReplace:    view.OrReplace,
				Materialized: view.Materialized,
				IfNotExists:  view.IfNotExists,
				Populate:     view.Populate,
			}
			if view.Database != nil {
				expectedView.Database = *view.Database
			}
			if view.OnCluster != nil {
				expectedView.Cluster = *view.OnCluster
			}
			if view.To != nil {
				expectedView.To = *view.To
			}
			if view.Engine != nil {
				// Build engine string from new ViewEngine structure
				engineStr := view.Engine.Name
				if len(view.Engine.Parameters) > 0 {
					engineStr += "("
					var params []string
					for _, param := range view.Engine.Parameters {
						if param.String != nil {
							params = append(params, *param.String)
						} else if param.Number != nil {
							params = append(params, *param.Number)
						} else if param.Ident != nil {
							params = append(params, *param.Ident)
						}
					}
					engineStr += strings.Join(params, ",") + ")"
				} else {
					// Add empty parentheses if no parameters
					engineStr += "()"
				}

				// Add ORDER BY clause if present
				if view.Engine.OrderBy != nil {
					exprStr := view.Engine.OrderBy.Expression.String()
					// Handle different formats based on whether it's a tuple or single expression
					if len(exprStr) > 2 && exprStr[0] == '(' && exprStr[len(exprStr)-1] == ')' {
						// Tuple expression: ORDERBY(date,user_id)
						engineStr += "ORDERBY(" + exprStr[1:len(exprStr)-1] + ")"
					} else {
						// Single expression: ORDERBYdate
						engineStr += "ORDERBY" + exprStr
					}
				}

				// Add other clauses as needed
				if view.Engine.PartitionBy != nil {
					engineStr += "PARTITIONBY(" + view.Engine.PartitionBy.Expression.String() + ")"
				}

				expectedView.Engine = engineStr
			}
			expectedViews = append(expectedViews, expectedView)

		} else if stmt.AttachView != nil {
			view := stmt.AttachView
			expectedView := ExpectedView{
				Name:        view.Name,
				Operation:   "ATTACH",
				IfNotExists: view.IfNotExists,
			}
			if view.Database != nil {
				expectedView.Database = *view.Database
			}
			if view.OnCluster != nil {
				expectedView.Cluster = *view.OnCluster
			}
			expectedViews = append(expectedViews, expectedView)

		} else if stmt.DetachView != nil {
			view := stmt.DetachView
			expectedView := ExpectedView{
				Name:        view.Name,
				Operation:   "DETACH",
				IfExists:    view.IfExists,
				Permanently: view.Permanently,
				Sync:        view.Sync,
			}
			if view.Database != nil {
				expectedView.Database = *view.Database
			}
			if view.OnCluster != nil {
				expectedView.Cluster = *view.OnCluster
			}
			expectedViews = append(expectedViews, expectedView)

		} else if stmt.DropView != nil {
			view := stmt.DropView
			expectedView := ExpectedView{
				Name:      view.Name,
				Operation: "DROP",
				IfExists:  view.IfExists,
				Sync:      view.Sync,
			}
			if view.Database != nil {
				expectedView.Database = *view.Database
			}
			if view.OnCluster != nil {
				expectedView.Cluster = *view.OnCluster
			}
			expectedViews = append(expectedViews, expectedView)

		} else if stmt.CreateTable != nil {
			table := stmt.CreateTable
			expectedTable := ExpectedTable{
				Name:        table.Name,
				Operation:   "CREATE",
				OrReplace:   table.OrReplace,
				IfNotExists: table.IfNotExists,
			}
			if table.Database != nil {
				expectedTable.Database = *table.Database
			}
			if table.OnCluster != nil {
				expectedTable.Cluster = *table.OnCluster
			}
			if table.Engine != nil {
				expectedTable.Engine = formatTableEngine(table.Engine)
			}
			if table.Comment != nil {
				expectedTable.Comment = removeQuotes(*table.Comment)
			}
			if table.OrderBy != nil {
				expectedTable.OrderBy = "expression"
			}
			if table.PartitionBy != nil {
				expectedTable.PartitionBy = "expression"
			}
			if table.PrimaryKey != nil {
				expectedTable.PrimaryKey = "expression"
			}
			if table.SampleBy != nil {
				expectedTable.SampleBy = "expression"
			}
			if table.TTL != nil {
				expectedTable.TTL = "expression"
			}
			if table.Settings != nil {
				settings := make(map[string]string)
				for _, setting := range table.Settings.Settings {
					settings[setting.Name] = setting.Value
				}
				expectedTable.Settings = settings
			}

			// Process columns from table elements
			var columns []ExpectedColumn
			for _, element := range table.Elements {
				if element.Column == nil {
					continue // Skip indexes, constraints, and projections for now
				}
				col := element.Column
				expectedCol := ExpectedColumn{
					Name:     col.Name,
					DataType: formatDataType(col.DataType),
				}
				if col.Default != nil {
					expectedCol.Default = col.Default.Type + " expression"
				}
				if col.Codec != nil {
					expectedCol.Codec = formatCodec(col.Codec)
				}
				if col.TTL != nil {
					expectedCol.TTL = "expression"
				}
				if col.Comment != nil {
					expectedCol.Comment = removeQuotes(*col.Comment)
				}
				columns = append(columns, expectedCol)
			}
			expectedTable.Columns = columns
			expectedTables = append(expectedTables, expectedTable)

		} else if stmt.AttachTable != nil {
			table := stmt.AttachTable
			expectedTable := ExpectedTable{
				Name:        table.Name,
				Operation:   "ATTACH",
				IfNotExists: table.IfNotExists,
			}
			if table.Database != nil {
				expectedTable.Database = *table.Database
			}
			if table.OnCluster != nil {
				expectedTable.Cluster = *table.OnCluster
			}
			expectedTables = append(expectedTables, expectedTable)

		} else if stmt.DetachTable != nil {
			table := stmt.DetachTable
			expectedTable := ExpectedTable{
				Name:        table.Name,
				Operation:   "DETACH",
				IfExists:    table.IfExists,
				Permanently: table.Permanently,
				Sync:        table.Sync,
			}
			if table.Database != nil {
				expectedTable.Database = *table.Database
			}
			if table.OnCluster != nil {
				expectedTable.Cluster = *table.OnCluster
			}
			expectedTables = append(expectedTables, expectedTable)

		} else if stmt.DropTable != nil {
			table := stmt.DropTable
			expectedTable := ExpectedTable{
				Name:      table.Name,
				Operation: "DROP",
				IfExists:  table.IfExists,
				Sync:      table.Sync,
			}
			if table.Database != nil {
				expectedTable.Database = *table.Database
			}
			if table.OnCluster != nil {
				expectedTable.Cluster = *table.OnCluster
			}
			expectedTables = append(expectedTables, expectedTable)

		} else if stmt.RenameTable != nil {
			for _, rename := range stmt.RenameTable.Renames {
				expectedTable := ExpectedTable{
					Name:      rename.FromName,
					Operation: "RENAME",
				}
				if rename.FromDatabase != nil {
					expectedTable.Database = *rename.FromDatabase
				}
				if stmt.RenameTable.OnCluster != nil {
					expectedTable.Cluster = *stmt.RenameTable.OnCluster
				}
				expectedTables = append(expectedTables, expectedTable)
			}
		} else if stmt.AlterTable != nil {
			table := stmt.AlterTable
			tableName := table.Name
			if table.Database != nil {
				tableName = *table.Database + "." + table.Name
			}

			// Initialize operation counts for this table if not exists
			if alterOperations[tableName] == nil {
				alterOperations[tableName] = make(map[string]int)
			}

			// Count and accumulate operations by type
			for _, op := range table.Operations {
				if op.AddColumn != nil {
					alterOperations[tableName]["ADD_COLUMN"]++
				} else if op.DropColumn != nil {
					alterOperations[tableName]["DROP_COLUMN"]++
				} else if op.ModifyColumn != nil {
					alterOperations[tableName]["MODIFY_COLUMN"]++
				} else if op.RenameColumn != nil {
					alterOperations[tableName]["RENAME_COLUMN"]++
				} else if op.CommentColumn != nil {
					alterOperations[tableName]["COMMENT_COLUMN"]++
				} else if op.ClearColumn != nil {
					alterOperations[tableName]["CLEAR_COLUMN"]++
				} else if op.AddIndex != nil {
					alterOperations[tableName]["ADD_INDEX"]++
				} else if op.DropIndex != nil {
					alterOperations[tableName]["DROP_INDEX"]++
				} else if op.AddConstraint != nil {
					alterOperations[tableName]["ADD_CONSTRAINT"]++
				} else if op.DropConstraint != nil {
					alterOperations[tableName]["DROP_CONSTRAINT"]++
				} else if op.AddProjection != nil {
					alterOperations[tableName]["ADD_PROJECTION"]++
				} else if op.DropProjection != nil {
					alterOperations[tableName]["DROP_PROJECTION"]++
				} else if op.ModifyTTL != nil {
					alterOperations[tableName]["MODIFY_TTL"]++
				} else if op.DeleteTTL != nil {
					alterOperations[tableName]["DELETE_TTL"]++
				} else if op.Update != nil {
					alterOperations[tableName]["UPDATE"]++
				} else if op.Delete != nil {
					alterOperations[tableName]["DELETE"]++
				} else if op.Freeze != nil {
					alterOperations[tableName]["FREEZE"]++
				} else if op.AttachPartition != nil {
					alterOperations[tableName]["ATTACH_PARTITION"]++
				} else if op.DetachPartition != nil {
					alterOperations[tableName]["DETACH_PARTITION"]++
				} else if op.DropPartition != nil {
					alterOperations[tableName]["DROP_PARTITION"]++
				} else if op.MovePartition != nil {
					alterOperations[tableName]["MOVE_PARTITION"]++
				} else if op.ReplacePartition != nil {
					alterOperations[tableName]["REPLACE_PARTITION"]++
				} else if op.FetchPartition != nil {
					alterOperations[tableName]["FETCH_PARTITION"]++
				} else if op.ModifyOrderBy != nil {
					alterOperations[tableName]["MODIFY_ORDER_BY"]++
				} else if op.ModifySampleBy != nil {
					alterOperations[tableName]["MODIFY_SAMPLE_BY"]++
				} else if op.RemoveSampleBy != nil {
					alterOperations[tableName]["REMOVE_SAMPLE_BY"]++
				} else if op.ModifySetting != nil {
					alterOperations[tableName]["MODIFY_SETTING"]++
				} else if op.ResetSetting != nil {
					alterOperations[tableName]["RESET_SETTING"]++
				}
			}

			// Create ALTER TABLE entry immediately to preserve order
			var database, name string
			if parts := strings.Split(tableName, "."); len(parts) == 2 {
				database = parts[0]
				name = parts[1]
			} else {
				name = tableName
			}

			expectedTable := ExpectedTable{
				Name:            name,
				Database:        database,
				Operation:       "ALTER",
				IfExists:        table.IfExists,
				AlterOperations: make(map[string]int),
			}

			if table.OnCluster != nil {
				expectedTable.Cluster = *table.OnCluster
			}

			// Copy the accumulated operations for this table
			for opType, count := range alterOperations[tableName] {
				expectedTable.AlterOperations[opType] = count
			}

			expectedTables = append(expectedTables, expectedTable)
		} else if stmt.SelectStatement != nil {
			sel := stmt.SelectStatement
			expectedQuery := ExpectedQuery{
				Distinct: sel.Distinct,
			}

			// Extract column information
			if sel.Columns != nil {
				for _, col := range sel.Columns {
					if col.Star != nil {
						expectedQuery.Columns = append(expectedQuery.Columns, "*")
					} else if col.Expression != nil {
						// For simplicity, just indicate we have non-wildcard columns
						if len(expectedQuery.Columns) == 0 || expectedQuery.Columns[0] != "non_wildcard" {
							expectedQuery.Columns = append(expectedQuery.Columns, "non_wildcard")
						}
					}
				}
			}

			// Extract FROM information
			if sel.From != nil {
				if sel.From.Table.TableName != nil {
					table := sel.From.Table.TableName
					if table.Database != nil {
						expectedQuery.From = *table.Database + "." + table.Table
					} else {
						expectedQuery.From = table.Table
					}
				}

				// Count JOINs
				expectedQuery.JoinCount = len(sel.From.Joins)
			}

			// Check for clauses
			expectedQuery.HasWhere = sel.Where != nil
			expectedQuery.HasGroupBy = sel.GroupBy != nil
			expectedQuery.HasHaving = sel.Having != nil
			expectedQuery.HasOrderBy = sel.OrderBy != nil
			expectedQuery.HasLimit = sel.Limit != nil

			// Check for window functions in columns
			if sel.Columns != nil {
				for _, col := range sel.Columns {
					if col.Expression != nil && hasWindowFunction(col.Expression) {
						expectedQuery.WindowFuncs = true
						break
					}
				}
			}

			expectedQueries = append(expectedQueries, expectedQuery)
		}
	}

	testCase := TestCase{}
	if len(expectedDatabases) > 0 {
		testCase.Databases = expectedDatabases
	}
	if len(expectedDictionaries) > 0 {
		testCase.Dictionaries = expectedDictionaries
	}
	if len(expectedViews) > 0 {
		testCase.Views = expectedViews
	}
	if len(expectedTables) > 0 {
		testCase.Tables = expectedTables
	}
	if len(expectedQueries) > 0 {
		testCase.Queries = expectedQueries
	}

	return testCase
}

// hasWindowFunction checks if an expression contains window functions
func hasWindowFunction(expr *Expression) bool {
	if expr == nil {
		return false
	}
	// Navigate through the expression hierarchy to find function calls
	if expr.Or != nil && expr.Or.And != nil && expr.Or.And.Not != nil &&
		expr.Or.And.Not.Comparison != nil && expr.Or.And.Not.Comparison.Addition != nil &&
		expr.Or.And.Not.Comparison.Addition.Multiplication != nil &&
		expr.Or.And.Not.Comparison.Addition.Multiplication.Unary != nil &&
		expr.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary != nil &&
		expr.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Function != nil &&
		expr.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Function.Over != nil {
		return true
	}
	// Could add more recursive checks for nested expressions
	return false
}

func verifyGrammar(t *testing.T, actualGrammar *SQL, expected TestCase, sqlFile string) {
	// Use the same logic as generateTestCaseFromGrammar to extract actual results
	actualTestCase := generateTestCaseFromGrammar(actualGrammar)

	// Compare arrays by length first
	require.Len(t, actualTestCase.Databases, len(expected.Databases),
		"Wrong number of databases in %s", sqlFile)
	require.Len(t, actualTestCase.Dictionaries, len(expected.Dictionaries),
		"Wrong number of dictionaries in %s", sqlFile)
	require.Len(t, actualTestCase.Views, len(expected.Views),
		"Wrong number of views in %s", sqlFile)
	require.Len(t, actualTestCase.Tables, len(expected.Tables),
		"Wrong number of tables in %s", sqlFile)
	require.Len(t, actualTestCase.Queries, len(expected.Queries),
		"Wrong number of queries in %s", sqlFile)

	// Check databases in order
	for i, expectedDB := range expected.Databases {
		actualDB := actualTestCase.Databases[i]
		require.Equal(t, expectedDB.Name, actualDB.Name,
			"Wrong database name at index %d in %s", i, sqlFile)
		require.Equal(t, expectedDB.Cluster, actualDB.Cluster,
			"Wrong cluster in database %s at index %d from %s", expectedDB.Name, i, sqlFile)
		require.Equal(t, expectedDB.Engine, actualDB.Engine,
			"Wrong engine in database %s at index %d from %s", expectedDB.Name, i, sqlFile)
		require.Equal(t, expectedDB.Comment, actualDB.Comment,
			"Wrong comment in database %s at index %d from %s", expectedDB.Name, i, sqlFile)
	}

	// Check dictionaries in order
	for i, expectedDict := range expected.Dictionaries {
		actualDict := actualTestCase.Dictionaries[i]
		require.Equal(t, expectedDict.Name, actualDict.Name,
			"Wrong dictionary name at index %d in %s", i, sqlFile)
		require.Equal(t, expectedDict.Database, actualDict.Database,
			"Wrong database in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.Cluster, actualDict.Cluster,
			"Wrong cluster in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.Operation, actualDict.Operation,
			"Wrong operation in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.OrReplace, actualDict.OrReplace,
			"Wrong OR REPLACE flag in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.IfNotExists, actualDict.IfNotExists,
			"Wrong IF NOT EXISTS flag in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.IfExists, actualDict.IfExists,
			"Wrong IF EXISTS flag in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.Permanently, actualDict.Permanently,
			"Wrong PERMANENTLY flag in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.Sync, actualDict.Sync,
			"Wrong SYNC flag in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
		require.Equal(t, expectedDict.Comment, actualDict.Comment,
			"Wrong comment in dictionary %s at index %d from %s", expectedDict.Name, i, sqlFile)
	}

	// Check views in order
	for i, expectedView := range expected.Views {
		actualView := actualTestCase.Views[i]
		require.Equal(t, expectedView.Name, actualView.Name,
			"Wrong view name at index %d in %s", i, sqlFile)
		require.Equal(t, expectedView.Database, actualView.Database,
			"Wrong database in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Cluster, actualView.Cluster,
			"Wrong cluster in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Operation, actualView.Operation,
			"Wrong operation in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.OrReplace, actualView.OrReplace,
			"Wrong OR REPLACE flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Materialized, actualView.Materialized,
			"Wrong MATERIALIZED flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.IfNotExists, actualView.IfNotExists,
			"Wrong IF NOT EXISTS flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.IfExists, actualView.IfExists,
			"Wrong IF EXISTS flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Permanently, actualView.Permanently,
			"Wrong PERMANENTLY flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Sync, actualView.Sync,
			"Wrong SYNC flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.To, actualView.To,
			"Wrong TO clause in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Engine, actualView.Engine,
			"Wrong ENGINE clause in view %s at index %d from %s", expectedView.Name, i, sqlFile)
		require.Equal(t, expectedView.Populate, actualView.Populate,
			"Wrong POPULATE flag in view %s at index %d from %s", expectedView.Name, i, sqlFile)
	}

	// Check tables in order
	for i, expectedTable := range expected.Tables {
		actualTable := actualTestCase.Tables[i]
		require.Equal(t, expectedTable.Name, actualTable.Name,
			"Wrong table name at index %d in %s", i, sqlFile)
		require.Equal(t, expectedTable.Database, actualTable.Database,
			"Wrong database in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Cluster, actualTable.Cluster,
			"Wrong cluster in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Operation, actualTable.Operation,
			"Wrong operation in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.OrReplace, actualTable.OrReplace,
			"Wrong OR REPLACE flag in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.IfNotExists, actualTable.IfNotExists,
			"Wrong IF NOT EXISTS flag in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.IfExists, actualTable.IfExists,
			"Wrong IF EXISTS flag in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Permanently, actualTable.Permanently,
			"Wrong PERMANENTLY flag in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Sync, actualTable.Sync,
			"Wrong SYNC flag in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Engine, actualTable.Engine,
			"Wrong engine in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Comment, actualTable.Comment,
			"Wrong comment in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.OrderBy, actualTable.OrderBy,
			"Wrong ORDER BY in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.PartitionBy, actualTable.PartitionBy,
			"Wrong PARTITION BY in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.PrimaryKey, actualTable.PrimaryKey,
			"Wrong PRIMARY KEY in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.SampleBy, actualTable.SampleBy,
			"Wrong SAMPLE BY in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.TTL, actualTable.TTL,
			"Wrong TTL in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Settings, actualTable.Settings,
			"Wrong settings in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.AlterOperations, actualTable.AlterOperations,
			"Wrong alter operations in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
		require.Equal(t, expectedTable.Columns, actualTable.Columns,
			"Wrong columns in table %s at index %d from %s", expectedTable.Name, i, sqlFile)
	}
}

// formatTableEngine formats a table ENGINE clause with parameters
func formatTableEngine(engine *TableEngine) string {
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

// formatDataType formats a data type with all its components
func formatDataType(dataType *DataType) string {
	if dataType == nil {
		return ""
	}

	if dataType.Nullable != nil {
		return "Nullable(" + formatDataType(dataType.Nullable.Type) + ")"
	}
	if dataType.Array != nil {
		return "Array(" + formatDataType(dataType.Array.Type) + ")"
	}
	if dataType.Tuple != nil {
		result := "Tuple("
		for i, element := range dataType.Tuple.Elements {
			if i > 0 {
				result += ", "
			}
			if element.Name != nil {
				result += *element.Name + " " + formatDataType(element.Type)
			} else {
				result += formatDataType(element.UnnamedType)
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
			result += col.Name + " " + formatDataType(col.Type)
		}
		return result + ")"
	}
	if dataType.Map != nil {
		return "Map(" + formatDataType(dataType.Map.KeyType) + ", " + formatDataType(dataType.Map.ValueType) + ")"
	}
	if dataType.LowCardinality != nil {
		return "LowCardinality(" + formatDataType(dataType.LowCardinality.Type) + ")"
	}
	//nolint:nestif // Complex nested logic needed for data type formatting in tests
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

// formatCodec formats a codec clause
func formatCodec(codec *CodecClause) string {
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
