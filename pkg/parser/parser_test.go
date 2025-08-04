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
	Databases    map[string]ExpectedDatabase   `yaml:"databases,omitempty"`
	Dictionaries map[string]ExpectedDictionary `yaml:"dictionaries,omitempty"`
	Views        map[string]ExpectedView       `yaml:"views,omitempty"`
	Tables       map[string]ExpectedTable      `yaml:"tables,omitempty"`
}

// ExpectedDatabase represents expected database properties
type ExpectedDatabase struct {
	Name    string `yaml:"name"`
	Cluster string `yaml:"cluster"`
	Engine  string `yaml:"engine"`
	Comment string `yaml:"comment"`
}

// ExpectedDictionary represents expected dictionary properties
type ExpectedDictionary struct {
	Name        string `yaml:"name"`
	Database    string `yaml:"database"`
	Cluster     string `yaml:"cluster"`
	OrReplace   bool   `yaml:"or_replace"`
	Comment     string `yaml:"comment"`
	Operation   string `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP
	IfNotExists bool   `yaml:"if_not_exists"`
	IfExists    bool   `yaml:"if_exists"`
	Permanently bool   `yaml:"permanently"`
	Sync        bool   `yaml:"sync"`
}

// ExpectedView represents expected view properties
type ExpectedView struct {
	Name         string `yaml:"name"`
	Database     string `yaml:"database"`
	Cluster      string `yaml:"cluster"`
	OrReplace    bool   `yaml:"or_replace"`
	Materialized bool   `yaml:"materialized"`
	Operation    string `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP, RENAME
	IfNotExists  bool   `yaml:"if_not_exists"`
	IfExists     bool   `yaml:"if_exists"`
	Permanently  bool   `yaml:"permanently"`
	Sync         bool   `yaml:"sync"`
	To           string `yaml:"to,omitempty"`
	Engine       string `yaml:"engine,omitempty"`
	Populate     bool   `yaml:"populate"`
}

// ExpectedTable represents expected table properties
type ExpectedTable struct {
	Name        string            `yaml:"name"`
	Database    string            `yaml:"database"`
	Cluster     string            `yaml:"cluster"`
	Operation   string            `yaml:"operation"` // CREATE, ATTACH, DETACH, DROP, RENAME
	OrReplace   bool              `yaml:"or_replace"`
	IfNotExists bool              `yaml:"if_not_exists"`
	IfExists    bool              `yaml:"if_exists"`
	Permanently bool              `yaml:"permanently"`
	Sync        bool              `yaml:"sync"`
	Engine      string            `yaml:"engine,omitempty"`
	Comment     string            `yaml:"comment,omitempty"`
	Columns     []ExpectedColumn  `yaml:"columns,omitempty"`
	OrderBy     string            `yaml:"order_by,omitempty"`
	PartitionBy string            `yaml:"partition_by,omitempty"`
	PrimaryKey  string            `yaml:"primary_key,omitempty"`
	SampleBy    string            `yaml:"sample_by,omitempty"`
	TTL         string            `yaml:"ttl,omitempty"`
	Settings    map[string]string `yaml:"settings,omitempty"`
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

var updateFlag = flag.Bool("update", false, "update YAML test files")

// formatEngine formats a database engine with optional parameters
func formatEngine(engine *DatabaseEngine) string {
	if engine.Parameters == nil || len(engine.Parameters) == 0 {
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

				err = os.WriteFile(yamlPath, yamlData, 0644)
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
func generateTestCaseFromGrammar(grammar *Grammar) TestCase {
	expectedDatabases := make(map[string]ExpectedDatabase)

	// First pass: collect all database references from all statement types
	for _, stmt := range grammar.Statements {
		var dbName string
		var expectedDB ExpectedDatabase

		if stmt.CreateDatabase != nil {
			db := stmt.CreateDatabase
			dbName = db.Name
			expectedDB = ExpectedDatabase{
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
		} else if stmt.AlterDatabase != nil {
			db := stmt.AlterDatabase
			dbName = db.Name
			// Check if database already exists, preserve existing data
			if existingDB, exists := expectedDatabases[dbName]; exists {
				expectedDB = existingDB
			} else {
				expectedDB = ExpectedDatabase{
					Name: db.Name,
				}
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
			if db.Action != nil && db.Action.ModifyComment != nil {
				expectedDB.Comment = removeQuotes(*db.Action.ModifyComment)
			}
		} else if stmt.AttachDatabase != nil {
			db := stmt.AttachDatabase
			dbName = db.Name
			expectedDB = ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
			if db.Engine != nil {
				expectedDB.Engine = formatEngine(db.Engine)
			}
		} else if stmt.DetachDatabase != nil {
			db := stmt.DetachDatabase
			// Skip IF EXISTS operations since they may not refer to existing databases
			if db.IfExists {
				continue
			}
			dbName = db.Name
			expectedDB = ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
		} else if stmt.DropDatabase != nil {
			db := stmt.DropDatabase
			// Skip IF EXISTS operations since they may not refer to existing databases
			if db.IfExists {
				continue
			}
			dbName = db.Name
			expectedDB = ExpectedDatabase{
				Name: db.Name,
			}
			if db.OnCluster != nil {
				expectedDB.Cluster = *db.OnCluster
			}
		} else if stmt.RenameDatabase != nil {
			for _, rename := range stmt.RenameDatabase.Renames {
				// Process FROM database
				expectedDB = ExpectedDatabase{
					Name: rename.From,
				}
				if stmt.RenameDatabase.OnCluster != nil {
					expectedDB.Cluster = *stmt.RenameDatabase.OnCluster
				}
				expectedDatabases[rename.From] = expectedDB
			}
			continue
		}

		if dbName != "" {
			expectedDatabases[dbName] = expectedDB
		}
	}

	// Process dictionaries
	expectedDictionaries := make(map[string]ExpectedDictionary)
	for _, stmt := range grammar.Statements {
		if stmt.CreateDictionary != nil {
			dict := stmt.CreateDictionary
			dictName := dict.Name
			if dict.Database != nil {
				dictName = *dict.Database + "." + dict.Name
			}

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
			expectedDictionaries[dictName] = expectedDict
		} else if stmt.AttachDictionary != nil {
			dict := stmt.AttachDictionary
			dictName := dict.Name
			if dict.Database != nil {
				dictName = *dict.Database + "." + dict.Name
			}

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
			expectedDictionaries[dictName] = expectedDict
		} else if stmt.DetachDictionary != nil {
			dict := stmt.DetachDictionary
			dictName := dict.Name
			if dict.Database != nil {
				dictName = *dict.Database + "." + dict.Name
			}

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
			expectedDictionaries[dictName] = expectedDict
		} else if stmt.DropDictionary != nil {
			dict := stmt.DropDictionary
			dictName := dict.Name
			if dict.Database != nil {
				dictName = *dict.Database + "." + dict.Name
			}

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
			expectedDictionaries[dictName] = expectedDict
		} else if stmt.AttachDictionary != nil {
			dict := stmt.AttachDictionary
			dictName := dict.Name
			if dict.Database != nil {
				dictName = *dict.Database + "." + dict.Name
			}

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
			expectedDictionaries[dictName] = expectedDict
		} else if stmt.RenameDictionary != nil {
			for _, rename := range stmt.RenameDictionary.Renames {
				// Process FROM dictionary
				fromName := rename.FromName
				if rename.FromDatabase != nil {
					fromName = *rename.FromDatabase + "." + rename.FromName
				}

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
				expectedDictionaries[fromName] = expectedDict
			}
		}
	}

	// Process views
	expectedViews := make(map[string]ExpectedView)
	for _, stmt := range grammar.Statements {
		if stmt.CreateView != nil {
			view := stmt.CreateView
			viewName := view.Name
			if view.Database != nil {
				viewName = *view.Database + "." + view.Name
			}

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
				expectedView.Engine = view.Engine.Raw
			}
			expectedViews[viewName] = expectedView
		} else if stmt.AttachView != nil {
			view := stmt.AttachView
			viewName := view.Name
			if view.Database != nil {
				viewName = *view.Database + "." + view.Name
			}

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
			expectedViews[viewName] = expectedView
		} else if stmt.DetachView != nil {
			view := stmt.DetachView
			viewName := view.Name
			if view.Database != nil {
				viewName = *view.Database + "." + view.Name
			}

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
			expectedViews[viewName] = expectedView
		} else if stmt.DropView != nil {
			view := stmt.DropView
			viewName := view.Name
			if view.Database != nil {
				viewName = *view.Database + "." + view.Name
			}

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
			expectedViews[viewName] = expectedView
		}
	}

	// Process tables (CREATE TABLE and materialized view operations)
	expectedTables := make(map[string]ExpectedTable)
	for _, stmt := range grammar.Statements {
		if stmt.CreateTable != nil {
			table := stmt.CreateTable
			tableName := table.Name
			if table.Database != nil {
				tableName = *table.Database + "." + table.Name
			}

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
					continue // Skip indexes and constraints for now
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

			expectedTables[tableName] = expectedTable
		} else if stmt.AttachTable != nil {
			table := stmt.AttachTable
			tableName := table.Name
			if table.Database != nil {
				tableName = *table.Database + "." + table.Name
			}

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
			expectedTables[tableName] = expectedTable
		} else if stmt.DetachTable != nil {
			table := stmt.DetachTable
			tableName := table.Name
			if table.Database != nil {
				tableName = *table.Database + "." + table.Name
			}

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
			expectedTables[tableName] = expectedTable
		} else if stmt.DropTable != nil {
			table := stmt.DropTable
			tableName := table.Name
			if table.Database != nil {
				tableName = *table.Database + "." + table.Name
			}

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
			expectedTables[tableName] = expectedTable
		} else if stmt.RenameTable != nil {
			for _, rename := range stmt.RenameTable.Renames {
				// Process FROM table
				fromName := rename.FromName
				if rename.FromDatabase != nil {
					fromName = *rename.FromDatabase + "." + rename.FromName
				}

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
				expectedTables[fromName] = expectedTable
			}
		}
	}

	testCase := TestCase{}

	// Only include databases section if there are any databases
	if len(expectedDatabases) > 0 {
		testCase.Databases = expectedDatabases
	}

	// Only include dictionaries section if there are any dictionaries
	if len(expectedDictionaries) > 0 {
		testCase.Dictionaries = expectedDictionaries
	}

	// Only include views section if there are any views
	if len(expectedViews) > 0 {
		testCase.Views = expectedViews
	}

	// Only include tables section if there are any tables
	if len(expectedTables) > 0 {
		testCase.Tables = expectedTables
	}

	return testCase
}

func verifyGrammar(t *testing.T, actualGrammar *Grammar, expected TestCase, sqlFile string) {
	// Use the same logic as generateTestCaseFromGrammar to extract actual results
	actualTestCase := generateTestCaseFromGrammar(actualGrammar)
	actualDatabases := actualTestCase.Databases
	expectedDatabases := expected.Databases

	// Handle nil vs empty map cases
	if actualDatabases == nil {
		actualDatabases = make(map[string]ExpectedDatabase)
	}
	if expectedDatabases == nil {
		expectedDatabases = make(map[string]ExpectedDatabase)
	}

	// Check database count
	require.Len(t, actualDatabases, len(expectedDatabases),
		"Wrong number of databases in %s", sqlFile)

	// Check each expected database
	for dbName, expectedDB := range expectedDatabases {
		actualDB, exists := actualDatabases[dbName]
		require.True(t, exists, "Database %s not found in %s", dbName, sqlFile)

		// Verify database properties
		require.Equal(t, expectedDB.Name, actualDB.Name,
			"Wrong database name in %s", sqlFile)
		require.Equal(t, expectedDB.Cluster, actualDB.Cluster,
			"Wrong cluster in database %s from %s", dbName, sqlFile)
		require.Equal(t, expectedDB.Engine, actualDB.Engine,
			"Wrong engine in database %s from %s", dbName, sqlFile)
		require.Equal(t, expectedDB.Comment, actualDB.Comment,
			"Wrong comment in database %s from %s", dbName, sqlFile)
	}

	// Check dictionaries
	actualDictionaries := actualTestCase.Dictionaries
	expectedDictionaries := expected.Dictionaries

	// Handle nil vs empty map cases
	if actualDictionaries == nil {
		actualDictionaries = make(map[string]ExpectedDictionary)
	}
	if expectedDictionaries == nil {
		expectedDictionaries = make(map[string]ExpectedDictionary)
	}

	require.Len(t, actualDictionaries, len(expectedDictionaries),
		"Wrong number of dictionaries in %s", sqlFile)

	// Check each expected dictionary
	for dictName, expectedDict := range expectedDictionaries {
		actualDict, exists := actualDictionaries[dictName]
		require.True(t, exists, "Dictionary %s not found in %s", dictName, sqlFile)

		// Verify dictionary properties
		require.Equal(t, expectedDict.Name, actualDict.Name,
			"Wrong dictionary name in %s", sqlFile)
		require.Equal(t, expectedDict.Database, actualDict.Database,
			"Wrong database in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.Cluster, actualDict.Cluster,
			"Wrong cluster in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.Operation, actualDict.Operation,
			"Wrong operation in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.OrReplace, actualDict.OrReplace,
			"Wrong OR REPLACE flag in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.IfNotExists, actualDict.IfNotExists,
			"Wrong IF NOT EXISTS flag in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.IfExists, actualDict.IfExists,
			"Wrong IF EXISTS flag in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.Permanently, actualDict.Permanently,
			"Wrong PERMANENTLY flag in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.Sync, actualDict.Sync,
			"Wrong SYNC flag in dictionary %s from %s", dictName, sqlFile)
		require.Equal(t, expectedDict.Comment, actualDict.Comment,
			"Wrong comment in dictionary %s from %s", dictName, sqlFile)
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
