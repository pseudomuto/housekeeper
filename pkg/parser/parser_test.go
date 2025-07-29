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
	Databases    map[string]ExpectedDatabase    `yaml:"databases,omitempty"`
	Dictionaries map[string]ExpectedDictionary  `yaml:"dictionaries,omitempty"`
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
	Operation   string `yaml:"operation"`     // CREATE, ATTACH, DETACH, DROP
	IfNotExists bool   `yaml:"if_not_exists"`
	IfExists    bool   `yaml:"if_exists"`
	Permanently bool   `yaml:"permanently"`
	Sync        bool   `yaml:"sync"`
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
				OrReplace:   dict.OrReplace != nil,
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
