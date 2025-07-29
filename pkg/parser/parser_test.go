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
	Databases map[string]ExpectedDatabase `yaml:"databases"`
}

// ExpectedDatabase represents expected database properties
type ExpectedDatabase struct {
	Name    string `yaml:"name"`
	Cluster string `yaml:"cluster"`
	Engine  string `yaml:"engine"`
	Comment string `yaml:"comment"`
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

	return TestCase{Databases: expectedDatabases}
}

func verifyGrammar(t *testing.T, actualGrammar *Grammar, expected TestCase, sqlFile string) {
	// Use the same logic as generateTestCaseFromGrammar to extract actual results
	actualTestCase := generateTestCaseFromGrammar(actualGrammar)
	actualDatabases := actualTestCase.Databases

	// Check database count
	require.Len(t, actualDatabases, len(expected.Databases),
		"Wrong number of databases in %s", sqlFile)

	// Check each expected database
	for dbName, expectedDB := range expected.Databases {
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
}
