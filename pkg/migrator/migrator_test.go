package migrator_test

import (
	"embed"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

//go:embed testdata/*.yaml
var testdataFS embed.FS

type (
	// MigrationTestCase represents expected results for a migration test
	MigrationTestCase struct {
		Description       string                  `yaml:"description"`
		CurrentSQL        string                  `yaml:"current_sql"`
		TargetSQL         string                  `yaml:"target_sql"`
		ExpectedMigration ExpectedMigrationResult `yaml:"expected_migration"`
	}

	// ExpectedMigrationResult represents expected migration properties
	ExpectedMigrationResult struct {
		UpContains   []string `yaml:"up_contains"`
		DownContains []string `yaml:"down_contains"`
		DiffCount    int      `yaml:"diff_count"`
		DiffTypes    []string `yaml:"diff_types"`
	}
)

func TestMigrationGeneration(t *testing.T) {
	// Find all YAML test files in embedded testdata
	yamlFiles, err := fs.Glob(testdataFS, "testdata/*.yaml")
	require.NoError(t, err, "Failed to find YAML test files")

	// Run each test case
	for _, yamlPath := range yamlFiles {
		yamlFile := filepath.Base(yamlPath)
		testName := strings.TrimSuffix(yamlFile, ".yaml")

		t.Run(testName, func(t *testing.T) {
			// Read YAML test case
			yamlData, err := testdataFS.ReadFile(yamlPath)
			require.NoError(t, err, "Failed to read YAML file: %s", yamlFile)

			var testCase MigrationTestCase
			err = yaml.Unmarshal(yamlData, &testCase)
			require.NoError(t, err, "Failed to parse YAML file: %s", yamlFile)

			// Parse current and target SQL
			var currentGrammar, targetGrammar *parser.Grammar

			if testCase.CurrentSQL == "" {
				currentGrammar = &parser.Grammar{Statements: []*parser.Statement{}}
			} else {
				currentGrammar, err = parser.ParseSQL(testCase.CurrentSQL)
				require.NoError(t, err, "Failed to parse current SQL")
			}

			if testCase.TargetSQL == "" {
				targetGrammar = &parser.Grammar{Statements: []*parser.Statement{}}
			} else {
				targetGrammar, err = parser.ParseSQL(testCase.TargetSQL)
				require.NoError(t, err, "Failed to parse target SQL")
			}

			// Generate migration
			migration, err := GenerateMigration(currentGrammar, targetGrammar, testName)
			if testCase.ExpectedMigration.DiffCount == 0 {
				require.Error(t, err, "Expected error for invalid operation or no differences")
				// Could be no differences or unsupported operation
				if !strings.Contains(err.Error(), "no differences found") {
					// If not "no differences", it should be an unsupported operation error
					require.ErrorIs(t, err, ErrUnsupported,
						"Expected unsupported operation error, got: %s", err.Error())
				}
				return
			}

			require.NoError(t, err, "Failed to generate migration")

			// Verify migration contents
			verifyMigrationResult(t, migration, testCase.ExpectedMigration, testName)
		})
	}
}

func verifyMigrationResult(t *testing.T, migration *Migration, expected ExpectedMigrationResult, testName string) {
	// Verify UP migration contains expected statements
	for _, expectedContent := range expected.UpContains {
		require.Contains(t, migration.Up, expectedContent,
			"UP migration missing expected content in %s", testName)
	}

	// Verify DOWN migration contains expected statements
	for _, expectedContent := range expected.DownContains {
		require.Contains(t, migration.Down, expectedContent,
			"DOWN migration missing expected content in %s", testName)
	}

	// Verify migration metadata
	require.NotEmpty(t, migration.Version, "Migration version should not be empty")
	require.Equal(t, testName, migration.Name, "Migration name mismatch")
	require.NotEmpty(t, migration.Up, "UP migration should not be empty")
	require.NotEmpty(t, migration.Down, "DOWN migration should not be empty")
}
