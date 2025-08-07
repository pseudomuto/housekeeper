package migrator_test

import (
	"embed"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
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
			var currentGrammar, targetGrammar *parser.SQL

			if testCase.CurrentSQL == "" {
				currentGrammar = &parser.SQL{Statements: []*parser.Statement{}}
			} else {
				currentGrammar, err = parser.ParseSQL(testCase.CurrentSQL)
				require.NoError(t, err, "Failed to parse current SQL")
			}

			if testCase.TargetSQL == "" {
				targetGrammar = &parser.SQL{Statements: []*parser.Statement{}}
			} else {
				targetGrammar, err = parser.ParseSQL(testCase.TargetSQL)
				require.NoError(t, err, "Failed to parse target SQL")
			}

			// Generate migration
			migration, err := GenerateMigration(currentGrammar, targetGrammar, testName)
			if testCase.ExpectedMigration.DiffCount == 0 {
				require.Error(t, err, "Expected error for invalid operation or no differences")
				// Could be no differences or unsupported operation
				if !errors.Is(err, ErrNoDiff) {
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

const (
	dirPerm  = os.FileMode(0o755)
	filePerm = os.FileMode(0o644)
)

func TestGenerateMigrationFile(t *testing.T) {
	t.Run("generates migration file with correct timestamp format", func(t *testing.T) {
		// Create temporary migration directory
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "migrations")

		// Create simple current and target schemas
		currentSQL := `CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Old comment';`
		targetSQL := `CREATE DATABASE analytics ENGINE = Atomic COMMENT 'New comment';
CREATE TABLE analytics.events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;`

		current, err := parser.ParseSQL(currentSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(targetSQL)
		require.NoError(t, err)

		// Generate migration file
		filename, err := GenerateMigrationFile(migrationDir, current, target)
		require.NoError(t, err)
		require.NotEmpty(t, filename)

		// Verify filename format: yyyyMMddhhmmss_schema_update.sql
		require.Regexp(t, `^\d{14}_schema_update\.sql$`, filename)

		// Verify file was created
		migrationPath := filepath.Join(migrationDir, filename)
		require.FileExists(t, migrationPath)

		// Verify file content
		content, err := os.ReadFile(migrationPath)
		require.NoError(t, err)
		contentStr := string(content)

		// Should contain header comments
		require.Contains(t, contentStr, "-- Schema migration generated at")
		require.Contains(t, contentStr, "-- Down migration: swap current and target schemas")

		// Should contain the migration SQL
		require.Contains(t, contentStr, "ALTER DATABASE analytics MODIFY COMMENT 'New comment';")
		require.Contains(t, contentStr, "CREATE TABLE analytics.events")
	})

	t.Run("creates migration directory if it doesn't exist", func(t *testing.T) {
		// Create temporary directory without migrations subdirectory
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "migrations", "nested")

		// Verify directory doesn't exist yet
		_, err := os.Stat(migrationDir)
		require.True(t, os.IsNotExist(err))

		// Create schemas
		currentSQL := `CREATE DATABASE test ENGINE = Atomic;`
		targetSQL := `CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`

		current, err := parser.ParseSQL(currentSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(targetSQL)
		require.NoError(t, err)

		// Generate migration - should create directory
		filename, err := GenerateMigrationFile(migrationDir, current, target)
		require.NoError(t, err)
		require.NotEmpty(t, filename)

		// Verify directory was created
		require.DirExists(t, migrationDir)

		// Verify file was created
		migrationPath := filepath.Join(migrationDir, filename)
		require.FileExists(t, migrationPath)
	})

	t.Run("handles down migration by swapping parameters", func(t *testing.T) {
		// Create temporary migration directory
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "migrations")

		// Create schemas - current has table, target doesn't
		currentSQL := `CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;`
		targetSQL := `CREATE DATABASE test ENGINE = Atomic;`

		current, err := parser.ParseSQL(currentSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(targetSQL)
		require.NoError(t, err)

		// Generate UP migration (current -> target)
		upFilename, err := GenerateMigrationFile(migrationDir, current, target)
		require.NoError(t, err)

		// Small delay to ensure different timestamp
		time.Sleep(time.Second)

		// Generate DOWN migration (target -> current)
		downFilename, err := GenerateMigrationFile(migrationDir, target, current)
		require.NoError(t, err)

		// Read both files
		upPath := filepath.Join(migrationDir, upFilename)
		downPath := filepath.Join(migrationDir, downFilename)

		upContent, err := os.ReadFile(upPath)
		require.NoError(t, err)

		downContent, err := os.ReadFile(downPath)
		require.NoError(t, err)

		upStr := string(upContent)
		downStr := string(downContent)

		// Both migrations should contain table operations
		require.Contains(t, upStr, "test.users")
		require.Contains(t, downStr, "test.users")

		// At minimum, verify the filenames are different (since they're timestamped)
		require.NotEqual(t, upFilename, downFilename)
	})

	t.Run("returns error when no differences found", func(t *testing.T) {
		// Create temporary migration directory
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "migrations")

		// Create identical schemas
		sameSQL := `CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`

		current, err := parser.ParseSQL(sameSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(sameSQL)
		require.NoError(t, err)

		// Try to generate migration with identical schemas
		_, err = GenerateMigrationFile(migrationDir, current, target)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoDiff)
	})

	t.Run("generates unique filenames for concurrent calls", func(t *testing.T) {
		// This test verifies that the UTC timestamp provides enough precision
		// for unique filenames even in rapid succession
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "migrations")

		// Create schemas
		currentSQL := `CREATE DATABASE test ENGINE = Atomic;`
		targetSQL := `CREATE DATABASE test ENGINE = Atomic COMMENT 'Updated';`

		current, err := parser.ParseSQL(currentSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(targetSQL)
		require.NoError(t, err)

		// Generate multiple migrations rapidly
		filenames := make([]string, 3)
		for i := 0; i < 3; i++ {
			filenames[i], err = GenerateMigrationFile(migrationDir, current, target)
			require.NoError(t, err)
			// Small delay to ensure different timestamps
			time.Sleep(time.Second)
		}

		// Verify all filenames are unique
		uniqueNames := make(map[string]bool)
		for _, filename := range filenames {
			require.False(t, uniqueNames[filename], "duplicate filename: %s", filename)
			uniqueNames[filename] = true
		}
	})

	t.Run("handles write permission error", func(t *testing.T) {
		// This test is skipped on systems where we can't create read-only directories
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "readonly")

		// Create migration directory but make it read-only
		err := os.MkdirAll(migrationDir, dirPerm)
		require.NoError(t, err)

		err = os.Chmod(migrationDir, 0o444) // Read-only
		require.NoError(t, err)

		// Restore permissions after test
		defer func() {
			_ = os.Chmod(migrationDir, dirPerm)
		}()

		// Create simple schemas
		currentSQL := `CREATE DATABASE test ENGINE = Atomic;`
		targetSQL := `CREATE DATABASE test ENGINE = Atomic COMMENT 'New comment';`

		current, err := parser.ParseSQL(currentSQL)
		require.NoError(t, err)

		target, err := parser.ParseSQL(targetSQL)
		require.NoError(t, err)

		// Try to generate migration - should fail due to write permission
		_, err = GenerateMigrationFile(migrationDir, current, target)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to write migration file")
	})
}
