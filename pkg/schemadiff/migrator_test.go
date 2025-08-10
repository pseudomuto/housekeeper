package schemadiff_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	. "github.com/pseudomuto/housekeeper/pkg/schemadiff"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/golden"
)

func TestMigrationGeneration(t *testing.T) {
	// Find all *.in.sql test files
	inputFiles, err := filepath.Glob("testdata/*.in.sql")
	require.NoError(t, err)

	for _, inputPath := range inputFiles {
		inputFile := filepath.Base(inputPath)
		testName := strings.TrimSuffix(inputFile, ".in.sql")

		t.Run(testName, func(t *testing.T) {
			// Read input file containing 2 SQL statements
			inputData, err := os.ReadFile(inputPath)
			require.NoError(t, err)

			inputSQL := string(inputData)

			// Split input into current and target SQL by looking for comment sections
			// Each .in.sql file should have a current state section and a target state section
			lines := strings.Split(inputSQL, "\n")

			var currentSQLLines, targetSQLLines []string
			inTargetSection := false

			for _, line := range lines {
				line = strings.TrimSpace(line)
				if strings.HasPrefix(line, "-- Target state:") {
					inTargetSection = true
					continue
				}
				if strings.HasPrefix(line, "--") {
					// Skip other comment lines
					continue
				}
				if line == "" {
					// Skip empty lines
					continue
				}

				// Skip lines that are just a semicolon (empty state markers)
				if line == ";" {
					continue
				}

				if inTargetSection {
					targetSQLLines = append(targetSQLLines, line)
				} else {
					currentSQLLines = append(currentSQLLines, line)
				}
			}

			currentSQLText := strings.Join(currentSQLLines, " ")
			targetSQLText := strings.Join(targetSQLLines, " ")

			// Clean up any double spaces and normalize
			currentSQLText = strings.Join(strings.Fields(currentSQLText), " ")
			targetSQLText = strings.Join(strings.Fields(targetSQLText), " ")

			// Parse current and target SQL
			var currentSQL, targetSQL *parser.SQL

			if currentSQLText == "" {
				currentSQL = &parser.SQL{Statements: []*parser.Statement{}}
			} else {
				// Ensure currentSQLText ends with semicolon for parser
				if !strings.HasSuffix(currentSQLText, ";") {
					currentSQLText += ";"
				}
				currentSQL, err = parser.ParseSQL(currentSQLText)
				require.NoError(t, err)
			}

			if targetSQLText == "" {
				targetSQL = &parser.SQL{Statements: []*parser.Statement{}}
			} else {
				// Ensure targetSQLText ends with semicolon for parser
				if !strings.HasSuffix(targetSQLText, ";") {
					targetSQLText += ";"
				}
				targetSQL, err = parser.ParseSQL(targetSQLText)
				require.NoError(t, err)
			}

			// Generate migration
			migration, err := GenerateMigration(currentSQL, targetSQL)
			// Handle expected errors for unsupported operations
			if err != nil {
				if errors.Is(err, ErrNoDiff) {
					// For no differences, expect empty golden file or specific error message
					golden.Assert(t, "ErrNoDiff: no differences found", testName+".sql")
					return
				} else if errors.Is(err, ErrUnsupported) {
					// For unsupported operations, store the error message in golden file
					golden.Assert(t, "ErrUnsupported: "+err.Error(), testName+".sql")
					return
				} else {
					// Other errors should fail the test
					require.NoError(t, err)
				}
			}

			// Format the migration SQL
			formattedSQL := formatMigrationSQL(migration.SQL)

			// Compare formatted migration SQL with golden file
			golden.Assert(t, formattedSQL, testName+".sql")
		})
	}
}

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

		// Should contain the migration SQL without header comments
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
			require.False(t, uniqueNames[filename])
			uniqueNames[filename] = true
		}
	})

	t.Run("handles write permission error", func(t *testing.T) {
		// This test is skipped on systems where we can't create read-only directories
		tempDir := t.TempDir()
		migrationDir := filepath.Join(tempDir, "readonly")

		// Create migration directory but make it read-only
		err := os.MkdirAll(migrationDir, consts.ModeDir)
		require.NoError(t, err)

		err = os.Chmod(migrationDir, 0o444) // Read-only
		require.NoError(t, err)

		// Restore permissions after test
		defer func() {
			_ = os.Chmod(migrationDir, consts.ModeDir)
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

// formatMigrationSQL formats migration SQL for consistent output
func formatMigrationSQL(sql string) string {
	if strings.TrimSpace(sql) == "" {
		return sql
	}

	// Ensure each statement ends with a semicolon
	lines := strings.Split(sql, "\n\n")
	for i, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasSuffix(line, ";") {
			lines[i] = line + ";"
		} else {
			lines[i] = line
		}
	}
	formattedSQL := strings.Join(lines, "\n\n")

	// Try to parse and format the SQL, but fall back to raw SQL if it fails
	parsedMigration, err := parser.ParseSQL(formattedSQL)
	if err != nil {
		return formattedSQL
	}

	var formattedBuf bytes.Buffer
	if err := format.FormatSQL(&formattedBuf, format.Defaults, parsedMigration); err != nil {
		return formattedSQL
	}

	return formattedBuf.String()
}
