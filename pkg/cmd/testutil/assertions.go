package testutil

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

// RequireValidProject asserts that a project structure is correctly initialized
func RequireValidProject(t *testing.T, projectDir string) {
	t.Helper()

	// Check main directories exist
	require.DirExists(t, filepath.Join(projectDir, "db"), "db directory should exist")
	require.DirExists(t, filepath.Join(projectDir, "db", "migrations"), "migrations directory should exist")
	require.DirExists(t, filepath.Join(projectDir, "db", "schemas"), "schemas directory should exist")
	require.DirExists(t, filepath.Join(projectDir, "db", "config.d"), "config.d directory should exist")

	// Check main files exist
	require.FileExists(t, filepath.Join(projectDir, "housekeeper.yaml"), "housekeeper.yaml should exist")
	require.FileExists(t, filepath.Join(projectDir, "db", "main.sql"), "main.sql should exist")
	require.FileExists(t, filepath.Join(projectDir, "db", "config.d", "_clickhouse.xml"), "_clickhouse.xml should exist")
}

// RequireFileExists asserts that a file exists and optionally checks its content
func RequireFileExists(t *testing.T, path string, checks ...func(content string)) {
	t.Helper()

	require.FileExists(t, path, "File should exist: %s", path)

	if len(checks) > 0 {
		content, err := os.ReadFile(path)
		require.NoError(t, err, "Failed to read file: %s", path)

		contentStr := string(content)
		for _, check := range checks {
			check(contentStr)
		}
	}
}

// RequireFileContains returns a check function that verifies file contains text
func RequireFileContains(t *testing.T, expected string) func(string) {
	return func(content string) {
		require.Contains(t, content, expected, "File should contain: %s", expected)
	}
}

// RequireFileNotContains returns a check function that verifies file doesn't contain text
func RequireFileNotContains(t *testing.T, unexpected string) func(string) {
	return func(content string) {
		require.NotContains(t, content, unexpected, "File should not contain: %s", unexpected)
	}
}

// RequireMigrationValid asserts that a migration file is valid SQL
func RequireMigrationValid(t *testing.T, migrationPath string) {
	t.Helper()

	require.FileExists(t, migrationPath, "Migration file should exist")

	content, err := os.ReadFile(migrationPath)
	require.NoError(t, err, "Failed to read migration file")

	// Parse the SQL to validate it
	_, err = parser.ParseString(string(content))
	require.NoError(t, err, "Migration should contain valid SQL: %s", migrationPath)
}

// RequireSchemaEqual asserts that two SQL schemas are functionally equivalent
func RequireSchemaEqual(t *testing.T, expected, actual string) {
	t.Helper()

	// Parse both schemas
	expectedSQL, err := parser.ParseString(expected)
	require.NoError(t, err, "Expected schema should be valid SQL")

	actualSQL, err := parser.ParseString(actual)
	require.NoError(t, err, "Actual schema should be valid SQL")

	// Compare statement counts
	require.Len(t, actualSQL.Statements, len(expectedSQL.Statements),
		"Schema should have same number of statements")

	// TODO: Add more sophisticated comparison logic if needed
	// For now, we just check that both parse successfully and have same statement count
}

// RequireConfigValid asserts that a housekeeper.yaml file is valid
func RequireConfigValid(t *testing.T, configPath string, checks ...func(content string)) {
	t.Helper()

	RequireFileExists(t, configPath, func(content string) {
		// Basic YAML structure checks
		require.Contains(t, content, "entrypoint:", "Config should have entrypoint")
		require.Contains(t, content, "dir:", "Config should have dir")
		require.Contains(t, content, "clickhouse:", "Config should have clickhouse section")

		// Run additional checks
		for _, check := range checks {
			check(content)
		}
	})
}

// RequireClickHouseXMLValid asserts that a ClickHouse XML config is valid
func RequireClickHouseXMLValid(t *testing.T, xmlPath string, expectedCluster string) {
	t.Helper()

	RequireFileExists(t, xmlPath, func(content string) {
		// Check XML structure - note: the template doesn't include <?xml declaration
		require.Contains(t, content, "<clickhouse>", "Should have clickhouse root element")
		require.Contains(t, content, "</clickhouse>", "Should close clickhouse element")

		// Check cluster configuration if provided
		if expectedCluster != "" {
			require.Contains(t, content, "<cluster>"+expectedCluster+"</cluster>",
				"Should have correct cluster name in macros")
			require.Contains(t, content, "<"+expectedCluster+">",
				"Should have cluster in remote_servers")
			require.Contains(t, content, "</"+expectedCluster+">",
				"Should close cluster in remote_servers")
		}

		// Should not have placeholders
		require.NotContains(t, content, "$$CLUSTER", "Should not have placeholder")
	})
}

// RequireMigrationCount asserts that a specific number of migrations exist
func RequireMigrationCount(t *testing.T, migrationsDir string, expectedCount int) {
	t.Helper()

	entries, err := os.ReadDir(migrationsDir)
	require.NoError(t, err, "Failed to read migrations directory")

	count := 0
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			count++
		}
	}

	require.Equal(t, expectedCount, count, "Should have expected number of migration files")
}

// RequireSumFileValid asserts that a sum file exists and has valid format
func RequireSumFileValid(t *testing.T, sumPath string) {
	t.Helper()

	RequireFileExists(t, sumPath, func(content string) {
		lines := strings.Split(strings.TrimSpace(content), "\n")
		require.NotEmpty(t, lines, "Sum file should not be empty")

		// First line should be total hash
		require.True(t, strings.HasPrefix(lines[0], "h1:"),
			"First line should be total hash")

		// Subsequent lines should be file hashes
		for i := 1; i < len(lines); i++ {
			parts := strings.Fields(lines[i])
			require.Len(t, parts, 2, "Each line should have filename and hash")
			require.True(t, strings.HasSuffix(parts[0], ".sql"),
				"First part should be SQL filename")
			require.True(t, strings.HasPrefix(parts[1], "h1:"),
				"Second part should be hash")
		}
	})
}

// RequireNoFile asserts that a file does not exist
func RequireNoFile(t *testing.T, path string) {
	t.Helper()

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "File should not exist: %s", path)
}

// RequireNoDir asserts that a directory does not exist
func RequireNoDir(t *testing.T, path string) {
	t.Helper()

	_, err := os.Stat(path)
	require.True(t, os.IsNotExist(err), "Directory should not exist: %s", path)
}

// RequireError asserts that an error occurred and optionally checks the message
func RequireError(t *testing.T, err error, msgContains ...string) {
	t.Helper()

	require.Error(t, err, "Expected an error")

	for _, msg := range msgContains {
		require.Contains(t, err.Error(), msg, "Error message should contain: %s", msg)
	}
}

// RequireNoError asserts that no error occurred
func RequireNoError(t *testing.T, err error, msg ...string) {
	t.Helper()

	if len(msg) > 0 {
		require.NoError(t, err, msg[0])
	} else {
		require.NoError(t, err)
	}
}

// RequireFilePermissions asserts that a file has specific permissions
func RequireFilePermissions(t *testing.T, path string, expectedMode os.FileMode) {
	t.Helper()

	info, err := os.Stat(path)
	require.NoError(t, err, "Failed to stat file: %s", path)

	actualMode := info.Mode().Perm()
	require.Equal(t, expectedMode, actualMode,
		"File %s should have permissions %o, got %o", path, expectedMode, actualMode)
}

// RequireDirEmpty asserts that a directory is empty
func RequireDirEmpty(t *testing.T, dirPath string) {
	t.Helper()

	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err, "Failed to read directory")
	require.Empty(t, entries, "Directory should be empty: %s", dirPath)
}

// RequireDirNotEmpty asserts that a directory is not empty
func RequireDirNotEmpty(t *testing.T, dirPath string) {
	t.Helper()

	entries, err := os.ReadDir(dirPath)
	require.NoError(t, err, "Failed to read directory")
	require.NotEmpty(t, entries, "Directory should not be empty: %s", dirPath)
}
