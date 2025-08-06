package project_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
)

func TestProjectParseSchema(t *testing.T) {
	// Setup a test project with our test fixtures
	setupTestProject := func(t *testing.T) *project.Project {
		t.Helper()
		tmpDir := t.TempDir()

		// Copy test config to temp directory
		configContent, err := os.ReadFile("testdata/housekeeper.yaml")
		require.NoError(t, err)

		err = os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), configContent, filePerms)
		require.NoError(t, err)

		// Copy test schema files
		err = copyDir("testdata/db", filepath.Join(tmpDir, "db"))
		require.NoError(t, err)

		// Initialize project
		p := project.New(tmpDir)
		err = p.Initialize()
		require.NoError(t, err)

		return p
	}

	t.Run("parses simple schema without imports", func(t *testing.T) {
		p := setupTestProject(t)

		grammar, err := p.ParseSchema("test")
		require.NoError(t, err)
		require.NotNil(t, grammar)

		// Verify parsed content
		require.Len(t, grammar.Statements, 3) // 1 database + 2 tables

		// Check first statement is CREATE DATABASE
		require.NotNil(t, grammar.Statements[0].CreateDatabase)
		require.Equal(t, "test_db", grammar.Statements[0].CreateDatabase.Name)

		// Check tables exist
		require.NotNil(t, grammar.Statements[1].CreateTable)
		require.NotNil(t, grammar.Statements[2].CreateTable)
	})

	t.Run("parses schema with imports", func(t *testing.T) {
		p := setupTestProject(t)

		grammar, err := p.ParseSchema("imports")
		require.NoError(t, err)
		require.NotNil(t, grammar)

		// Should have database + 2 imported tables
		require.Len(t, grammar.Statements, 3)

		// Check database
		require.NotNil(t, grammar.Statements[0].CreateDatabase)
		require.Equal(t, "imports_db", grammar.Statements[0].CreateDatabase.Name)

		// Check imported tables
		require.NotNil(t, grammar.Statements[1].CreateTable)
		require.Equal(t, "users", grammar.Statements[1].CreateTable.Name)

		require.NotNil(t, grammar.Statements[2].CreateTable)
		require.Equal(t, "products", grammar.Statements[2].CreateTable.Name)
	})

	t.Run("parses different environment", func(t *testing.T) {
		p := setupTestProject(t)

		grammar, err := p.ParseSchema("staging")
		require.NoError(t, err)
		require.NotNil(t, grammar)

		require.Len(t, grammar.Statements, 2) // 1 database + 1 table

		// Check database name is staging_db
		require.NotNil(t, grammar.Statements[0].CreateDatabase)
		require.Equal(t, "staging_db", grammar.Statements[0].CreateDatabase.Name)

		// Check events table
		require.NotNil(t, grammar.Statements[1].CreateTable)
		require.Equal(t, "events", grammar.Statements[1].CreateTable.Name)
	})

	t.Run("returns error for non-existent environment", func(t *testing.T) {
		p := setupTestProject(t)

		_, err := p.ParseSchema("nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Env not found: nonexistent")
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create config with non-existent entrypoint
		configContent := `environments:
  - name: missing
    url: clickhouse://localhost:9000/test
    entrypoint: nonexistent/file.sql`

		err := os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), []byte(configContent), filePerms)
		require.NoError(t, err)

		p := project.New(tmpDir)
		err = p.Initialize()
		require.NoError(t, err)

		_, err = p.ParseSchema("missing")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load schema from")
	})

	t.Run("handles case insensitive environment names", func(t *testing.T) {
		p := setupTestProject(t)

		// Test with different cases
		_, err := p.ParseSchema("TEST")
		require.NoError(t, err)

		_, err = p.ParseSchema("Test")
		require.NoError(t, err)

		_, err = p.ParseSchema("STAGING")
		require.NoError(t, err)
	})

	t.Run("returns error when project is not initialized", func(t *testing.T) {
		tmpDir := t.TempDir()
		p := project.New(tmpDir)

		// Don't initialize - config will be nil
		_, err := p.ParseSchema("test")
		require.Error(t, err)
		require.Contains(t, err.Error(), "project not initialized")
	})
}

// copyDir recursively copies a directory and its contents
func copyDir(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		relPath, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, relPath)

		if info.IsDir() {
			return os.MkdirAll(dstPath, info.Mode())
		}

		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		return os.WriteFile(dstPath, data, info.Mode())
	})
}
