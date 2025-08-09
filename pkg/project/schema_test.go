package project_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/consts"
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

		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), configContent, consts.ModeFile))

		// Copy test schema files
		require.NoError(t, copyDir("testdata/db", filepath.Join(tmpDir, "db")))

		// Initialize project
		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		return p
	}

	t.Run("parses simple schema without imports", func(t *testing.T) {
		p := setupTestProject(t)

		grammar, err := p.ParseSchema()
		require.NoError(t, err)
		require.NotNil(t, grammar)

		// Verify parsed content (using the main.sql entrypoint)
		require.Len(t, grammar.Statements, 3) // 1 database + 2 tables

		// Check first statement is CREATE DATABASE
		require.NotNil(t, grammar.Statements[0].CreateDatabase)
		require.Equal(t, "test_db", grammar.Statements[0].CreateDatabase.Name)

		// Check tables exist
		require.NotNil(t, grammar.Statements[1].CreateTable)
		require.NotNil(t, grammar.Statements[2].CreateTable)
	})

	t.Run("parses schema with imports", func(t *testing.T) {
		// Create a custom project to test import functionality
		tmpDir := t.TempDir()

		// Create config pointing to a schema with imports
		configContent := `clickhouse:
  version: "25.7"
  config_dir: "db/config.d"
  cluster: "cluster"
entrypoint: db/with_imports.sql
dir: db/migrations`

		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), []byte(configContent), consts.ModeFile))
		require.NoError(t, copyDir("testdata/db", filepath.Join(tmpDir, "db")))

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		grammar, err := p.ParseSchema()
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

	t.Run("parses different schema file", func(t *testing.T) {
		// Create a custom project to test a different schema file
		tmpDir := t.TempDir()

		// Create config pointing to staging schema
		configContent := `clickhouse:
  version: "25.7"
  config_dir: "db/config.d"  
  cluster: "cluster"
entrypoint: db/staging.sql
dir: db/migrations`

		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), []byte(configContent), consts.ModeFile))
		require.NoError(t, copyDir("testdata/db", filepath.Join(tmpDir, "db")))

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		grammar, err := p.ParseSchema()
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

	t.Run("returns error for invalid configuration", func(t *testing.T) {
		// Create project with empty config
		tmpDir := t.TempDir()

		configContent := `clickhouse:
  version: "25.7"
entrypoint: ""
dir: ""`

		require.NoError(t, os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), []byte(configContent), consts.ModeFile))

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		_, err := p.ParseSchema()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load schema from")
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create config with non-existent entrypoint
		configContent := `entrypoint: nonexistent/file.sql
dir: db/migrations`

		err := os.WriteFile(filepath.Join(tmpDir, "housekeeper.yaml"), []byte(configContent), consts.ModeFile)
		require.NoError(t, err)

		p := project.New(tmpDir)
		err = p.Initialize(project.InitOptions{})
		require.NoError(t, err)

		_, err = p.ParseSchema()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load schema from")
	})

	t.Run("can parse schema multiple times", func(t *testing.T) {
		p := setupTestProject(t)

		// Test that we can parse the same schema multiple times
		grammar1, err := p.ParseSchema()
		require.NoError(t, err)
		require.NotNil(t, grammar1)

		grammar2, err := p.ParseSchema()
		require.NoError(t, err)
		require.NotNil(t, grammar2)

		// Should get the same results
		require.Len(t, grammar2.Statements, len(grammar1.Statements))
	})

	t.Run("returns error when project is not initialized", func(t *testing.T) {
		tmpDir := t.TempDir()
		p := project.New(tmpDir)

		// Don't initialize - config will be nil
		_, err := p.ParseSchema()
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
