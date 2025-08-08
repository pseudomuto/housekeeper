package project_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
)

const (
	dirPerms  = os.FileMode(0o755)
	filePerms = os.FileMode(0o644)
)

func TestProjectInitialize(t *testing.T) {
	t.Run("creates all missing directories and files", func(t *testing.T) {
		tmpDir := t.TempDir()

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Verify directories were created)
		assertDirExists(t, filepath.Join(tmpDir, "db"))
		assertDirExists(t, filepath.Join(tmpDir, "db", "migrations"))
		assertDirExists(t, filepath.Join(tmpDir, "db", "migrations", "dev"))
		assertDirExists(t, filepath.Join(tmpDir, "db", "schemas"))
		assertDirExists(t, filepath.Join(tmpDir, "db", "config.d")) // ClickHouse config directory

		// Verify files were created
		assertFileExists(t, filepath.Join(tmpDir, "db", "main.sql"))
		assertFileExists(t, filepath.Join(tmpDir, "housekeeper.yaml"))

		// Verify file contents are not empty
		mainSQL, err := os.ReadFile(filepath.Join(tmpDir, "db", "main.sql"))
		require.NoError(t, err)
		require.NotEmpty(t, mainSQL)

		configYAML, err := os.ReadFile(filepath.Join(tmpDir, "housekeeper.yaml"))
		require.NoError(t, err)
		require.NotEmpty(t, configYAML)
	})

	t.Run("preserves existing files", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create an existing file with custom content
		existingContent := []byte("environments: ~")
		housekeeperPath := filepath.Join(tmpDir, "housekeeper.yaml")
		require.NoError(t, os.WriteFile(housekeeperPath, existingContent, filePerms))

		// Initialize the project
		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Verify the existing file was not overwritten
		content, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)
		require.Equal(t, existingContent, content)
	})

	t.Run("preserves existing directories", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create an existing directory with a custom file
		dbDir := filepath.Join(tmpDir, "db")
		require.NoError(t, os.MkdirAll(dbDir, dirPerms))

		customFile := filepath.Join(dbDir, "custom.sql")
		require.NoError(t, os.WriteFile(customFile, []byte("custom"), filePerms))

		// Initialize the project
		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Verify the custom file still exists
		assertFileExists(t, customFile)
		content, err := os.ReadFile(customFile)
		require.NoError(t, err)
		require.Equal(t, []byte("custom"), content)

		// Verify default files were also created
		assertFileExists(t, filepath.Join(dbDir, "main.sql"))
	})

	t.Run("is idempotent", func(t *testing.T) {
		tmpDir := t.TempDir()

		p := project.New(tmpDir)

		// First initialization
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Modify a file
		housekeeperPath := filepath.Join(tmpDir, "housekeeper.yaml")
		originalContent, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)

		modifiedContent := append(originalContent, []byte("\n# Custom comment")...)
		require.NoError(t, os.WriteFile(housekeeperPath, modifiedContent, filePerms))

		// Second initialization
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Verify the modified file was not overwritten
		content, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)
		require.Equal(t, modifiedContent, content)
	})

	t.Run("creates nested directories when only file is missing", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create only the top-level db directory
		dbDir := filepath.Join(tmpDir, "db")
		err := os.MkdirAll(dbDir, dirPerms)
		require.NoError(t, err)

		p := project.New(tmpDir)
		err = p.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify nested directories were created for the file
		assertDirExists(t, filepath.Join(dbDir, "migrations"))
		assertDirExists(t, filepath.Join(dbDir, "migrations", "dev"))
		assertDirExists(t, filepath.Join(dbDir, "schemas"))
		assertFileExists(t, filepath.Join(dbDir, "main.sql"))
	})

	t.Run("returns error if root is not a directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "not_a_dir")

		// Create a file instead of directory
		err := os.WriteFile(filePath, []byte("content"), filePerms)
		require.NoError(t, err)

		p := project.New(filePath)
		err = p.Initialize(project.InitOptions{})
		require.Error(t, err)
		// The error can be either from ensureDirectory or from trying to create subdirectories
		require.True(t,
			contains(err.Error(), "is not a directory") ||
				contains(err.Error(), "not a directory"),
			"error should indicate that path is not a directory: %v", err)
	})

	t.Run("returns error if root does not exist", func(t *testing.T) {
		nonExistentPath := "/non/existent/path"

		p := project.New(nonExistentPath)
		err := p.Initialize(project.InitOptions{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to stat dir")
	})

	t.Run("handles permission errors gracefully", func(t *testing.T) {
		if os.Getuid() == 0 {
			t.Skip("Cannot test permission errors as root")
		}

		tmpDir := t.TempDir()

		// Create a directory with no write permissions
		readOnlyDir := filepath.Join(tmpDir, "readonly")
		err := os.MkdirAll(readOnlyDir, os.FileMode(0o555))
		require.NoError(t, err)

		p := project.New(readOnlyDir)
		err = p.Initialize(project.InitOptions{})
		require.Error(t, err)
		// The error should be about failing to create a directory or file
		require.Contains(t, err.Error(), "failed to")
	})

	t.Run("creates custom ClickHouse config directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create custom config with different config dir
		configContent := `
clickhouse:
  version: "24.8"
  config_dir: "custom/clickhouse"
  cluster: "test-cluster"
environments:
  - name: dev
    dev: docker://clickhouse:9000/dev
    url: clickhouse://localhost:9000/prod
    entrypoint: db/main.sql
    dir: db/migrations
`
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		require.NoError(t, os.WriteFile(configPath, []byte(configContent), filePerms))

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(project.InitOptions{}))

		// Verify custom config directory was created
		assertDirExists(t, filepath.Join(tmpDir, "custom", "clickhouse"))
	})

	t.Run("initializes with custom cluster name", func(t *testing.T) {
		tmpDir := t.TempDir()

		options := project.InitOptions{
			Cluster: "production",
		}

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(options))

		// Verify config file was created and updated with custom cluster
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		assertFileExists(t, configPath)

		// Load and verify the config contains the custom cluster
		config, err := project.LoadConfigFile(configPath)
		require.NoError(t, err)
		require.Equal(t, "production", config.ClickHouse.Cluster)
		require.Equal(t, project.DefaultClickHouseVersion, config.ClickHouse.Version)
		require.Equal(t, project.DefaultClickHouseConfigDir, config.ClickHouse.ConfigDir)
	})

	t.Run("initializes with default cluster when not specified", func(t *testing.T) {
		tmpDir := t.TempDir()

		options := project.InitOptions{
			Cluster: "", // Empty cluster should use default
		}

		p := project.New(tmpDir)
		require.NoError(t, p.Initialize(options))

		// Verify config file was created with default cluster
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		config, err := project.LoadConfigFile(configPath)
		require.NoError(t, err)
		require.Equal(t, project.DefaultClickHouseCluster, config.ClickHouse.Cluster)
		require.Equal(t, "cluster", config.ClickHouse.Cluster)
	})
}

func assertDirExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.True(t, info.IsDir(), "path should be a directory: %s", path)
}

func assertFileExists(t *testing.T, path string) {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.False(t, info.IsDir(), "path should be a file: %s", path)
}

func contains(s, substr string) bool {
	return strings.Contains(s, substr)
}
