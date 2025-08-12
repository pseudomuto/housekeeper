package project_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
)

func TestProjectInitialize_CreatesDirectoriesAndFiles(t *testing.T) {
	t.Run("creates all missing directories and files", func(t *testing.T) {
		tmpDir := t.TempDir()

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify directories were created)
		require.DirExists(t, filepath.Join(tmpDir, "db"))
		require.DirExists(t, filepath.Join(tmpDir, "db", "migrations"))
		require.DirExists(t, filepath.Join(tmpDir, "db", "schemas"))
		require.DirExists(t, filepath.Join(tmpDir, "db", "config.d")) // ClickHouse config directory

		// Verify files were created
		require.FileExists(t, filepath.Join(tmpDir, "db", "main.sql"))
		require.FileExists(t, filepath.Join(tmpDir, "housekeeper.yaml"))
		require.FileExists(t, filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml"))

		// Verify file contents are not empty
		mainSQL, err := os.ReadFile(filepath.Join(tmpDir, "db", "main.sql"))
		require.NoError(t, err)
		require.NotEmpty(t, mainSQL)

		configYAML, err := os.ReadFile(filepath.Join(tmpDir, "housekeeper.yaml"))
		require.NoError(t, err)
		require.NotEmpty(t, configYAML)

		// Verify ClickHouse XML file is not empty and has default cluster
		clickhouseXML, err := os.ReadFile(filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml"))
		require.NoError(t, err)
		require.NotEmpty(t, clickhouseXML)
		// Should have default cluster name
		require.Contains(t, string(clickhouseXML), "<cluster>cluster</cluster>")
		require.Contains(t, string(clickhouseXML), "<cluster>")
		require.Contains(t, string(clickhouseXML), "</cluster>")
		// Should NOT have placeholder
		require.NotContains(t, string(clickhouseXML), "$$CLUSTER")
	})
}

func TestProjectInitialize_PreservesExisting(t *testing.T) {
	t.Run("preserves existing files", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create an existing file with custom content
		existingContent := []byte("entrypoint: custom.sql\ndir: custom/migrations")
		housekeeperPath := filepath.Join(tmpDir, "housekeeper.yaml")
		require.NoError(t, os.WriteFile(housekeeperPath, existingContent, consts.ModeFile))

		// Create an existing ClickHouse XML with custom content
		configDir := filepath.Join(tmpDir, "db", "config.d")
		require.NoError(t, os.MkdirAll(configDir, consts.ModeDir))
		existingXMLContent := []byte("<clickhouse><custom>value</custom></clickhouse>")
		clickhouseXMLPath := filepath.Join(configDir, "_clickhouse.xml")
		require.NoError(t, os.WriteFile(clickhouseXMLPath, existingXMLContent, consts.ModeFile))

		// Initialize the project
		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify the existing file was not overwritten
		content, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)
		require.Equal(t, existingContent, content)

		// Verify the existing ClickHouse XML was not overwritten
		xmlContent, err := os.ReadFile(clickhouseXMLPath)
		require.NoError(t, err)
		require.Equal(t, existingXMLContent, xmlContent)
	})

	t.Run("preserves existing directories", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create an existing directory with a custom file
		dbDir := filepath.Join(tmpDir, "db")
		require.NoError(t, os.MkdirAll(dbDir, consts.ModeDir))

		customFile := filepath.Join(dbDir, "custom.sql")
		require.NoError(t, os.WriteFile(customFile, []byte("custom"), consts.ModeFile))

		// Initialize the project
		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify the custom file still exists
		require.FileExists(t, customFile)
		content, err := os.ReadFile(customFile)
		require.NoError(t, err)
		require.Equal(t, []byte("custom"), content)

		// Verify default files were also created
		require.FileExists(t, filepath.Join(dbDir, "main.sql"))
	})

	t.Run("is idempotent", func(t *testing.T) {
		tmpDir := t.TempDir()

		// First initialization
		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Modify a file
		housekeeperPath := filepath.Join(tmpDir, "housekeeper.yaml")
		originalContent, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)

		modifiedContent := append(originalContent, []byte("\n# Custom comment")...)
		require.NoError(t, os.WriteFile(housekeeperPath, modifiedContent, consts.ModeFile))

		// Second initialization
		proj = project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify the modified file was not overwritten
		content, err := os.ReadFile(housekeeperPath)
		require.NoError(t, err)
		require.Equal(t, modifiedContent, content)
	})

	t.Run("creates nested directories when only file is missing", func(t *testing.T) {
		tmpDir := t.TempDir()

		// Create only the top-level db directory
		dbDir := filepath.Join(tmpDir, "db")
		err := os.MkdirAll(dbDir, consts.ModeDir)
		require.NoError(t, err)

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify nested directories were created for the file
		require.DirExists(t, filepath.Join(dbDir, "migrations"))
		require.DirExists(t, filepath.Join(dbDir, "schemas"))
		require.FileExists(t, filepath.Join(dbDir, "main.sql"))
	})
}

func TestProjectInitialize_ErrorHandling(t *testing.T) {
	t.Run("returns error if root is not a directory", func(t *testing.T) {
		tmpDir := t.TempDir()
		filePath := filepath.Join(tmpDir, "not_a_dir")

		// Create a file instead of directory
		err := os.WriteFile(filePath, []byte("content"), consts.ModeFile)
		require.NoError(t, err)

		proj := project.New(project.ProjectParams{
			Dir:       filePath,
			Formatter: format.New(format.Defaults),
		})
		err = proj.Initialize(project.InitOptions{})
		require.Error(t, err)
		// The error should indicate that the path is not a directory
		require.True(t,
			strings.Contains(err.Error(), "is not a directory") ||
				strings.Contains(err.Error(), "not a directory"),
			"error should indicate that path is not a directory: %v", err)
	})

	t.Run("returns error if root does not exist", func(t *testing.T) {
		nonExistentPath := "/non/existent/path"

		proj := project.New(project.ProjectParams{
			Dir:       nonExistentPath,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to create project directory")
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

		proj := project.New(project.ProjectParams{
			Dir:       readOnlyDir,
			Formatter: format.New(format.Defaults),
		})
		err = proj.Initialize(project.InitOptions{})
		require.Error(t, err)
		// The error should be about failing to create a directory or file
		require.Contains(t, err.Error(), "failed to")
	})
}

func TestProjectInitialize_DefaultConfiguration(t *testing.T) {
	t.Run("creates default ClickHouse config directory", func(t *testing.T) {
		tmpDir := t.TempDir()

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Verify default config directory was created
		require.DirExists(t, filepath.Join(tmpDir, "db", "config.d"))

		// Verify config file was created with default values
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		require.FileExists(t, configPath)

		configContent, err := os.ReadFile(configPath)
		require.NoError(t, err)
		require.Contains(t, string(configContent), `cluster: "cluster"`)
		require.Contains(t, string(configContent), `config_dir: "db/config.d"`)
	})
}

func TestProjectInitialize_ClusterConfiguration(t *testing.T) {
	t.Run("initializes with custom cluster name", func(t *testing.T) {
		tmpDir := t.TempDir()

		options := project.InitOptions{
			Cluster: "production",
		}

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(options)
		require.NoError(t, err)

		// Verify config file was created and updated with custom cluster
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		require.FileExists(t, configPath)

		// Load and verify the config contains the custom cluster
		cfg, err := config.LoadConfigFile(configPath)
		require.NoError(t, err)
		require.Equal(t, "production", cfg.ClickHouse.Cluster)
		require.Equal(t, consts.DefaultClickHouseVersion, cfg.ClickHouse.Version)
		require.Equal(t, consts.DefaultClickHouseConfigDir, cfg.ClickHouse.ConfigDir)

		// Verify the ClickHouse XML file was created with the custom cluster name
		clickhouseXMLPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
		require.FileExists(t, clickhouseXMLPath)
		clickhouseXML, err := os.ReadFile(clickhouseXMLPath)
		require.NoError(t, err)

		// Check that the cluster name was properly replaced in all locations
		xmlContent := string(clickhouseXML)
		// In macros
		require.Contains(t, xmlContent, "<cluster>production</cluster>")
		// In remote_servers opening tag
		require.Contains(t, xmlContent, "<production>")
		// In remote_servers closing tag
		require.Contains(t, xmlContent, "</production>")
		// Should NOT have placeholder
		require.NotContains(t, xmlContent, "$$CLUSTER")
		// Should NOT have default cluster name
		require.NotContains(t, xmlContent, "<cluster>cluster</cluster>")
	})

	t.Run("initializes with cluster name containing special characters", func(t *testing.T) {
		tmpDir := t.TempDir()

		options := project.InitOptions{
			Cluster: "prod_cluster_01",
		}

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(options)
		require.NoError(t, err)

		// Verify the ClickHouse XML file was created with the cluster name with special chars
		clickhouseXMLPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
		require.FileExists(t, clickhouseXMLPath)
		clickhouseXML, err := os.ReadFile(clickhouseXMLPath)
		require.NoError(t, err)

		// Check that the cluster name with underscores and numbers works correctly
		xmlContent := string(clickhouseXML)
		// In macros
		require.Contains(t, xmlContent, "<cluster>prod_cluster_01</cluster>")
		// In remote_servers opening tag
		require.Contains(t, xmlContent, "<prod_cluster_01>")
		// In remote_servers closing tag
		require.Contains(t, xmlContent, "</prod_cluster_01>")
		// Should NOT have placeholder
		require.NotContains(t, xmlContent, "$$CLUSTER")
	})

	t.Run("initializes with default cluster when not specified", func(t *testing.T) {
		tmpDir := t.TempDir()

		options := project.InitOptions{
			Cluster: "", // Empty cluster should use default
		}

		proj := project.New(project.ProjectParams{
			Dir:       tmpDir,
			Formatter: format.New(format.Defaults),
		})
		err := proj.Initialize(options)
		require.NoError(t, err)

		// Verify config file was created with default cluster
		configPath := filepath.Join(tmpDir, "housekeeper.yaml")
		cfg, err := config.LoadConfigFile(configPath)
		require.NoError(t, err)
		require.Equal(t, consts.DefaultClickHouseCluster, cfg.ClickHouse.Cluster)
		require.Equal(t, "cluster", cfg.ClickHouse.Cluster)

		// Verify the ClickHouse XML file was created with the default cluster name
		clickhouseXMLPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
		require.FileExists(t, clickhouseXMLPath)
		clickhouseXML, err := os.ReadFile(clickhouseXMLPath)
		require.NoError(t, err)

		// Check that the default cluster name is used in all locations
		xmlContent := string(clickhouseXML)
		// In macros
		require.Contains(t, xmlContent, "<cluster>cluster</cluster>")
		// In remote_servers - count occurrences to make sure both tags exist
		clusterOpenCount := strings.Count(xmlContent, "<cluster>")
		clusterCloseCount := strings.Count(xmlContent, "</cluster>")
		require.GreaterOrEqual(t, clusterOpenCount, 2, "Should have at least 2 <cluster> tags (one in macros, one in remote_servers)")
		require.GreaterOrEqual(t, clusterCloseCount, 2, "Should have at least 2 </cluster> tags")
		// Should NOT have placeholder
		require.NotContains(t, xmlContent, "$$CLUSTER")
	})
}
