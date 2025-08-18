package cmd

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestInitCommand_BasicInitialization(t *testing.T) {
	// Create a temp directory without initializing
	tmpDir := t.TempDir()

	// Create project instance
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	// Get the init command
	command := initCmd(proj)

	// Create a test context with no flags
	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	// Execute the command
	err := command.Action(ctx, testCmd)
	require.NoError(t, err, "Init command should succeed")

	// Verify project structure was created
	testutil.RequireValidProject(t, tmpDir)

	// Verify default configuration
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	cfg, err := config.LoadConfigFile(configPath)
	require.NoError(t, err)
	require.Equal(t, consts.DefaultClickHouseCluster, cfg.ClickHouse.Cluster)
	require.Equal(t, consts.DefaultClickHouseVersion, cfg.ClickHouse.Version)
	require.Equal(t, consts.DefaultClickHouseConfigDir, cfg.ClickHouse.ConfigDir)

	// Verify ClickHouse XML with default cluster
	xmlPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
	testutil.RequireClickHouseXMLValid(t, xmlPath, "cluster")
}

func TestInitCommand_WithCustomCluster(t *testing.T) {
	// Create a temp directory without initializing
	tmpDir := t.TempDir()

	// Create project instance
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	// Get the init command
	command := initCmd(proj)

	// Create a test CLI app with the cluster flag
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--cluster", "production"})
	require.NoError(t, err, "Init command with cluster flag should succeed")

	// Verify project structure was created
	testutil.RequireValidProject(t, tmpDir)

	// Verify custom cluster in configuration
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	cfg, err := config.LoadConfigFile(configPath)
	require.NoError(t, err)
	require.Equal(t, "production", cfg.ClickHouse.Cluster)

	// Verify ClickHouse XML with custom cluster
	xmlPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
	testutil.RequireClickHouseXMLValid(t, xmlPath, "production")
}

func TestInitCommand_IdempotentInitialization(t *testing.T) {
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Modify a file to ensure it's not overwritten
	mainSQLPath := fixture.GetMainSchemaPath()
	customContent := []byte("-- Custom content that should be preserved\n")
	err := os.WriteFile(mainSQLPath, customContent, consts.ModeFile)
	require.NoError(t, err)

	// Run init command again
	command := initCmd(fixture.Project)
	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err, "Second init should succeed")

	// Verify custom content was preserved
	content, err := os.ReadFile(mainSQLPath)
	require.NoError(t, err)
	require.Equal(t, customContent, content, "Custom content should be preserved")

	// Verify project structure is still valid
	testutil.RequireValidProject(t, fixture.Dir)
}

func TestInitCommand_PreservesExistingConfig(t *testing.T) {
	tmpDir := t.TempDir()

	// Create an existing config with custom settings
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	customConfig := `entrypoint: custom/main.sql
dir: custom/migrations

clickhouse:
  version: "24.3"
  cluster: "staging"
  config_dir: "custom/config"
`
	err := os.WriteFile(configPath, []byte(customConfig), consts.ModeFile)
	require.NoError(t, err)

	// Create project and run init
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	command := initCmd(proj)
	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err, "Init should succeed with existing config")

	// Verify config was not overwritten
	content, err := os.ReadFile(configPath)
	require.NoError(t, err)
	require.Equal(t, customConfig, string(content), "Existing config should be preserved")
}

func TestInitCommand_CreatesNestedDirectories(t *testing.T) {
	tmpDir := t.TempDir()

	// Create only the db directory
	dbDir := filepath.Join(tmpDir, "db")
	err := os.MkdirAll(dbDir, consts.ModeDir)
	require.NoError(t, err)

	// Create project and run init
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	command := initCmd(proj)
	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err, "Init should create nested directories")

	// Verify all nested directories were created
	require.DirExists(t, filepath.Join(dbDir, "migrations"))
	require.DirExists(t, filepath.Join(dbDir, "schemas"))
	require.DirExists(t, filepath.Join(dbDir, "config.d"))
}

func TestInitCommand_WithClusterContainingSpecialCharacters(t *testing.T) {
	tmpDir := t.TempDir()

	// Create project instance
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	// Get the init command
	command := initCmd(proj)

	// Create a test CLI app with cluster containing underscores and numbers
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--cluster", "prod_cluster_01"})
	require.NoError(t, err, "Init with special characters in cluster should succeed")

	// Verify cluster name in config
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	cfg, err := config.LoadConfigFile(configPath)
	require.NoError(t, err)
	require.Equal(t, "prod_cluster_01", cfg.ClickHouse.Cluster)

	// Verify ClickHouse XML with special character cluster
	xmlPath := filepath.Join(tmpDir, "db", "config.d", "_clickhouse.xml")
	testutil.RequireClickHouseXMLValid(t, xmlPath, "prod_cluster_01")
}

func TestInitCommand_EmptyClusterFlag(t *testing.T) {
	tmpDir := t.TempDir()

	// Create project instance
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	// Get the init command
	command := initCmd(proj)

	// Create a test CLI app with empty cluster flag
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--cluster", ""})
	require.NoError(t, err, "Init with empty cluster flag should succeed")

	// Should use default cluster
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	cfg, err := config.LoadConfigFile(configPath)
	require.NoError(t, err)
	require.Equal(t, consts.DefaultClickHouseCluster, cfg.ClickHouse.Cluster)
}

func TestInitCommand_ProjectAlreadyInitialized(t *testing.T) {
	// Use test fixture which already initializes the project
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Add some custom files to verify they're preserved
	customFile := filepath.Join(fixture.Dir, "db", "custom.sql")
	err := os.WriteFile(customFile, []byte("-- Custom SQL"), consts.ModeFile)
	require.NoError(t, err)

	// Run init command again
	command := initCmd(fixture.Project)
	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err, "Re-initialization should succeed")

	// Verify custom file still exists
	require.FileExists(t, customFile)
	content, err := os.ReadFile(customFile)
	require.NoError(t, err)
	require.Equal(t, []byte("-- Custom SQL"), content)

	// Verify project structure is still valid
	testutil.RequireValidProject(t, fixture.Dir)
}
