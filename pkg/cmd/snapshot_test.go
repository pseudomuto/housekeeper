package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestSnapshotCommand_WithMigrations(t *testing.T) {
	// Test creating snapshot with existing migrations
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Creating snapshot")
	require.Contains(t, output, "with 2 migrations")
	require.Contains(t, output, "Snapshot created successfully!")
	require.Contains(t, output, "Consolidated 2 migrations")

	// Check that snapshot file was created
	files, err := os.ReadDir(fixture.GetMigrationsDir())
	require.NoError(t, err)

	var snapshotFound bool
	for _, file := range files {
		if strings.Contains(file.Name(), "_snapshot.sql") {
			snapshotFound = true
			break
		}
	}
	require.True(t, snapshotFound, "Snapshot file should be created")
}

func TestSnapshotCommand_NoMigrations(t *testing.T) {
	// Test snapshot with no migrations
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "No migrations found to create snapshot from")
}

func TestSnapshotCommand_CustomDescription(t *testing.T) {
	// Test snapshot with custom description
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	var buf bytes.Buffer
	app.Writer = &buf

	err := app.Run(ctx, []string{"test", "--description", "Initial baseline"})
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Description: Initial baseline")
}

func TestSnapshotCommand_DefaultDescription(t *testing.T) {
	// Test snapshot with default description
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Description: Schema snapshot")
}

func TestSnapshotCommand_FlagConfiguration(t *testing.T) {
	// Test that flags are configured correctly
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	require.Equal(t, "snapshot", command.Name)
	require.Equal(t, "Create a snapshot from migrations or project schema", command.Usage)
	require.Len(t, command.Flags, 2)

	// Check description flag
	descFlag := command.Flags[0].(*cli.StringFlag)
	require.Equal(t, "description", descFlag.Name)
	require.Empty(t, descFlag.Aliases) // No aliases anymore to avoid conflict with global -d
	require.Equal(t, "Schema snapshot", descFlag.Value)

	// Check bootstrap flag
	bootstrapFlag := command.Flags[1].(*cli.BoolFlag)
	require.Equal(t, "bootstrap", bootstrapFlag.Name)
	require.Equal(t, "Create snapshot from project schema instead of existing migrations", bootstrapFlag.Usage)
}

func TestSnapshotCommand_DescriptionFlag(t *testing.T) {
	// Test description flag with full name (no alias anymore)
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	var buf bytes.Buffer
	app.Writer = &buf

	err := app.Run(ctx, []string{"test", "--description", "Full flag test"})
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Description: Full flag test")
}

func TestSnapshotCommand_RemovesMigrationFiles(t *testing.T) {
	// Test that snapshot removes original migration files
	migrations := []testutil.MigrationFile{
		{Version: "001_first", SQL: "CREATE DATABASE test1;"},
		{Version: "002_second", SQL: "CREATE DATABASE test2;"},
	}

	fixture := testutil.TestProject(t).
		WithMigrations(migrations)
	defer fixture.Cleanup()

	// Verify migration files exist before snapshot
	file1 := filepath.Join(fixture.GetMigrationsDir(), "001_first.sql")
	file2 := filepath.Join(fixture.GetMigrationsDir(), "002_second.sql")
	require.FileExists(t, file1)
	require.FileExists(t, file2)

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Verify original migration files were removed
	testutil.RequireNoFile(t, file1)
	testutil.RequireNoFile(t, file2)

	// Verify removal was logged
	output := buf.String()
	require.Contains(t, output, "Removed migration file")
}

func TestSnapshotCommand_WithSampleMigrations(t *testing.T) {
	// Test snapshot with sample migrations
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.SampleMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Creating snapshot")
	require.Contains(t, output, "Snapshot created successfully!")
	require.Contains(t, output, "migrations")
}

func TestSnapshotCommand_SnapshotFileContent(t *testing.T) {
	// Test that snapshot file contains expected content
	fixture := testutil.TestProject(t).
		WithMigrations([]testutil.MigrationFile{
			{Version: "001_test", SQL: "CREATE DATABASE snapshot_test ENGINE = Atomic;"},
		})
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Find snapshot file
	files, err := os.ReadDir(fixture.GetMigrationsDir())
	require.NoError(t, err)

	var snapshotFile string
	for _, file := range files {
		if strings.Contains(file.Name(), "_snapshot.sql") {
			snapshotFile = filepath.Join(fixture.GetMigrationsDir(), file.Name())
			break
		}
	}
	require.NotEmpty(t, snapshotFile, "Snapshot file should exist")

	// Read snapshot content
	content, err := os.ReadFile(snapshotFile)
	require.NoError(t, err)
	require.Contains(t, string(content), "CREATE DATABASE `snapshot_test`")
}

func TestSnapshotCommand_TimestampFormat(t *testing.T) {
	// Test that snapshot uses correct timestamp format
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Check output for timestamp format
	output := buf.String()
	require.Regexp(t, `Creating snapshot \d{14}_snapshot`, output)
}

func TestSnapshotCommand_FileCreationError(t *testing.T) {
	// Test snapshot when file creation fails
	if os.Getuid() == 0 {
		t.Skip("Cannot test permission errors as root")
	}

	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	// Make migrations directory read-only
	err := os.Chmod(fixture.GetMigrationsDir(), 0o555)
	require.NoError(t, err)

	// Restore permissions for cleanup
	defer func() {
		_ = os.Chmod(fixture.GetMigrationsDir(), 0o755)
	}()

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create snapshot file")
}

func TestSnapshotCommand_InvalidMigrationsDirectory(t *testing.T) {
	// Test snapshot with invalid migrations directory
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Remove migrations directory
	err := os.RemoveAll(fixture.GetMigrationsDir())
	require.NoError(t, err)

	command := snapshot(fixture.Project, fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.Error(t, err)
	// Should fail to load migration directory
	require.NotContains(t, err.Error(), "No migrations found")
}

func TestSnapshotCommand_LongDescription(t *testing.T) {
	// Test snapshot with long description
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	longDesc := "This is a very long description that contains multiple words and should be properly handled by the snapshot command"

	command := snapshot(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	var buf bytes.Buffer
	app.Writer = &buf

	err := app.Run(ctx, []string{"test", "--description", longDesc})
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Description: "+longDesc)
}
