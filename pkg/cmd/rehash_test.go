package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestRehashCommand_WithMigrations(t *testing.T) {
	// Test rehash with existing migrations
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Check output message
	output := buf.String()
	require.Contains(t, output, "Successfully rehashed")
	require.Contains(t, output, "migration(s)")

	// Check that sum file was created
	sumPath := filepath.Join(fixture.Dir, fixture.Config.Dir, "housekeeper.sum")
	require.FileExists(t, sumPath)

	// Verify sum file content
	testutil.RequireSumFileValid(t, sumPath)
}

func TestRehashCommand_EmptyMigrationsDirectory(t *testing.T) {
	// Test rehash with empty migrations directory
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Ensure migrations directory exists but is empty
	testutil.RequireDirEmpty(t, fixture.GetMigrationsDir())

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Should report 0 migrations
	output := buf.String()
	require.Contains(t, output, "Successfully rehashed 0 migration(s)")

	// Sum file should still be created
	sumPath := filepath.Join(fixture.Dir, fixture.Config.Dir, "housekeeper.sum")
	require.FileExists(t, sumPath)
}

func TestRehashCommand_NoMigrationsDirectory(t *testing.T) {
	// Test rehash when migrations directory doesn't exist
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Remove migrations directory
	err := os.RemoveAll(fixture.GetMigrationsDir())
	require.NoError(t, err)

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "migrations directory does not exist")
}

func TestRehashCommand_UpdatesExistingSumFile(t *testing.T) {
	// Test that rehash updates existing sum file
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations()).
		WithSumFile("h1:oldhash=\n001_init.sql h1:oldfilehash=")
	defer fixture.Cleanup()

	sumPath := filepath.Join(fixture.Dir, fixture.Config.Dir, "housekeeper.sum")

	// Read original sum file
	originalContent, err := os.ReadFile(sumPath)
	require.NoError(t, err)
	require.Contains(t, string(originalContent), "oldhash")

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Check that sum file was updated
	newContent, err := os.ReadFile(sumPath)
	require.NoError(t, err)

	newContentStr := string(newContent)
	require.NotContains(t, newContentStr, "oldhash") // Old hash should be gone
	require.Contains(t, newContentStr, "h1:")        // Should have new hash format

	// Verify new sum file is valid
	testutil.RequireSumFileValid(t, sumPath)
}

func TestRehashCommand_WithMultipleMigrations(t *testing.T) {
	// Test rehash with multiple migrations
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.SampleMigrations())
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Should report correct number of migrations
	output := buf.String()
	require.Contains(t, output, "Successfully rehashed")
	require.Contains(t, output, "migration(s)")

	// Verify sum file has entries for all migrations
	sumPath := filepath.Join(fixture.Dir, fixture.Config.Dir, "housekeeper.sum")
	content, err := os.ReadFile(sumPath)
	require.NoError(t, err)

	sumContent := string(content)
	lines := strings.Split(strings.TrimSpace(sumContent), "\n")
	// Should have total hash line + one line per migration
	require.GreaterOrEqual(t, len(lines), len(testutil.SampleMigrations()))
}

func TestRehashCommand_FilePermissions(t *testing.T) {
	// Test that rehash sets correct file permissions on sum file
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Check sum file permissions
	sumPath := filepath.Join(fixture.Dir, fixture.Config.Dir, "housekeeper.sum")
	testutil.RequireFilePermissions(t, sumPath, consts.ModeFile)
}

func TestRehashCommand_InvalidMigrations(t *testing.T) {
	// Test rehash with invalid migration files
	fixture := testutil.TestProject(t).
		WithMigrations([]testutil.MigrationFile{
			testutil.InvalidMigration(),
		})
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to load migration directory")
}

func TestRehashCommand_MigrationCount(t *testing.T) {
	// Test that rehash reports correct migration count
	migrations := []testutil.MigrationFile{
		{Version: "001_first", SQL: "CREATE DATABASE test1;"},
		{Version: "002_second", SQL: "CREATE DATABASE test2;"},
		{Version: "003_third", SQL: "CREATE DATABASE test3;"},
	}

	fixture := testutil.TestProject(t).
		WithMigrations(migrations)
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Successfully rehashed 3 migration(s)")
}

func TestRehashCommand_CommandStructure(t *testing.T) {
	// Test that command has correct structure
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := rehash(fixture.Project)

	require.Equal(t, "rehash", command.Name)
	require.Equal(t, "Regenerate the sum file for all migrations", command.Usage)
	require.Empty(t, command.Flags) // No flags
}

func TestRehashCommand_ReadOnlyMigrationsDir(t *testing.T) {
	// Test rehash when project root directory is read-only (can't create sum file)
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
		_ = os.Chmod(fixture.GetMigrationsDir(), consts.ModeDir)
	}()

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to create sum file")
}

func TestRehashCommand_WithSubdirectories(t *testing.T) {
	// Test rehash ignores subdirectories and non-SQL files
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	// Create subdirectory and non-SQL file in migrations dir
	subDir := filepath.Join(fixture.GetMigrationsDir(), "subdir")
	err := os.MkdirAll(subDir, consts.ModeDir)
	require.NoError(t, err)

	txtFile := filepath.Join(fixture.GetMigrationsDir(), "readme.txt")
	err = os.WriteFile(txtFile, []byte("Not a migration"), consts.ModeFile)
	require.NoError(t, err)

	command := rehash(fixture.Project)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err = command.Action(ctx, testCmd)
	require.NoError(t, err)

	// Should only count actual migration files
	output := buf.String()
	require.Contains(t, output, "Successfully rehashed 2 migration(s)") // Only SQL migrations
}

func TestTestableRehash(t *testing.T) {
	// Test that TestableRehash works the same as rehash
	fixture := testutil.TestProject(t).
		WithMigrations(testutil.MinimalMigrations())
	defer fixture.Cleanup()

	command := TestableRehash(fixture.Project)

	require.Equal(t, "rehash", command.Name)
	require.NotNil(t, command.Action)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "Successfully rehashed")
}
