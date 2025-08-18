package cmd

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestFmtCommand_RequiresPath(t *testing.T) {
	// Test that fmt command requires a path argument
	command := fmtCmd()

	// Create a test CLI app (no arguments provided)
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"}) // No path argument
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one path argument is required")
}

func TestFmtCommand_SingleFile(t *testing.T) {
	// Test formatting a single file to stdout
	tmpDir := t.TempDir()

	// Create a SQL file with unformatted content
	sqlFile := filepath.Join(tmpDir, "test.sql")
	unformattedSQL := "CREATE DATABASE test ENGINE=Atomic;CREATE TABLE test.users(id UInt64,name String)ENGINE=MergeTree()ORDER BY id;"
	err := os.WriteFile(sqlFile, []byte(unformattedSQL), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", sqlFile})
	require.NoError(t, err)

	// Check that output is formatted
	output := buf.String()
	require.Contains(t, output, "CREATE DATABASE")
	require.Contains(t, output, "CREATE TABLE")
	// Should be properly formatted (not all on one line)
	require.Greater(t, strings.Count(output, "\n"), 1)
}

func TestFmtCommand_SingleFileWriteBack(t *testing.T) {
	// Test formatting a single file with write-back
	tmpDir := t.TempDir()

	// Create a SQL file with unformatted content
	sqlFile := filepath.Join(tmpDir, "test.sql")
	unformattedSQL := "CREATE DATABASE test ENGINE=Atomic;CREATE TABLE test.users(id UInt64,name String)ENGINE=MergeTree()ORDER BY id;"
	err := os.WriteFile(sqlFile, []byte(unformattedSQL), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", "-w", sqlFile})
	require.NoError(t, err)

	// Check that file was modified
	content, err := os.ReadFile(sqlFile)
	require.NoError(t, err)

	formatted := string(content)
	require.Contains(t, formatted, "CREATE DATABASE")
	require.Contains(t, formatted, "CREATE TABLE")
	// Should be properly formatted
	require.Greater(t, strings.Count(formatted, "\n"), 1)
	require.NotEqual(t, unformattedSQL, formatted)
}

func TestFmtCommand_Directory(t *testing.T) {
	// Test formatting all SQL files in a directory
	tmpDir := t.TempDir()

	// Create multiple SQL files
	file1 := filepath.Join(tmpDir, "schema1.sql")
	file2 := filepath.Join(tmpDir, "schema2.sql")

	sql1 := "CREATE DATABASE db1 ENGINE=Atomic;"
	sql2 := "CREATE DATABASE db2 ENGINE=Atomic;"

	err := os.WriteFile(file1, []byte(sql1), consts.ModeFile)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte(sql2), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", tmpDir})
	require.NoError(t, err)

	// Should have formatted both files
	output := buf.String()
	require.Contains(t, output, "db1")
	require.Contains(t, output, "db2")
}

func TestFmtCommand_DirectoryWriteBack(t *testing.T) {
	// Test formatting directory with write-back
	tmpDir := t.TempDir()

	// Create multiple SQL files
	file1 := filepath.Join(tmpDir, "schema1.sql")
	file2 := filepath.Join(tmpDir, "schema2.sql")

	unformatted1 := "CREATE DATABASE db1 ENGINE=Atomic;"
	unformatted2 := "CREATE DATABASE db2 ENGINE=Atomic;"

	err := os.WriteFile(file1, []byte(unformatted1), consts.ModeFile)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte(unformatted2), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", "-w", tmpDir})
	require.NoError(t, err)

	// Check that both files were modified
	content1, err := os.ReadFile(file1)
	require.NoError(t, err)
	content2, err := os.ReadFile(file2)
	require.NoError(t, err)

	require.Contains(t, string(content1), "db1")
	require.Contains(t, string(content2), "db2")
}

func TestFmtCommand_RecursiveDirectory(t *testing.T) {
	// Test formatting SQL files recursively in subdirectories
	tmpDir := t.TempDir()
	subDir := filepath.Join(tmpDir, "subdir")
	err := os.MkdirAll(subDir, consts.ModeDir)
	require.NoError(t, err)

	// Create SQL files in both directories
	file1 := filepath.Join(tmpDir, "root.sql")
	file2 := filepath.Join(subDir, "sub.sql")

	sql1 := "CREATE DATABASE root ENGINE=Atomic;"
	sql2 := "CREATE DATABASE sub ENGINE=Atomic;"

	err = os.WriteFile(file1, []byte(sql1), consts.ModeFile)
	require.NoError(t, err)
	err = os.WriteFile(file2, []byte(sql2), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", tmpDir})
	require.NoError(t, err)

	// Should have formatted both files
	output := buf.String()
	require.Contains(t, output, "root")
	require.Contains(t, output, "sub")
}

func TestFmtCommand_NonexistentPath(t *testing.T) {
	// Test formatting nonexistent path
	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "/nonexistent/path"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to access path")
}

func TestFmtCommand_InvalidSQL(t *testing.T) {
	// Test formatting file with invalid SQL
	tmpDir := t.TempDir()

	sqlFile := filepath.Join(tmpDir, "invalid.sql")
	invalidSQL := "INVALID SQL SYNTAX HERE;"
	err := os.WriteFile(sqlFile, []byte(invalidSQL), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", sqlFile})
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to parse SQL")
}

func TestFmtCommand_EmptyDirectory(t *testing.T) {
	// Test formatting empty directory (no SQL files)
	tmpDir := t.TempDir()

	// Create non-SQL file
	txtFile := filepath.Join(tmpDir, "readme.txt")
	err := os.WriteFile(txtFile, []byte("Not SQL"), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", tmpDir})
	require.Error(t, err)
	require.Contains(t, err.Error(), "no SQL files found")
}

func TestFmtCommand_FlagConfiguration(t *testing.T) {
	// Test that flags are configured correctly
	command := fmtCmd()

	require.Equal(t, "fmt", command.Name)
	require.Equal(t, "Format SQL files", command.Usage)
	require.Equal(t, "<path>", command.ArgsUsage)
	require.Len(t, command.Flags, 1)

	// Check write flag
	writeFlag := command.Flags[0].(*cli.BoolFlag)
	require.Equal(t, "write", writeFlag.Name)
	require.Equal(t, []string{"w"}, writeFlag.Aliases)
}

func TestFmtCommand_ComplexSQL(t *testing.T) {
	// Test formatting complex SQL with multiple statements
	tmpDir := t.TempDir()

	sqlFile := filepath.Join(tmpDir, "complex.sql")
	complexSQL := `CREATE DATABASE analytics ENGINE=Atomic;
CREATE TABLE analytics.events(id UInt64,user_id UInt64,timestamp DateTime DEFAULT now(),data Map(String,String))ENGINE=MergeTree()ORDER BY(user_id,timestamp)PARTITION BY toYYYYMM(timestamp);
CREATE MATERIALIZED VIEW analytics.daily_stats ENGINE=MergeTree()ORDER BY date AS SELECT toDate(timestamp)as date,count()as events FROM analytics.events GROUP BY date;`

	err := os.WriteFile(sqlFile, []byte(complexSQL), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", sqlFile})
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "CREATE DATABASE")
	require.Contains(t, output, "CREATE TABLE")
	require.Contains(t, output, "CREATE MATERIALIZED VIEW")
	// Should be properly formatted across multiple lines
	require.Greater(t, strings.Count(output, "\n"), 5)
}

func TestFmtCommand_EmptyFile(t *testing.T) {
	// Test formatting empty SQL file
	tmpDir := t.TempDir()

	sqlFile := filepath.Join(tmpDir, "empty.sql")
	err := os.WriteFile(sqlFile, []byte(""), consts.ModeFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", sqlFile})
	require.NoError(t, err)

	// Empty file should produce empty output
	output := buf.String()
	require.Empty(t, strings.TrimSpace(output))
}

func TestFmtCommand_WritePermissions(t *testing.T) {
	// Test that write-back preserves file permissions
	tmpDir := t.TempDir()

	sqlFile := filepath.Join(tmpDir, "test.sql")
	sql := "CREATE DATABASE test ENGINE=Atomic;"
	err := os.WriteFile(sqlFile, []byte(sql), consts.ModeFile)
	require.NoError(t, err)

	// Get original file info
	originalInfo, err := os.Stat(sqlFile)
	require.NoError(t, err)

	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err = app.Run(ctx, []string{"test", "-w", sqlFile})
	require.NoError(t, err)

	// Check that file still exists and has expected permissions
	newInfo, err := os.Stat(sqlFile)
	require.NoError(t, err)
	require.Equal(t, originalInfo.Mode(), newInfo.Mode())
}

func TestFmtCommand_MultipleArguments(t *testing.T) {
	// Test that command rejects multiple arguments
	command := fmtCmd()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "file1.sql", "file2.sql"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "exactly one path argument is required")
}
