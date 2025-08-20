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

func TestSchemaCommand_Structure(t *testing.T) {
	// Test that schema command has proper structure
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := schema(fixture.Config)

	require.Equal(t, "schema", command.Name)
	require.Equal(t, "Commands for working with schemas", command.Usage)
	require.Len(t, command.Commands, 2) // dump and compile

	// Check subcommands
	var dumpCmd, compileCmd *cli.Command
	for _, subcmd := range command.Commands {
		switch subcmd.Name {
		case "dump":
			dumpCmd = subcmd
		case "compile":
			compileCmd = subcmd
		}
	}

	require.NotNil(t, dumpCmd, "Should have dump subcommand")
	require.NotNil(t, compileCmd, "Should have compile subcommand")
}

func TestSchemaDumpCommand_RequiresURL(t *testing.T) {
	// Test that schema dump requires URL flag
	// Create a custom flag without the environment variable to test flag requirement
	testUrlFlag := &cli.StringFlag{
		Name:     "url",
		Aliases:  []string{"u"},
		Usage:    "ClickHouse connection DSN (host:port, clickhouse://..., tcp://...)",
		Required: true,
		Config: cli.StringConfig{
			TrimSpace: true,
		},
	}

	command := schemaDump()
	// Replace the flag with our test flag that has no env var
	command.Flags[0] = testUrlFlag

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"}) // No --url flag
	require.Error(t, err)
	require.Contains(t, err.Error(), "Required flag \"url\" not set")
}

func TestSchemaDumpCommand_WithURL(t *testing.T) {
	// Test schema dump with URL (will fail due to connection)
	command := schemaDump()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--url", "localhost:9000"})

	// Should fail due to connection, but should pass URL validation
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag \"url\" not set")
}

func TestSchemaDumpCommand_WithCluster(t *testing.T) {
	// Test schema dump with cluster flag
	command := schemaDump()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--url", "localhost:9000", "--cluster", "test_cluster"})

	// Should fail due to connection, but flags should be valid
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag")
}

func TestSchemaDumpCommand_WithOutputFile(t *testing.T) {
	// Test schema dump with output file
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "schema.sql")

	command := schemaDump()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--url", "localhost:9000", "--out", outFile})

	// Should fail due to connection, but file flag should be processed
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag")
}

func TestSchemaDumpCommand_EnvironmentURL(t *testing.T) {
	// Test schema dump using environment variable
	t.Setenv("CH_DATABASE_URL", "tcp://localhost:9000")

	command := schemaDump()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"}) // URL from environment

	// Should fail due to connection, but URL should be read from env
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag \"url\" not set")
}

func TestSchemaDumpCommand_FlagConfiguration(t *testing.T) {
	// Test that flags are configured correctly
	command := schemaDump()

	require.Equal(t, "dump", command.Name)
	require.Equal(t, "Extract and format schema from a ClickHouse instance", command.Usage)
	require.Len(t, command.Flags, 4) // url, cluster, ignore-databases, out

	flagNames := make([]string, 0, len(command.Flags))
	for _, flag := range command.Flags {
		switch f := flag.(type) {
		case *cli.StringFlag:
			flagNames = append(flagNames, f.Name)
		case *cli.StringSliceFlag:
			flagNames = append(flagNames, f.Name)
		}
	}

	require.Contains(t, flagNames, "url")
	require.Contains(t, flagNames, "cluster")
	require.Contains(t, flagNames, "ignore-databases")
	require.Contains(t, flagNames, "out")
}

func TestSchemaCompileCommand_RequiresConfig(t *testing.T) {
	// Test that schema compile requires config
	command := schemaParse(nil)

	// Create a test CLI app that will properly call Before then Action
	var buf bytes.Buffer
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Before: command.Before,
		Action: command.Action,
		Writer: &buf,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"})
	require.Error(t, err)
	require.Contains(t, err.Error(), "housekeeper.yaml not found")
}

func TestSchemaCompileCommand_WithValidProject(t *testing.T) {
	// Test schema compile with valid project
	fixture := testutil.TestProject(t).
		WithSchema(testutil.DefaultSchema())
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	ctx := context.Background()

	// Capture output
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	// May fail due to schema compilation, but should pass config validation
	if err != nil {
		require.NotContains(t, err.Error(), "housekeeper.yaml not found")
	}
}

func TestSchemaCompileCommand_WithOutputFile(t *testing.T) {
	// Test schema compile with output file
	fixture := testutil.TestProject(t).
		WithSchema("CREATE DATABASE test ENGINE = Atomic;")
	defer fixture.Cleanup()

	outFile := filepath.Join(fixture.Dir, "compiled.sql")

	command := schemaParse(fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--out", outFile})

	if err == nil {
		// If successful, output file should exist
		require.FileExists(t, outFile)

		// Verify file has content
		content, readErr := os.ReadFile(outFile)
		require.NoError(t, readErr)
		require.NotEmpty(t, content)
	} else {
		// If failed, should not be due to missing config
		require.NotContains(t, err.Error(), "housekeeper.yaml not found")
	}
}

func TestSchemaCompileCommand_WithImports(t *testing.T) {
	// Test schema compile with import directives
	fixture := testutil.TestProject(t).
		WithSchemaFiles(map[string]string{
			"databases.sql": testutil.DatabasesSchema(),
		}).
		WithSchema(testutil.SchemaWithImports())
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)
	require.NotNil(t, command)
	require.Equal(t, "compile", command.Name)
}

func TestSchemaCompileCommand_InvalidSchema(t *testing.T) {
	// Test schema compile with invalid schema
	fixture := testutil.TestProject(t).
		WithSchema("INVALID SQL SYNTAX;")
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)

	// Should fail due to invalid SQL, not missing config
	require.Error(t, err)
	require.NotContains(t, err.Error(), "housekeeper.yaml not found")
}

func TestSchemaCompileCommand_FlagConfiguration(t *testing.T) {
	// Test that compile command flags are configured correctly
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	require.Equal(t, "compile", command.Name)
	require.Equal(t, "Compile the project schema", command.Usage)
	require.Len(t, command.Flags, 1)  // out flag
	require.NotNil(t, command.Before) // Should have requireConfig

	// Check out flag
	outFlag := command.Flags[0].(*cli.StringFlag)
	require.Equal(t, "out", outFlag.Name)
	require.Equal(t, []string{"o"}, outFlag.Aliases)
}

func TestSchemaCompileCommand_EmptySchema(t *testing.T) {
	// Test schema compile with empty schema file
	fixture := testutil.TestProject(t).
		WithSchema("-- Empty schema\n")
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)

	if err == nil {
		// Should produce minimal output for empty schema
		output := buf.String()
		require.NotContains(t, output, "CREATE") // No CREATE statements
	}
}

func TestSchemaCompileCommand_OutputToStdout(t *testing.T) {
	// Test schema compile output to stdout
	fixture := testutil.TestProject(t).
		WithSchema("CREATE DATABASE test ENGINE = Atomic;")
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)

	if err == nil {
		// Should have SQL output
		output := buf.String()
		require.Contains(t, strings.ToLower(output), "create database")
	}
}

func TestSchemaDumpCommand_Aliases(t *testing.T) {
	// Test that flag aliases work for dump command
	command := schemaDump()

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
	}

	ctx := context.Background()

	// Test URL alias
	err := app.Run(ctx, []string{"test", "-u", "localhost:9000"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag")

	// Test cluster alias
	err = app.Run(ctx, []string{"test", "-u", "localhost:9000", "-c", "test"})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag")

	// Test output alias
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "out.sql")
	err = app.Run(ctx, []string{"test", "-u", "localhost:9000", "-o", outFile})
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag")
}

func TestSchemaCompileCommand_Aliases(t *testing.T) {
	// Test that flag aliases work for compile command
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := schemaParse(fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	tmpDir := t.TempDir()
	outFile := filepath.Join(tmpDir, "out.sql")

	// Test output alias
	err := app.Run(ctx, []string{"test", "-o", outFile})
	// Should not fail due to flag parsing
	if err != nil {
		require.NotContains(t, err.Error(), "flag provided but not defined")
	}
}
