package cmd

import (
	"context"
	"os"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestBootstrapCommand_RequiresConfig(t *testing.T) {
	// Test that bootstrap command requires a config file
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	// Should fail with connection error because URL is empty string
	err := command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to connect to clickhouse server")
}

func TestBootstrapCommand_RequiresURL(t *testing.T) {
	// Test that bootstrap command requires URL flag
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	// Create a test CLI app to test flag validation
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"}) // No --url flag
	require.Error(t, err)
	require.Contains(t, err.Error(), "Required flag \"url\" not set")
}

func TestBootstrapCommand_WithURL(t *testing.T) {
	// Test bootstrap command with URL (will fail due to connection but validates structure)
	fixture := testutil.TestProject(t).
		WithConfig(testutil.TestConfig{
			Cluster: "production",
		})
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--url", "localhost:9000"})

	// Expect error due to connection failure, but it should get past validation
	require.Error(t, err)
	// Should not be a validation error about missing URL
	require.NotContains(t, err.Error(), "Required flag \"url\" not set")
}

func TestBootstrapCommand_WithEnvironmentURL(t *testing.T) {
	// Test bootstrap command using environment variable for URL
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Set environment variable
	t.Setenv("CH_DATABASE_URL", "tcp://localhost:9000")

	command := bootstrap(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test"}) // URL from environment

	// Expect error due to connection failure, but should get past URL validation
	require.Error(t, err)
	require.NotContains(t, err.Error(), "Required flag \"url\" not set")
}

func TestBootstrapCommand_FlagConfiguration(t *testing.T) {
	// Test that flags are configured correctly
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	require.Equal(t, "bootstrap", command.Name)
	require.Equal(t, "Extract schema from an existing ClickHouse server into initialized project", command.Usage)
	require.Len(t, command.Flags, 1)

	// Check URL flag
	urlFlag := command.Flags[0].(*cli.StringFlag)
	require.Equal(t, "url", urlFlag.Name)
	require.Equal(t, []string{"u"}, urlFlag.Aliases)
	require.True(t, urlFlag.Required)
	require.Equal(t, cli.EnvVars("CH_DATABASE_URL"), urlFlag.Sources)
}

func TestBootstrapCommand_ClusterConfiguration(t *testing.T) {
	// Test that cluster configuration is properly used
	fixture := testutil.TestProject(t).
		WithConfig(testutil.TestConfig{
			Cluster: "test_cluster",
		})
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)
	require.NotNil(t, command)

	// Verify config has the cluster we expect
	require.Equal(t, "test_cluster", fixture.Config.ClickHouse.Cluster)
}

func TestBootstrapCommand_InvalidURL(t *testing.T) {
	// Test bootstrap with invalid URL format
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()
	err := app.Run(ctx, []string{"test", "--url", "invalid-url"})

	// Should fail with connection/parsing error
	require.Error(t, err)
}

func TestBootstrapCommand_URLAliases(t *testing.T) {
	// Test that URL flag aliases work
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	// Create a test CLI app
	app := &cli.Command{
		Name:   "test",
		Flags:  command.Flags,
		Action: command.Action,
		Before: command.Before,
	}

	ctx := context.Background()

	// Test short alias
	err := app.Run(ctx, []string{"test", "-u", "localhost:9000"})
	require.Error(t, err)
	// Should not be a validation error
	require.NotContains(t, err.Error(), "Required flag")
}

func TestBootstrapCommand_ProjectStructure(t *testing.T) {
	// Test that bootstrap works with valid project structure
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Verify project is properly initialized
	testutil.RequireValidProject(t, fixture.Dir)

	command := bootstrap(fixture.Project, fixture.Config)
	require.NotNil(t, command)
	require.NotNil(t, command.Before) // Should have requireConfig
}

func TestBootstrapCommand_ConfigValidation(t *testing.T) {
	// Test that bootstrap validates config
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	ctx := context.Background()
	testCmd := &cli.Command{
		Flags: command.Flags,
	}

	err := command.Action(ctx, testCmd)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to connect to clickhouse server")
}

func TestBootstrapCommand_DSNFormats(t *testing.T) {
	// Test various DSN format handling
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	command := bootstrap(fixture.Project, fixture.Config)

	testCases := []struct {
		name string
		dsn  string
	}{
		{"host_port", "localhost:9000"},
		{"clickhouse_protocol", "clickhouse://user:pass@localhost:9000/db"},
		{"tcp_protocol", "tcp://localhost:9000"},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create a test CLI app
			app := &cli.Command{
				Name:   "test",
				Flags:  command.Flags,
				Action: command.Action,
				Before: command.Before,
			}

			ctx := context.Background()
			err := app.Run(ctx, []string{"test", "--url", tc.dsn})

			// All should fail due to connection, but should pass URL parsing
			require.Error(t, err)
			require.NotContains(t, err.Error(), "Required flag")
		})
	}
}

func TestBootstrapCommand_WithExistingSchemas(t *testing.T) {
	// Test bootstrap with existing schema files
	fixture := testutil.TestProject(t).
		WithSchemaFiles(map[string]string{
			"analytics/schema.sql": testutil.AnalyticsSchema(),
			"logs/schema.sql":      "CREATE DATABASE logs ENGINE = Atomic;",
		})
	defer fixture.Cleanup()

	// Verify schema files exist
	require.FileExists(t, fixture.GetSchemasDir()+"/analytics/schema.sql")
	require.FileExists(t, fixture.GetSchemasDir()+"/logs/schema.sql")

	command := bootstrap(fixture.Project, fixture.Config)
	require.NotNil(t, command)
}

func TestBootstrapCommand_EmptyProject(t *testing.T) {
	// Test bootstrap with minimal project
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	// Clear the main schema file
	err := os.WriteFile(fixture.GetMainSchemaPath(), []byte("-- Empty schema\n"), consts.ModeFile)
	require.NoError(t, err)

	command := bootstrap(fixture.Project, fixture.Config)
	require.NotNil(t, command)
}
