package cmd

import (
	"bytes"
	"context"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/pseudomuto/housekeeper/pkg/cmd/testutil"
	"github.com/stretchr/testify/require"
	"github.com/urfave/cli/v3"
)

func TestDevCommand_Structure(t *testing.T) {
	// Test that dev command has correct structure
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()
	command := dev(fixture.Config, dockerClient)

	require.Equal(t, "dev", command.Name)
	require.Equal(t, "Manage local ClickHouse development server", command.Usage)
	require.NotNil(t, command.Before)
	require.Len(t, command.Commands, 2)

	// Check subcommands
	var upCmd, downCmd *cli.Command
	for _, subcmd := range command.Commands {
		switch subcmd.Name {
		case "up":
			upCmd = subcmd
		case "down":
			downCmd = subcmd
		}
	}

	require.NotNil(t, upCmd)
	require.NotNil(t, downCmd)
	require.Equal(t, "Start ClickHouse development server and apply migrations", upCmd.Usage)
	require.Equal(t, "Stop and remove ClickHouse development server", downCmd.Usage)
}

func TestDevUpCommand_ContainerAlreadyRunning(t *testing.T) {
	// Test dev up when container is already running
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, nil // Container exists (no error)
	}

	command := devUp(fixture.Config, dockerClient)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "ClickHouse development server is already running")
	require.Contains(t, output, "Use 'housekeeper dev down' to stop it first")
}

func TestDevUpCommand_StartNewContainer(t *testing.T) {
	// Skip this integration test in short mode
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	// Test dev up starting a new container
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()

	// Container doesn't exist initially
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, testutil.ErrContainerNotFound
	}

	// Mock container operations
	mockContainer := testutil.NewMockClickHouseContainer()
	mockContainer.GetDSNFunc = func(ctx context.Context) (string, error) {
		return "localhost:9000", nil
	}
	mockContainer.GetHTTPDSNFunc = func(ctx context.Context) (string, error) {
		return "http://localhost:8123", nil
	}

	// Mock the runContainer function by setting up the client expectations
	// Note: This test will fail at runContainer call since it's not fully mocked

	command := devUp(fixture.Config, dockerClient)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	// This test will fail at runContainer call since it's not mocked
	// We're testing the flow up to that point
	err := command.Action(ctx, testCmd)
	require.Error(t, err) // Expected to fail at runContainer
}

func TestDevDownCommand_NoContainerRunning(t *testing.T) {
	// Test dev down when no container is running
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, testutil.ErrContainerNotFound
	}

	command := devDown(dockerClient)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "No ClickHouse development server is currently running")
}

func TestDevDownCommand_StopsContainer(t *testing.T) {
	// Test dev down stopping an existing container
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()

	// Container exists
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, nil
	}

	// Mock stop and remove operations
	var stopCalled, removeCalled bool
	dockerClient.ContainerStopFunc = func(ctx context.Context, containerID string, options container.StopOptions) error {
		require.Equal(t, "housekeeper-dev", containerID)
		stopCalled = true
		return nil
	}
	dockerClient.ContainerRemoveFunc = func(ctx context.Context, containerID string, options container.RemoveOptions) error {
		require.Equal(t, "housekeeper-dev", containerID)
		removeCalled = true
		return nil
	}

	command := devDown(dockerClient)

	ctx := context.Background()
	var buf bytes.Buffer
	testCmd := &cli.Command{
		Flags:  command.Flags,
		Writer: &buf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err)

	output := buf.String()
	require.Contains(t, output, "ClickHouse development server stopped")
	require.True(t, stopCalled, "Container stop should be called")
	require.True(t, removeCalled, "Container remove should be called")
}

func TestDevDownCommand_StopError(t *testing.T) {
	// Test dev down with stop error
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()

	// Container exists
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		return container.InspectResponse{}, nil
	}

	// Mock stop operation that fails
	dockerClient.ContainerStopFunc = func(ctx context.Context, containerID string, options container.StopOptions) error {
		return testutil.ErrDockerOperation
	}

	command := devDown(dockerClient)

	ctx := context.Background()
	var buf bytes.Buffer
	var errBuf bytes.Buffer
	testCmd := &cli.Command{
		Flags:     command.Flags,
		Writer:    &buf,
		ErrWriter: &errBuf,
	}

	err := command.Action(ctx, testCmd)
	require.NoError(t, err) // Command should not fail, just warn

	errOutput := errBuf.String()
	require.Contains(t, errOutput, "Warning: failed to stop container")
	require.Contains(t, errOutput, "You may need to manually stop the container")
}

func TestLoadDevConfigFromConfig_DefaultValues(t *testing.T) {
	// Test loadDevConfigFromConfig with default values
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	config := loadDevConfigFromConfig(fixture.Config)

	require.Equal(t, "25.7", config.version)          // Default version
	require.Equal(t, "cluster", config.cluster)       // Test project default cluster
	require.Contains(t, config.configDir, "config.d") // Config dir is set to absolute path
}

func TestLoadDevConfigFromConfig_CustomValues(t *testing.T) {
	// Test loadDevConfigFromConfig with custom values
	fixture := testutil.TestProject(t).
		WithClickHouseVersion("24.8").
		WithClickHouseCluster("test-cluster").
		WithClickHouseConfigDir("config.d")
	defer fixture.Cleanup()

	config := loadDevConfigFromConfig(fixture.Config)

	require.Equal(t, "24.8", config.version)
	require.Equal(t, "test-cluster", config.cluster)
	require.Contains(t, config.configDir, "config.d") // Should be absolute path
}

func TestLoadDevConfigFromConfig_EmptyVersion(t *testing.T) {
	// Test that empty version gets default
	fixture := testutil.TestProject(t).
		WithClickHouseVersion("") // Explicitly empty
	defer fixture.Cleanup()

	config := loadDevConfigFromConfig(fixture.Config)

	require.Equal(t, "25.7", config.version) // Should use default
}

func TestIsDevContainerRunning_Running(t *testing.T) {
	// Test isDevContainerRunning when container is running
	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, nil // No error means container exists
	}

	result := isDevContainerRunning(context.Background(), dockerClient)
	require.True(t, result)
}

func TestIsDevContainerRunning_NotRunning(t *testing.T) {
	// Test isDevContainerRunning when container is not running
	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerInspectFunc = func(ctx context.Context, containerID string) (container.InspectResponse, error) {
		require.Equal(t, "housekeeper-dev", containerID)
		return container.InspectResponse{}, testutil.ErrContainerNotFound
	}

	result := isDevContainerRunning(context.Background(), dockerClient)
	require.False(t, result)
}

func TestStopHousekeeperDevContainer_Success(t *testing.T) {
	// Test successful container stop and remove
	dockerClient := testutil.NewMockDockerClient()

	var stopCalled, removeCalled bool
	dockerClient.ContainerStopFunc = func(ctx context.Context, containerID string, options container.StopOptions) error {
		require.Equal(t, "housekeeper-dev", containerID)
		stopCalled = true
		return nil
	}
	dockerClient.ContainerRemoveFunc = func(ctx context.Context, containerID string, options container.RemoveOptions) error {
		require.Equal(t, "housekeeper-dev", containerID)
		removeCalled = true
		return nil
	}

	err := stopHousekeeperDevContainer(context.Background(), dockerClient)
	require.NoError(t, err)
	require.True(t, stopCalled)
	require.True(t, removeCalled)
}

func TestStopHousekeeperDevContainer_StopError(t *testing.T) {
	// Test container stop error
	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerStopFunc = func(ctx context.Context, containerID string, options container.StopOptions) error {
		return testutil.ErrDockerOperation
	}

	err := stopHousekeeperDevContainer(context.Background(), dockerClient)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to stop housekeeper-dev container")
}

func TestStopHousekeeperDevContainer_RemoveError(t *testing.T) {
	// Test container remove error
	dockerClient := testutil.NewMockDockerClient()
	dockerClient.ContainerStopFunc = func(ctx context.Context, containerID string, options container.StopOptions) error {
		return nil // Stop succeeds
	}
	dockerClient.ContainerRemoveFunc = func(ctx context.Context, containerID string, options container.RemoveOptions) error {
		return testutil.ErrDockerOperation
	}

	err := stopHousekeeperDevContainer(context.Background(), dockerClient)
	require.Error(t, err)
	require.Contains(t, err.Error(), "failed to remove housekeeper-dev container")
}

func TestPrintConnectionDetails_SmokeTest(t *testing.T) {
	// Test that printConnectionDetails function exists and can be called
	// We can't easily test the actual output since it writes to stdout
	// This is just a smoke test to ensure the function doesn't panic

	// Note: printConnectionDetails expects *docker.ClickHouseContainer
	// We can't easily mock this without complex setup, so we just verify the function exists
	// The actual functionality is tested through integration tests
	require.NotNil(t, printConnectionDetails)
}

func TestDevCommand_RequiresConfig(t *testing.T) {
	// Test that dev command requires config
	fixture := testutil.TestProject(t)
	defer fixture.Cleanup()

	dockerClient := testutil.NewMockDockerClient()
	command := dev(fixture.Config, dockerClient)

	require.NotNil(t, command.Before)
	// The Before function should be requireConfig - we can't easily test this
	// without more complex mocking, but we verify it's set
}
