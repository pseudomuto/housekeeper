package docker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/docker/docker/client"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/stretchr/testify/require"
)

// skipIfNoDocker skips the test if Docker is not available
func skipIfNoDocker(t *testing.T) {
	t.Helper()

	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not available")
	}

	// Check if Docker daemon is running
	cmd := exec.Command("docker", "ps")
	if err := cmd.Run(); err != nil {
		t.Skip("Docker daemon not running")
	}
}

// setupDockerContainer creates a Docker container for testing
func setupDockerContainer(t *testing.T, tmpDir string, containerName string) *docker.ClickHouseContainer {
	t.Helper()

	// Create config directory structure
	configDir := filepath.Join(tmpDir, "config.d")
	require.NoError(t, os.MkdirAll(configDir, consts.ModeDir))

	// Create basic ClickHouse config for testing
	configContent := `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>true</console>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
</clickhouse>`

	configFile := filepath.Join(configDir, "config.xml")
	require.NoError(t, os.WriteFile(configFile, []byte(configContent), consts.ModeFile))

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	// Create Docker container with test options
	opts := docker.DockerOptions{
		Version:   "25.7",
		ConfigDir: configDir,
		Name:      containerName,
	}

	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err)
	return container
}

func TestDockerContainer_StartStop(t *testing.T) {
	skipIfNoDocker(t)

	tmpDir := t.TempDir()
	container := setupDockerContainer(t, tmpDir, "test-clickhouse-start-stop")

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up any existing container first
	_ = container.Stop(ctx)

	// Clean up at end
	defer func() {
		_ = container.Stop(ctx)
	}()

	// Start the container
	err := container.Start(ctx)
	require.NoError(t, err, "Failed to start ClickHouse container")

	// Verify DSN is available
	dsn, err := container.GetDSN(ctx)
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")

	httpDSN, err := container.GetHTTPDSN(ctx)
	require.NoError(t, err)
	require.Contains(t, httpDSN, "http://", "HTTP DSN should start with http://")

	// Stop the container
	err = container.Stop(ctx)
	require.NoError(t, err, "Failed to stop ClickHouse container")
}

func TestDockerContainer_WithCustomOptions(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping Docker tests in short mode")
	}

	skipIfNoDocker(t)

	tmpDir := t.TempDir()

	// Create config directory structure
	configDir := filepath.Join(tmpDir, "config.d")
	require.NoError(t, os.MkdirAll(configDir, consts.ModeDir))

	// Create basic ClickHouse config for testing
	configContent := `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>true</console>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
</clickhouse>`

	configFile := filepath.Join(configDir, "config.xml")
	require.NoError(t, os.WriteFile(configFile, []byte(configContent), consts.ModeFile))

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	// Use custom options with different version
	opts := docker.DockerOptions{
		Version:   "24.3", // Different version for testing
		ConfigDir: configDir,
		Name:      "test-clickhouse-custom",
	}
	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up any existing container first
	_ = container.Stop(ctx)

	// Clean up at end
	defer func() {
		_ = container.Stop(ctx)
	}()

	// Start the container
	err = container.Start(ctx)
	require.NoError(t, err)

	// Verify DSN is available (testcontainers assigns dynamic ports)
	dsn, err := container.GetDSN(ctx)
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")

	httpDSN, err := container.GetHTTPDSN(ctx)
	require.NoError(t, err)
	require.Contains(t, httpDSN, "http://", "HTTP DSN should start with http://")
}

func TestDockerContainer_StopNonExistent(t *testing.T) {
	skipIfNoDocker(t)

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	// Use custom version for testing without config
	opts := docker.DockerOptions{
		Version: "24.3",
		Name:    "test-clickhouse-stop-nonexistent",
	}
	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err)

	ctx := context.Background()

	// Stop should not error if container doesn't exist
	err = container.Stop(ctx)
	require.NoError(t, err)
}

func TestDockerContainer_RelativeConfigDir(t *testing.T) {
	skipIfNoDocker(t)

	tmpDir := t.TempDir()

	// Create config directory structure for relative path testing
	relativeConfigDir := "config.d"
	fullConfigDir := filepath.Join(tmpDir, relativeConfigDir)
	require.NoError(t, os.MkdirAll(fullConfigDir, 0o755))

	// Create basic ClickHouse config for testing
	configContent := `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>true</console>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
</clickhouse>`

	configFile := filepath.Join(fullConfigDir, "config.xml")
	require.NoError(t, os.WriteFile(configFile, []byte(configContent), consts.ModeFile))

	// Change to tmpDir to test relative path resolution
	require.NoError(t, os.Chdir(tmpDir))
	defer func() {
		// Change back to original directory
		_ = os.Chdir("/Users/pseudomuto/src/github.com/pseudomuto/housekeeper")
	}()

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err)

	// Use relative path in options to test conversion to absolute
	opts := docker.DockerOptions{
		Version:   "24.3",
		ConfigDir: relativeConfigDir, // Use relative path
		Name:      "test-clickhouse-relative",
	}
	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up any existing container first
	_ = container.Stop(ctx)

	// Clean up at end
	defer func() {
		_ = container.Stop(ctx)
	}()

	// Start should work with relative ConfigDir (converted to absolute internally)
	err = container.Start(ctx)
	require.NoError(t, err, "Failed to start ClickHouse container with relative ConfigDir")

	// Verify DSN is available
	dsn, err := container.GetDSN(ctx)
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")
}
