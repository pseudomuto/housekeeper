package docker_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/stretchr/testify/require"
)

// skipIfNoDocker skips the test if Docker is not available
func skipIfNoDocker(t *testing.T) {
	t.Helper()
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
func setupDockerContainer(t *testing.T, tmpDir string) *docker.Container {
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

	// Create Docker container with test options
	opts := docker.DockerOptions{
		Version:   "25.7",
		ConfigDir: configDir,
	}

	return docker.NewWithOptions(opts)
}

func TestDockerContainer_StartStop(t *testing.T) {
	skipIfNoDocker(t)

	tmpDir := t.TempDir()
	container := setupDockerContainer(t, tmpDir)

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
	dsn, err := container.GetDSN()
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")

	httpDSN, err := container.GetHTTPDSN()
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

	// Use custom options with different version
	opts := docker.DockerOptions{
		Version:   "24.3", // Different version for testing
		ConfigDir: configDir,
	}
	container := docker.NewWithOptions(opts)

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
	require.NoError(t, err)

	// Verify DSN is available (testcontainers assigns dynamic ports)
	dsn, err := container.GetDSN()
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")

	httpDSN, err := container.GetHTTPDSN()
	require.NoError(t, err)
	require.Contains(t, httpDSN, "http://", "HTTP DSN should start with http://")
}

func TestDockerContainer_StopNonExistent(t *testing.T) {
	skipIfNoDocker(t)

	// Use custom version for testing without config
	opts := docker.DockerOptions{
		Version: "24.3",
	}
	container := docker.NewWithOptions(opts)

	ctx := context.Background()

	// Stop should not error if container doesn't exist
	err := container.Stop(ctx)
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

	// Use relative path in options to test conversion to absolute
	opts := docker.DockerOptions{
		Version:   "24.3",
		ConfigDir: relativeConfigDir, // Use relative path
	}
	container := docker.NewWithOptions(opts)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Clean up any existing container first
	_ = container.Stop(ctx)

	// Clean up at end
	defer func() {
		_ = container.Stop(ctx)
	}()

	// Start should work with relative ConfigDir (converted to absolute internally)
	err := container.Start(ctx)
	require.NoError(t, err, "Failed to start ClickHouse container with relative ConfigDir")

	// Verify DSN is available
	dsn, err := container.GetDSN()
	require.NoError(t, err)
	require.Contains(t, dsn, ":", "DSN should contain host:port")
}
