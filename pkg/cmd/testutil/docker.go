package testutil

import (
	"context"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/docker/client"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/stretchr/testify/require"
)

// Common test errors
var (
	ErrContainerNotFound = errors.New("container not found")
	ErrDockerOperation   = errors.New("docker operation failed")
)

// SkipIfNoDocker skips the test if Docker is not available
func SkipIfNoDocker(t *testing.T) {
	t.Helper()

	// Check if Docker binary exists
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not available")
	}

	// Check if Docker daemon is running
	cmd := exec.CommandContext(t.Context(), "docker", "ps")
	if err := cmd.Run(); err != nil {
		t.Skip("Docker daemon not running")
	}
}

// SetupClickHouseContainer creates a ClickHouse container for testing
func SetupClickHouseContainer(t *testing.T, configDir string) *docker.ClickHouseContainer {
	t.Helper()

	SkipIfNoDocker(t)

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err, "Failed to create Docker client")

	// Ensure config directory exists and has basic config
	if configDir != "" {
		err := os.MkdirAll(configDir, consts.ModeDir)
		require.NoError(t, err, "Failed to create config directory")

		// Create a basic ClickHouse config if it doesn't exist
		configFile := filepath.Join(configDir, "config.xml")
		if _, err := os.Stat(configFile); os.IsNotExist(err) {
			configContent := BasicClickHouseConfig()
			err := os.WriteFile(configFile, []byte(configContent), consts.ModeFile)
			require.NoError(t, err, "Failed to write ClickHouse config")
		}
	}

	// Create container with test options
	opts := docker.DockerOptions{
		Version:   consts.DefaultClickHouseVersion,
		ConfigDir: configDir,
		Name:      "test-clickhouse-" + t.Name(),
	}

	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err, "Failed to create ClickHouse container")

	// Register cleanup
	t.Cleanup(func() {
		ctx := context.Background()
		_ = container.Stop(ctx)
	})

	return container
}

// StartClickHouseContainer starts a ClickHouse container and returns it
func StartClickHouseContainer(t *testing.T, configDir string) (*docker.ClickHouseContainer, string) {
	t.Helper()

	container := SetupClickHouseContainer(t, configDir)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start the container
	err := container.Start(ctx)
	require.NoError(t, err, "Failed to start ClickHouse container")

	// Get DSN
	dsn, err := container.GetDSN(ctx)
	require.NoError(t, err, "Failed to get container DSN")

	return container, dsn
}

// MockDockerClient creates a mock Docker client for testing that implements docker.DockerClient
type MockDockerClient struct {
	ImagePullFunc        func(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error)
	ContainerCreateFunc  func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error)
	ContainerStartFunc   func(ctx context.Context, containerID string, options container.StartOptions) error
	ContainerListFunc    func(ctx context.Context, options container.ListOptions) ([]container.Summary, error)
	ContainerStopFunc    func(ctx context.Context, containerID string, options container.StopOptions) error
	ContainerRemoveFunc  func(ctx context.Context, containerID string, options container.RemoveOptions) error
	ContainerInspectFunc func(ctx context.Context, containerID string) (container.InspectResponse, error)
}

// NewMockDockerClient creates a new mock Docker client with default implementations
func NewMockDockerClient() *MockDockerClient {
	return &MockDockerClient{
		ImagePullFunc: func(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error) {
			return io.NopCloser(strings.NewReader("pulling image")), nil
		},
		ContainerCreateFunc: func(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error) {
			return container.CreateResponse{ID: "mock-container-id"}, nil
		},
		ContainerStartFunc: func(ctx context.Context, containerID string, options container.StartOptions) error {
			return nil
		},
		ContainerListFunc: func(ctx context.Context, options container.ListOptions) ([]container.Summary, error) {
			return []container.Summary{}, nil
		},
		ContainerStopFunc: func(ctx context.Context, containerID string, options container.StopOptions) error {
			return nil
		},
		ContainerRemoveFunc: func(ctx context.Context, containerID string, options container.RemoveOptions) error {
			return nil
		},
		ContainerInspectFunc: func(ctx context.Context, containerID string) (container.InspectResponse, error) {
			return container.InspectResponse{}, ErrContainerNotFound
		},
	}
}

// ImagePull implements docker.DockerClient interface
func (m *MockDockerClient) ImagePull(ctx context.Context, refStr string, options image.PullOptions) (io.ReadCloser, error) {
	if m.ImagePullFunc != nil {
		return m.ImagePullFunc(ctx, refStr, options)
	}
	return io.NopCloser(strings.NewReader("pulling image")), nil
}

// ContainerCreate implements docker.DockerClient interface
func (m *MockDockerClient) ContainerCreate(ctx context.Context, config *container.Config, hostConfig *container.HostConfig, networkingConfig *network.NetworkingConfig, platform *v1.Platform, containerName string) (container.CreateResponse, error) {
	if m.ContainerCreateFunc != nil {
		return m.ContainerCreateFunc(ctx, config, hostConfig, networkingConfig, platform, containerName)
	}
	return container.CreateResponse{ID: "mock-container-id"}, nil
}

// ContainerStart implements docker.DockerClient interface
func (m *MockDockerClient) ContainerStart(ctx context.Context, containerID string, options container.StartOptions) error {
	if m.ContainerStartFunc != nil {
		return m.ContainerStartFunc(ctx, containerID, options)
	}
	return nil
}

// ContainerList implements docker.DockerClient interface
func (m *MockDockerClient) ContainerList(ctx context.Context, options container.ListOptions) ([]container.Summary, error) {
	if m.ContainerListFunc != nil {
		return m.ContainerListFunc(ctx, options)
	}
	return []container.Summary{}, nil
}

// ContainerStop implements docker.DockerClient interface
func (m *MockDockerClient) ContainerStop(ctx context.Context, containerID string, options container.StopOptions) error {
	if m.ContainerStopFunc != nil {
		return m.ContainerStopFunc(ctx, containerID, options)
	}
	return nil
}

// ContainerRemove implements docker.DockerClient interface
func (m *MockDockerClient) ContainerRemove(ctx context.Context, containerID string, options container.RemoveOptions) error {
	if m.ContainerRemoveFunc != nil {
		return m.ContainerRemoveFunc(ctx, containerID, options)
	}
	return nil
}

// ContainerInspect implements docker.DockerClient interface
func (m *MockDockerClient) ContainerInspect(ctx context.Context, containerID string) (container.InspectResponse, error) {
	if m.ContainerInspectFunc != nil {
		return m.ContainerInspectFunc(ctx, containerID)
	}
	return container.InspectResponse{}, ErrContainerNotFound
}

// MockClickHouseContainer creates a mock ClickHouse container for testing
type MockClickHouseContainer struct {
	GetDSNFunc     func(ctx context.Context) (string, error)
	GetHTTPDSNFunc func(ctx context.Context) (string, error)
	StartFunc      func(ctx context.Context) error
	StopFunc       func(ctx context.Context) error
	IsRunningFunc  func() bool
}

// NewMockClickHouseContainer creates a new mock ClickHouse container with default implementations
func NewMockClickHouseContainer() *MockClickHouseContainer {
	return &MockClickHouseContainer{
		GetDSNFunc: func(ctx context.Context) (string, error) {
			return "localhost:9000", nil
		},
		GetHTTPDSNFunc: func(ctx context.Context) (string, error) {
			return "http://localhost:8123", nil
		},
		StartFunc: func(ctx context.Context) error {
			return nil
		},
		StopFunc: func(ctx context.Context) error {
			return nil
		},
		IsRunningFunc: func() bool {
			return true
		},
	}
}

// GetDSN implements the ClickHouseContainer interface
func (m *MockClickHouseContainer) GetDSN(ctx context.Context) (string, error) {
	if m.GetDSNFunc != nil {
		return m.GetDSNFunc(ctx)
	}
	return "localhost:9000", nil
}

// GetHTTPDSN implements the ClickHouseContainer interface
func (m *MockClickHouseContainer) GetHTTPDSN(ctx context.Context) (string, error) {
	if m.GetHTTPDSNFunc != nil {
		return m.GetHTTPDSNFunc(ctx)
	}
	return "http://localhost:8123", nil
}

// Start implements the ClickHouseContainer interface
func (m *MockClickHouseContainer) Start(ctx context.Context) error {
	if m.StartFunc != nil {
		return m.StartFunc(ctx)
	}
	return nil
}

// Stop implements the ClickHouseContainer interface
func (m *MockClickHouseContainer) Stop(ctx context.Context) error {
	if m.StopFunc != nil {
		return m.StopFunc(ctx)
	}
	return nil
}

// IsRunning implements the ClickHouseContainer interface
func (m *MockClickHouseContainer) IsRunning() bool {
	if m.IsRunningFunc != nil {
		return m.IsRunningFunc()
	}
	return true
}

// BasicClickHouseConfig returns a basic ClickHouse configuration for testing
func BasicClickHouseConfig() string {
	return `<?xml version="1.0"?>
<clickhouse>
    <logger>
        <level>warning</level>
        <console>true</console>
    </logger>
    <listen_host>0.0.0.0</listen_host>
    <http_port>8123</http_port>
    <tcp_port>9000</tcp_port>
    <users>
        <default>
            <password></password>
            <networks>
                <ip>::/0</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
        </default>
    </users>
    <profiles>
        <default>
            <max_memory_usage>10000000000</max_memory_usage>
            <use_uncompressed_cache>0</use_uncompressed_cache>
            <load_balancing>random</load_balancing>
        </default>
    </profiles>
    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>`
}

// WaitForClickHouse waits for ClickHouse to be ready
func WaitForClickHouse(ctx context.Context, dsn string, maxRetries int) error {
	// Simple connection check - in real implementation, you'd use the ClickHouse client
	// For now, this is a placeholder
	time.Sleep(2 * time.Second)
	return nil
}

// CreateTestContainer creates a test container with custom options
func CreateTestContainer(t *testing.T, version, configDir, name string) *docker.ClickHouseContainer {
	t.Helper()

	SkipIfNoDocker(t)

	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	require.NoError(t, err, "Failed to create Docker client")

	// Use defaults if not provided
	if version == "" {
		version = consts.DefaultClickHouseVersion
	}
	if name == "" {
		name = "test-clickhouse-" + t.Name()
	}

	opts := docker.DockerOptions{
		Version:   version,
		ConfigDir: configDir,
		Name:      name,
	}

	container, err := docker.NewWithOptions(dockerClient, opts)
	require.NoError(t, err, "Failed to create ClickHouse container")

	// Register cleanup
	t.Cleanup(func() {
		ctx := context.Background()
		_ = container.Stop(ctx)
	})

	return container
}

// ContainerWithMigrations creates a container and applies the given migrations
func ContainerWithMigrations(t *testing.T, fixture *ProjectFixture, migrations []MigrationFile) (*docker.ClickHouseContainer, string) {
	t.Helper()

	// Add migrations to the fixture
	fixture.WithMigrations(migrations)

	// Start container
	configDir := filepath.Join(fixture.Dir, fixture.Config.ClickHouse.ConfigDir)
	container, dsn := StartClickHouseContainer(t, configDir)

	// TODO: Apply migrations using the ClickHouse client
	// This would require importing and using the clickhouse package
	// For now, we just return the container and DSN

	return container, dsn
}

// RequireDockerAvailable ensures Docker is available or fails the test
func RequireDockerAvailable(t *testing.T) {
	t.Helper()

	// Check if Docker binary exists
	if _, err := exec.LookPath("docker"); err != nil {
		t.Fatal("Docker is required for this test but not available")
	}

	// Check if Docker daemon is running
	cmd := exec.CommandContext(t.Context(), "docker", "ps")
	if err := cmd.Run(); err != nil {
		t.Fatal("Docker daemon is required for this test but not running")
	}
}
