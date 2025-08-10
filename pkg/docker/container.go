package docker

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/docker/docker/client"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
)

const (
	// DefaultClickHousePort is the default port for ClickHouse server
	DefaultClickHousePort = 9000

	// DefaultClickHouseHTTPPort is the default HTTP port for ClickHouse server
	DefaultClickHouseHTTPPort = 8123
)

type (
	// DockerOptions represents options for running ClickHouse in Docker
	DockerOptions struct {
		// Version is the ClickHouse version to run (default: latest)
		Version string

		// ConfigDir is the optional ClickHouse config directory path to mount (relative paths will be converted to absolute)
		ConfigDir string

		// Name is the container name (default: housekeeper-dev)
		Name string
	}

	// ClickHouseContainer manages ClickHouse Docker containers for development
	ClickHouseContainer struct {
		options DockerOptions
		engine  *Engine
		running bool
		ports   map[int]int // hostPort -> containerPort mapping
	}
)

// New creates a new ClickHouse Docker container with default options
//
// Example:
//
//	container := docker.New()
//
//	// Start ClickHouse container
//	if err := container.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//	defer container.Stop(ctx)
func New() (*ClickHouseContainer, error) {
	return NewWithOptions(DockerOptions{})
}

// NewWithOptions creates a new ClickHouse Docker container with custom options
//
// Example:
//
//	opts := docker.DockerOptions{
//		Version:   "25.7",
//		ConfigDir: "/path/to/project/db/config.d",
//	}
//	container, err := docker.NewWithOptions(opts)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Start ClickHouse container
//	if err := container.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//	defer container.Stop(ctx)
func NewWithOptions(opts DockerOptions) (*ClickHouseContainer, error) {
	// Create Docker client
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return nil, errors.Wrap(err, "failed to create Docker client")
	}

	engine := NewEngine(dockerClient)

	return &ClickHouseContainer{
		options: opts,
		engine:  engine,
		running: false,
		ports:   make(map[int]int),
	}, nil
}

// Start starts a ClickHouse Docker container with the configured version
func (c *ClickHouseContainer) Start(ctx context.Context) error {
	if c.running {
		return errors.New("container is already running")
	}

	// Determine ClickHouse version to use
	version := c.options.Version
	if version == "" {
		version = "latest"
	}

	// Determine container name to use
	containerName := c.options.Name
	if containerName == "" {
		containerName = "housekeeper-dev"
	}

	// Build container options
	containerOpts := ContainerOptions{
		Name:  containerName,
		Image: fmt.Sprintf("clickhouse/clickhouse-server:%s-alpine", version),
		Env: map[string]string{
			"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1",
		},
		Ports: map[int]int{
			// Use negative values as placeholders for dynamic port assignment
			-1: DefaultClickHousePort,     // Let Docker assign random host port for native port 9000
			-2: DefaultClickHouseHTTPPort, // Let Docker assign random host port for HTTP port 8123
		},
	}

	// Add config directory mount if specified
	if c.options.ConfigDir != "" {
		// Convert to absolute path to ensure proper mounting
		absConfigDir, err := filepath.Abs(c.options.ConfigDir)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path for ConfigDir: %s", c.options.ConfigDir)
		}

		containerOpts.Volumes = []ContainerVolume{
			{
				HostPath:      absConfigDir,
				ContainerPath: "/etc/clickhouse-server/config.d",
				ReadOnly:      true,
			},
		}
	}

	// Pull the image first
	if err := c.engine.Pull(ctx, containerOpts.Image); err != nil {
		return errors.Wrap(err, "failed to pull ClickHouse image")
	}

	// Start the container
	if err := c.engine.Start(ctx, containerOpts); err != nil {
		return errors.Wrap(err, "failed to start ClickHouse container")
	}

	c.running = true

	// Wait for ClickHouse to be ready
	if err := c.waitForReady(ctx); err != nil {
		return errors.Wrap(err, "ClickHouse container failed to become ready")
	}

	return nil
}

// waitForReady waits for ClickHouse to be ready to accept connections
func (c *ClickHouseContainer) waitForReady(ctx context.Context) error {
	// Simple wait implementation - just wait a few seconds
	// In a more sophisticated implementation, we would check HTTP endpoint
	select {
	case <-time.After(10 * time.Second):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Stop stops and removes the ClickHouse Docker container
func (c *ClickHouseContainer) Stop(ctx context.Context) error {
	if !c.running {
		return nil // Already stopped
	}

	// Determine container name
	containerName := c.options.Name
	if containerName == "" {
		containerName = "housekeeper-dev"
	}

	err := c.engine.Stop(ctx, containerName)
	c.running = false

	if err != nil {
		return errors.Wrap(err, "failed to stop ClickHouse container")
	}

	return nil
}

// GetDSN returns the DSN for connecting to the Docker ClickHouse instance
func (c *ClickHouseContainer) GetDSN(ctx context.Context) (string, error) {
	if !c.running {
		return "", errors.New("container is not running")
	}

	// Determine container name
	containerName := c.options.Name
	if containerName == "" {
		containerName = "housekeeper-dev"
	}

	// Inspect container to get port mapping
	inspect, err := c.engine.client.ContainerInspect(ctx, containerName)
	if err != nil {
		return "", errors.Wrap(err, "failed to inspect container")
	}

	// Find the mapped port for ClickHouse native port (9000)
	nativePort := nat.Port(fmt.Sprintf("%d/tcp", DefaultClickHousePort))
	bindings, exists := inspect.NetworkSettings.Ports[nativePort]
	if !exists || len(bindings) == 0 {
		return "", errors.New("ClickHouse native port not exposed")
	}

	host := "localhost"
	port := bindings[0].HostPort

	return fmt.Sprintf("clickhouse://default:@%s:%s/", host, port), nil
}

// GetHTTPDSN returns the HTTP DSN for connecting to the Docker ClickHouse instance
func (c *ClickHouseContainer) GetHTTPDSN(ctx context.Context) (string, error) {
	if !c.running {
		return "", errors.New("container is not running")
	}

	// Determine container name
	containerName := c.options.Name
	if containerName == "" {
		containerName = "housekeeper-dev"
	}

	// Inspect container to get port mapping
	inspect, err := c.engine.client.ContainerInspect(ctx, containerName)
	if err != nil {
		return "", errors.Wrap(err, "failed to inspect container")
	}

	// Find the mapped port for ClickHouse HTTP port (8123)
	httpPort := nat.Port(fmt.Sprintf("%d/tcp", DefaultClickHouseHTTPPort))
	bindings, exists := inspect.NetworkSettings.Ports[httpPort]
	if !exists || len(bindings) == 0 {
		return "", errors.New("ClickHouse HTTP port not exposed")
	}

	host := "localhost"
	port := bindings[0].HostPort

	return fmt.Sprintf("http://%s:%s", host, port), nil
}

// IsRunning returns true if the container is currently running
func (c *ClickHouseContainer) IsRunning() bool {
	return c.running
}
