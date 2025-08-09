package docker

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/mount"
	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/clickhouse"
	"github.com/testcontainers/testcontainers-go/wait"
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
	}

	// Container manages ClickHouse Docker containers for migration testing
	Container struct {
		options   DockerOptions
		container *clickhouse.ClickHouseContainer
	}
)

// New creates a new Docker container with default options
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
func New() *Container {
	return &Container{
		options: DockerOptions{},
	}
}

// NewWithOptions creates a new Docker container with custom options
//
// Example:
//
//	opts := docker.DockerOptions{
//		Version:   "25.7",
//		ConfigDir: "/path/to/project/db/config.d",
//	}
//	container := docker.NewWithOptions(opts)
//
//	// Start ClickHouse container
//	if err := container.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//	defer container.Stop(ctx)
func NewWithOptions(opts DockerOptions) *Container {
	return &Container{
		options: opts,
	}
}

// Start starts a ClickHouse Docker container with the configured version
func (c *Container) Start(ctx context.Context) error {
	if c.container != nil {
		return errors.New("container is already running")
	}

	// Determine ClickHouse version to use
	version := c.options.Version
	if version == "" {
		version = "latest"
	}

	// Build list of container customizers
	customizers := []testcontainers.ContainerCustomizer{
		clickhouse.WithUsername("default"),
		clickhouse.WithPassword(""),
		testcontainers.WithEnv(map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}),
		testcontainers.WithWaitStrategyAndDeadline(
			5*time.Minute,
			wait.
				NewHTTPStrategy("/").
				WithPort(nat.Port("8123/tcp")).
				WithStatusCodeMatcher(func(status int) bool {
					return status == 200
				}),
		),
	}

	// Add config directory mount if specified
	if c.options.ConfigDir != "" {
		// Convert to absolute path to ensure proper mounting
		absConfigDir, err := filepath.Abs(c.options.ConfigDir)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path for ConfigDir: %s", c.options.ConfigDir)
		}

		// Use HostConfigModifier instead of deprecated BindMount
		customizers = append(
			customizers,
			testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
				hostConfig.Mounts = []mount.Mount{
					{
						Type:   mount.TypeBind,
						Source: absConfigDir,
						Target: "/etc/clickhouse-server/config.d",
					},
				}
			}),
		)
	}

	// Start the container using the ClickHouse module (Run instead of deprecated RunContainer)
	container, err := clickhouse.Run(ctx,
		fmt.Sprintf("clickhouse/clickhouse-server:%s-alpine", version),
		customizers...,
	)
	if err != nil {
		return errors.Wrap(err, "failed to start ClickHouse container")
	}

	c.container = container
	return nil
}

// Stop stops and removes the ClickHouse Docker container
func (c *Container) Stop(ctx context.Context) error {
	if c.container == nil {
		return nil // Already stopped
	}

	err := c.container.Terminate(ctx)
	c.container = nil

	if err != nil {
		return errors.Wrap(err, "failed to stop ClickHouse container")
	}

	return nil
}

// GetDSN returns the DSN for connecting to the Docker ClickHouse instance
func (c *Container) GetDSN() (string, error) {
	if c.container == nil {
		return "", errors.New("container is not running")
	}

	// Use the ClickHouse container's built-in connection string method
	// This handles authentication automatically
	connectionString, err := c.container.ConnectionString(context.Background())
	if err != nil {
		return "", errors.Wrap(err, "failed to get connection string")
	}

	return connectionString, nil
}

// GetHTTPDSN returns the HTTP DSN for connecting to the Docker ClickHouse instance
func (c *Container) GetHTTPDSN() (string, error) {
	if c.container == nil {
		return "", errors.New("container is not running")
	}

	host, err := c.container.Host(context.Background())
	if err != nil {
		return "", errors.Wrap(err, "failed to get container host")
	}

	port, err := c.container.MappedPort(context.Background(), "8123/tcp")
	if err != nil {
		return "", errors.Wrap(err, "failed to get container port")
	}

	return fmt.Sprintf("http://%s:%s", host, port.Port()), nil
}

// IsRunning returns true if the container is currently running
func (c *Container) IsRunning() bool {
	return c.container != nil
}
