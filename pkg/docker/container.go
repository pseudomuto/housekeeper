package docker

import (
	"context"
	"fmt"
	"net"
	"path/filepath"
	"time"

	F "github.com/IBM/fp-go/v2/function"
	O "github.com/IBM/fp-go/v2/option"
	R "github.com/IBM/fp-go/v2/result"
	"github.com/pkg/errors"
)

const (
	DefaultClickHousePort     = 9000
	DefaultClickHouseHTTPPort = 8123

	readinessDeadline     = 120 * time.Second
	readinessPollInterval = 500 * time.Millisecond
	readinessDialTimeout  = 2 * time.Second
)

type (
	// DockerOptions configures a ClickHouseContainer.
	DockerOptions struct {
		// Version is the ClickHouse image tag to use (default: latest).
		Version string

		// ConfigDir is an optional host path to mount as /etc/clickhouse-server/config.d.
		// Relative paths are resolved to absolute before mounting.
		ConfigDir string

		// Name is the container name (default: housekeeper-dev).
		Name string

		// AddressResolver determines how to obtain the container's network address.
		// When nil, DockerAddressResolver is used (localhost + mapped host ports).
		// Use podman.NewPodmanAddressResolver() or podman.NewAutoDetectResolver() for Podman.
		AddressResolver AddressResolver
	}

	// ClickHouseContainer manages a ClickHouse container for local development.
	ClickHouseContainer struct {
		options         DockerOptions
		engine          *engine
		running         bool
		addressResolver AddressResolver
	}
)

// New creates a ClickHouseContainer with default options.
func New(dockerClient DockerClient) (*ClickHouseContainer, error) {
	return NewWithOptions(dockerClient, DockerOptions{})
}

// NewWithOptions creates a ClickHouseContainer with the given options.
func NewWithOptions(dockerClient DockerClient, opts DockerOptions) (*ClickHouseContainer, error) {
	resolver := opts.AddressResolver
	if resolver == nil {
		resolver = &DockerAddressResolver{}
	}
	return &ClickHouseContainer{
		options:         opts,
		engine:          newEngine(dockerClient),
		addressResolver: resolver,
	}, nil
}

func (c *ClickHouseContainer) containerName() string {
	return O.MonadGetOrElse(O.FromNonZero[string]()(c.options.Name), F.Constant("housekeeper-dev"))
}

// Start pulls the image, cleans up any stale container with the same name, then starts a
// fresh container and waits for ClickHouse to accept TCP connections.
//
// Stale cleanup prevents "name already in use" failures when a previous run was interrupted
// before Stop was called.
func (c *ClickHouseContainer) Start(ctx context.Context) error {
	if c.running {
		return errors.New("container is already running")
	}

	version := O.MonadGetOrElse(O.FromNonZero[string]()(c.options.Version), F.Constant("latest"))

	containerName := c.containerName()

	if err := c.engine.StopIfExists(ctx, containerName); err != nil {
		return errors.Wrapf(err, "failed to clean up existing container %q", containerName)
	}

	containerOpts := ContainerOptions{
		Name:  containerName,
		Image: fmt.Sprintf("clickhouse/clickhouse-server:%s-alpine", version),
		Env:   map[string]string{"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"},
		Ports: map[int]int{
			-1: DefaultClickHousePort,
			-2: DefaultClickHouseHTTPPort,
		},
	}

	if c.options.ConfigDir != "" {
		absConfigDir, err := filepath.Abs(c.options.ConfigDir)
		if err != nil {
			return errors.Wrapf(err, "failed to get absolute path for ConfigDir: %s", c.options.ConfigDir)
		}
		containerOpts.Volumes = []ContainerVolume{{
			HostPath:      absConfigDir,
			ContainerPath: "/etc/clickhouse-server/config.d",
			ReadOnly:      true,
		}}
	}

	return R.ToError(F.Pipe2(
		R.TryCatchError(struct{}{}, errors.Wrap(c.engine.Pull(ctx, containerOpts.Image), "failed to pull ClickHouse image")),
		R.Chain(func(_ struct{}) R.Result[struct{}] {
			return R.TryCatchError(struct{}{}, errors.Wrap(c.engine.Start(ctx, containerOpts), "failed to start ClickHouse container"))
		}),
		R.Chain(func(_ struct{}) R.Result[struct{}] {
			c.running = true
			return R.TryCatchError(struct{}{}, errors.Wrap(c.waitForReady(ctx), "ClickHouse container failed to become ready"))
		}),
	))
}

// waitForReady polls the ClickHouse native port via TCP until it accepts connections
// or readinessDeadline elapses. The address is resolved through the injected
// AddressResolver so Podman bridge IPs work without special-casing.
func (c *ClickHouseContainer) waitForReady(ctx context.Context) error {
	addr, err := c.addressResolver.Resolve(ctx, c.engine.client, c.containerName())
	if err != nil {
		return errors.Wrap(err, "failed to resolve container address for readiness check")
	}

	target := net.JoinHostPort(addr.Host, fmt.Sprintf("%d", addr.NativePort))
	deadline := time.Now().Add(readinessDeadline)

	for time.Now().Before(deadline) {
		conn, dialErr := net.DialTimeout("tcp", target, readinessDialTimeout)
		if dialErr == nil {
			_ = conn.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(readinessPollInterval):
		}
	}

	return errors.Errorf("ClickHouse at %s failed to become ready within %s", target, readinessDeadline)
}

// Stop stops and removes the container.
func (c *ClickHouseContainer) Stop(ctx context.Context) error {
	if !c.running {
		return nil
	}
	err := c.engine.Stop(ctx, c.containerName())
	c.running = false
	if err != nil {
		return errors.Wrap(err, "failed to stop ClickHouse container")
	}
	return nil
}

// GetDSN returns the native-protocol DSN for the running container.
func (c *ClickHouseContainer) GetDSN(ctx context.Context) (string, error) {
	if !c.running {
		return "", errors.New("container is not running")
	}
	addr, err := c.addressResolver.Resolve(ctx, c.engine.client, c.containerName())
	return R.UnwrapError(F.Pipe1(
		R.TryCatchError(addr, errors.Wrap(err, "failed to resolve container address")),
		R.Map(func(addr *ContainerAddress) string {
			return fmt.Sprintf("clickhouse://default:@%s:%d/", addr.Host, addr.NativePort)
		}),
	))
}

// GetHTTPDSN returns the HTTP DSN for the running container.
func (c *ClickHouseContainer) GetHTTPDSN(ctx context.Context) (string, error) {
	if !c.running {
		return "", errors.New("container is not running")
	}
	addr, err := c.addressResolver.Resolve(ctx, c.engine.client, c.containerName())
	return R.UnwrapError(F.Pipe1(
		R.TryCatchError(addr, errors.Wrap(err, "failed to resolve container address")),
		R.Map(func(addr *ContainerAddress) string {
			return fmt.Sprintf("http://%s:%d", addr.Host, addr.HTTPPort)
		}),
	))
}

// IsRunning reports whether the container is currently running.
func (c *ClickHouseContainer) IsRunning() bool {
	return c.running
}
