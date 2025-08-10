package docker

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

var runningContainers = filters.Arg("status", "running")

type (
	// DockerClient defines the interface for Docker operations used by the Engine.
	// This interface is satisfied by *client.Client and allows for easy mocking in tests.
	DockerClient interface {
		ImagePull(context.Context, string, image.PullOptions) (io.ReadCloser, error)
		ContainerCreate(context.Context, *container.Config, *container.HostConfig, *network.NetworkingConfig, *v1.Platform, string) (container.CreateResponse, error)
		ContainerStart(context.Context, string, container.StartOptions) error
		ContainerList(context.Context, container.ListOptions) ([]container.Summary, error)
		ContainerStop(context.Context, string, container.StopOptions) error
		ContainerRemove(context.Context, string, container.RemoveOptions) error
		ContainerInspect(context.Context, string) (container.InspectResponse, error)
	}

	Engine struct {
		client DockerClient
	}

	Container struct {
		Names  []string
		Image  string
		State  string
		Status string
	}

	ContainerOptions struct {
		Name    string
		Image   string
		Env     map[string]string
		Ports   map[int]int
		Volumes []ContainerVolume
	}

	ContainerVolume struct {
		HostPath      string `yaml:"hostPath"`
		ContainerPath string `yaml:"containerPath"`
		ReadOnly      bool   `yaml:"readOnly"`
	}
)

// NewEngine creates a new Docker Engine instance for managing Docker operations.
// The Docker client should be initialized and connected before passing to this constructor.
//
// Example:
//
//	// Create Docker client
//	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer cli.Close()
//
//	// Create engine
//	engine := docker.NewEngine(cli)
//
//	// Pull an image
//	if err := engine.Pull(ctx, "clickhouse/clickhouse-server:latest"); err != nil {
//		log.Fatal(err)
//	}
func NewEngine(cl DockerClient) *Engine {
	return &Engine{
		client: cl,
	}
}

func (c *Engine) Pull(ctx context.Context, img string) error {
	out, err := c.client.ImagePull(ctx, img, image.PullOptions{})
	if err != nil {
		return errors.Wrapf(err, "failed to pull image: %s", img)
	}

	defer func() { _ = out.Close() }()
	_, _ = io.Copy(os.Stdout, out)
	return nil
}

func (c *Engine) Start(ctx context.Context, opts ContainerOptions) error {
	// Build environment variables
	env := make([]string, 0, len(opts.Env))
	for key, value := range opts.Env {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	// Build port bindings
	exposedPorts := make(nat.PortSet)
	portBindings := make(nat.PortMap)
	for hostPort, containerPort := range opts.Ports {
		port := nat.Port(fmt.Sprintf("%d/tcp", containerPort))
		exposedPorts[port] = struct{}{}

		// If hostPort is 0 or negative, let Docker assign a random port
		hostPortStr := ""
		if hostPort > 0 {
			hostPortStr = strconv.Itoa(hostPort)
		}

		portBindings[port] = []nat.PortBinding{
			{
				HostPort: hostPortStr,
			},
		}
	}

	// Build volume bindings
	binds := make([]string, len(opts.Volumes))
	for i, volume := range opts.Volumes {
		bind := fmt.Sprintf("%s:%s", volume.HostPath, volume.ContainerPath)
		if volume.ReadOnly {
			bind += ":ro"
		}
		binds[i] = bind
	}

	resp, err := c.client.ContainerCreate(
		ctx,
		&container.Config{
			Image:        opts.Image,
			Env:          env,
			ExposedPorts: exposedPorts,
		},
		&container.HostConfig{
			PortBindings: portBindings,
			Binds:        binds,
		},
		nil,
		nil,
		opts.Name,
	)
	if err != nil {
		return errors.Wrapf(err, "failed to create container: %s", opts.Name)
	}

	if err := c.client.ContainerStart(ctx, resp.ID, container.StartOptions{}); err != nil {
		return errors.Wrapf(err, "failed to start container: %s", opts.Name)
	}

	return nil
}

func (c *Engine) List(ctx context.Context) ([]*Container, error) {
	list, err := c.client.ContainerList(ctx, container.ListOptions{
		Filters: filters.NewArgs(runningContainers),
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to list running containers")
	}

	res := make([]*Container, len(list))
	for i, c := range list {
		// Map slice of names to remove leading "/" prefix
		names := make([]string, len(c.Names))
		for j, name := range c.Names {
			names[j] = strings.TrimPrefix(name, "/")
		}

		res[i] = &Container{
			Names:  names,
			Image:  c.Image,
			State:  c.State,
			Status: c.Status,
		}
	}

	return res, nil
}

func (c *Engine) Stop(ctx context.Context, nameOrID string) error {
	timeout := 30
	if err := c.client.ContainerStop(ctx, nameOrID, container.StopOptions{
		Timeout: &timeout,
	}); err != nil {
		return errors.Wrapf(err, "failed to stop container: %s", nameOrID)
	}

	if err := c.client.ContainerRemove(ctx, nameOrID, container.RemoveOptions{
		Force: true,
	}); err != nil {
		return errors.Wrapf(err, "failed to remove container: %s", nameOrID)
	}

	return nil
}

func (c *Engine) Get(ctx context.Context, nameOrID string) (*Container, error) {
	inspect, err := c.client.ContainerInspect(ctx, nameOrID)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to inspect container: %s", nameOrID)
	}

	names := make([]string, len(inspect.Name))
	if inspect.Name != "" {
		names = []string{strings.TrimPrefix(inspect.Name, "/")}
	}

	return &Container{
		Names:  names,
		Image:  inspect.Config.Image,
		State:  inspect.State.Status,
		Status: inspect.State.Status,
	}, nil
}
