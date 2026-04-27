package docker

import (
	"context"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"

	A "github.com/IBM/fp-go/v2/array"
	F "github.com/IBM/fp-go/v2/function"
	O "github.com/IBM/fp-go/v2/option"
	R "github.com/IBM/fp-go/v2/result"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/filters"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	"github.com/docker/go-connections/nat"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
)

// isContainerNotFound reports whether err indicates the container was not found.
// Handles Docker SDK error messages and the errdefs package across SDK versions.
func isContainerNotFound(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return A.Any(F.Bind1st(strings.Contains, msg))([]string{
		"no such container", "not found", "does not exist",
	})
}

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

	engine struct {
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

func newEngine(cl DockerClient) *engine {
	return &engine{client: cl}
}

func (c *engine) Pull(ctx context.Context, img string) error {
	out, err := c.client.ImagePull(ctx, img, image.PullOptions{})
	return R.ToError(F.Pipe1(
		R.TryCatchError(out, errors.Wrapf(err, "failed to pull image: %s", img)),
		R.Chain(func(out io.ReadCloser) R.Result[struct{}] {
			defer func() { _ = out.Close() }()
			_, _ = io.Copy(os.Stdout, out)
			return R.Of(struct{}{})
		}),
	))
}

func (c *engine) Start(ctx context.Context, opts ContainerOptions) error {
	env := make([]string, 0, len(opts.Env))
	for key, value := range opts.Env {
		env = append(env, fmt.Sprintf("%s=%s", key, value))
	}

	exposedPorts := make(nat.PortSet)
	portBindings := make(nat.PortMap)
	for hostPort, containerPort := range opts.Ports {
		port := nat.Port(fmt.Sprintf("%d/tcp", containerPort))
		exposedPorts[port] = struct{}{}
		hostPortStr := O.Fold(F.Constant(""), strconv.Itoa)(
			O.FromPredicate(func(p int) bool { return p > 0 })(hostPort),
		)
		portBindings[port] = []nat.PortBinding{{HostPort: hostPortStr}}
	}

	binds := A.Map(func(v ContainerVolume) string {
		return fmt.Sprintf("%s:%s", v.HostPath, v.ContainerPath) +
			O.Fold(F.Constant(""), F.Constant1[bool, string](":ro"))(O.FromNonZero[bool]()(v.ReadOnly))
	})(opts.Volumes)

	resp, err := c.client.ContainerCreate(
		ctx,
		&container.Config{Image: opts.Image, Env: env, ExposedPorts: exposedPorts},
		&container.HostConfig{PortBindings: portBindings, Binds: binds},
		nil, nil, opts.Name,
	)
	return R.ToError(F.Pipe1(
		R.TryCatchError(resp, errors.Wrapf(err, "failed to create container: %s", opts.Name)),
		R.Chain(func(resp container.CreateResponse) R.Result[struct{}] {
			return R.TryCatchError(struct{}{}, errors.Wrapf(
				c.client.ContainerStart(ctx, resp.ID, container.StartOptions{}),
				"failed to start container: %s", opts.Name,
			))
		}),
	))
}

func (c *engine) List(ctx context.Context) ([]*Container, error) {
	list, err := c.client.ContainerList(ctx, container.ListOptions{Filters: filters.NewArgs(runningContainers)})
	return R.UnwrapError(F.Pipe1(
		R.TryCatchError(list, errors.Wrap(err, "failed to list running containers")),
		R.Map(A.Map(func(s container.Summary) *Container {
			return &Container{
				Names:  A.Map(F.Bind2nd(strings.TrimPrefix, "/"))(s.Names),
				Image:  s.Image,
				State:  s.State,
				Status: s.Status,
			}
		})),
	))
}

func (c *engine) Stop(ctx context.Context, nameOrID string) error {
	timeout := 30
	return R.ToError(F.Pipe1(
		R.TryCatchError(struct{}{}, errors.Wrapf(
			c.client.ContainerStop(ctx, nameOrID, container.StopOptions{Timeout: &timeout}),
			"failed to stop container: %s", nameOrID,
		)),
		R.Chain(func(_ struct{}) R.Result[struct{}] {
			return R.TryCatchError(struct{}{}, errors.Wrapf(
				c.client.ContainerRemove(ctx, nameOrID, container.RemoveOptions{Force: true}),
				"failed to remove container: %s", nameOrID,
			))
		}),
	))
}

// StopIfExists stops and removes nameOrID if it exists, silently ignoring not-found errors.
// Callers can invoke this unconditionally before creating a container to avoid
// "name already in use" failures when a previous run left a stale container.
func (c *engine) StopIfExists(ctx context.Context, nameOrID string) error {
	timeout := 10
	orWrap := func(msg string) func(R.Result[struct{}]) R.Result[struct{}] {
		return R.OrElse[struct{}](func(err error) R.Result[struct{}] {
			if isContainerNotFound(err) {
				return R.Of(struct{}{})
			}
			return R.Left[struct{}](errors.Wrap(err, msg))
		})
	}
	return R.ToError(F.Pipe3(
		R.TryCatchError(struct{}{}, c.client.ContainerStop(ctx, nameOrID, container.StopOptions{Timeout: &timeout})),
		orWrap(fmt.Sprintf("failed to stop container %q", nameOrID)),
		R.Chain(func(_ struct{}) R.Result[struct{}] {
			return R.TryCatchError(struct{}{}, c.client.ContainerRemove(ctx, nameOrID, container.RemoveOptions{Force: true}))
		}),
		orWrap(fmt.Sprintf("failed to remove container %q", nameOrID)),
	))
}

func (c *engine) Get(ctx context.Context, nameOrID string) (*Container, error) {
	inspect, err := c.client.ContainerInspect(ctx, nameOrID)
	return R.UnwrapError(F.Pipe1(
		R.TryCatchError(inspect, errors.Wrapf(err, "failed to inspect container: %s", nameOrID)),
		R.Map(func(inspect container.InspectResponse) *Container {
			names := O.Fold(F.Constant([]string{}), F.Flow2(F.Bind2nd(strings.TrimPrefix, "/"), A.Of[string]))(
				O.FromNonZero[string]()(inspect.Name),
			)
			return &Container{
				Names:  names,
				Image:  inspect.Config.Image,
				State:  inspect.State.Status,
				Status: inspect.State.Status,
			}
		}),
	))
}
