package docker

import (
	"context"
	"fmt"
	"strconv"

	"github.com/docker/go-connections/nat"
	"github.com/pkg/errors"
)

// ContainerAddress holds the resolved network coordinates for a running container.
// Different container runtimes expose connectivity differently; this struct normalises
// the result so the rest of the codebase never needs to know the origin.
type ContainerAddress struct {
	// Host is the IP address or hostname used to reach the container.
	Host string

	// NativePort is the ClickHouse native (binary) protocol port.
	NativePort int

	// HTTPPort is the ClickHouse HTTP interface port.
	HTTPPort int
}

// AddressResolver resolves the network address of a running container.
//
// Strategy Pattern: different runtimes (Docker, Podman) require fundamentally different
// address resolution strategies:
//
//   - Docker publishes each container port to a randomly-assigned host port on localhost.
//     The correct address is localhost:<randomHostPort>.
//
//   - Podman rootless does not reliably forward ports to localhost through the compat API.
//     The correct address is the container's direct bridge IP + the container port.
//
// Callers inject the desired resolver into DockerOptions.AddressResolver. When nil,
// ClickHouseContainer defaults to DockerAddressResolver (Docker-compatible behaviour).
// Use the podman package to obtain a Podman-aware resolver.
type AddressResolver interface {
	// Resolve returns the network address for the named container.
	// The provided DockerClient is used solely for inspection; callers must not mutate it.
	Resolve(ctx context.Context, client DockerClient, containerName string) (*ContainerAddress, error)
}

// DockerAddressResolver is the default AddressResolver for standard Docker environments.
//
// It reads the randomly-assigned host-port bindings that Docker creates when a container
// is started with published ports and returns localhost:<hostPort> for each service.
// This is the correct strategy for Docker Desktop, Docker Engine, and any runtime whose
// compat API properly sets up port-forwarding to the loopback interface.
type DockerAddressResolver struct{}

// Resolve implements AddressResolver for Docker.
func (r *DockerAddressResolver) Resolve(ctx context.Context, client DockerClient, containerName string) (*ContainerAddress, error) {
	inspect, err := client.ContainerInspect(ctx, containerName)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to inspect container %q", containerName)
	}

	nativePort := nat.Port(fmt.Sprintf("%d/tcp", DefaultClickHousePort))
	nativeBindings, ok := inspect.NetworkSettings.Ports[nativePort]
	if !ok || len(nativeBindings) == 0 {
		return nil, errors.Errorf("container %q: native port %d is not mapped", containerName, DefaultClickHousePort)
	}

	httpPort := nat.Port(fmt.Sprintf("%d/tcp", DefaultClickHouseHTTPPort))
	httpBindings, ok := inspect.NetworkSettings.Ports[httpPort]
	if !ok || len(httpBindings) == 0 {
		return nil, errors.Errorf("container %q: HTTP port %d is not mapped", containerName, DefaultClickHouseHTTPPort)
	}

	nativeMapped, err := strconv.Atoi(nativeBindings[0].HostPort)
	if err != nil {
		return nil, errors.Wrapf(err, "container %q: invalid native host port %q", containerName, nativeBindings[0].HostPort)
	}

	httpMapped, err := strconv.Atoi(httpBindings[0].HostPort)
	if err != nil {
		return nil, errors.Wrapf(err, "container %q: invalid HTTP host port %q", containerName, httpBindings[0].HostPort)
	}

	return &ContainerAddress{
		Host:       "localhost",
		NativePort: nativeMapped,
		HTTPPort:   httpMapped,
	}, nil
}
