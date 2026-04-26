package podman_test

import (
	"context"
	"io"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/api/types/network"
	v1 "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/podman"
	"github.com/stretchr/testify/require"
)

// mockDockerClient is a minimal stub that satisfies docker.DockerClient.
// Only ContainerInspect is exercised in resolver tests; all other methods panic.
type mockDockerClient struct {
	inspectFn func(ctx context.Context, name string) (container.InspectResponse, error)
}

func (m *mockDockerClient) ContainerInspect(ctx context.Context, containerID string) (container.InspectResponse, error) {
	return m.inspectFn(ctx, containerID)
}

func (m *mockDockerClient) ImagePull(_ context.Context, _ string, _ image.PullOptions) (io.ReadCloser, error) {
	panic("not implemented")
}
func (m *mockDockerClient) ContainerCreate(_ context.Context, _ *container.Config, _ *container.HostConfig, _ *network.NetworkingConfig, _ *v1.Platform, _ string) (container.CreateResponse, error) {
	panic("not implemented")
}
func (m *mockDockerClient) ContainerStart(_ context.Context, _ string, _ container.StartOptions) error {
	panic("not implemented")
}
func (m *mockDockerClient) ContainerList(_ context.Context, _ container.ListOptions) ([]container.Summary, error) {
	panic("not implemented")
}
func (m *mockDockerClient) ContainerStop(_ context.Context, _ string, _ container.StopOptions) error {
	panic("not implemented")
}
func (m *mockDockerClient) ContainerRemove(_ context.Context, _ string, _ container.RemoveOptions) error {
	panic("not implemented")
}

func TestPodmanAddressResolver_NetworkSettingsIP(t *testing.T) {
	// Simulate a container with a bridge IP in NetworkSettings.IPAddress (most common case).
	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return container.InspectResponse{
				NetworkSettings: &container.NetworkSettings{
					DefaultNetworkSettings: container.DefaultNetworkSettings{
						IPAddress: "10.88.0.5",
					},
				},
			}, nil
		},
	}

	resolver := podman.NewPodmanAddressResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "test-container")

	require.NoError(t, err)
	require.Equal(t, "10.88.0.5", addr.Host)
	require.Equal(t, docker.DefaultClickHousePort, addr.NativePort)
	require.Equal(t, docker.DefaultClickHouseHTTPPort, addr.HTTPPort)
}

func TestPodmanAddressResolver_FallbackToNetworkIP(t *testing.T) {
	// Simulate a container where NetworkSettings.IPAddress is empty (Podman CNI/netavark)
	// but the per-network entry has an IP.
	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return container.InspectResponse{
				NetworkSettings: &container.NetworkSettings{
					DefaultNetworkSettings: container.DefaultNetworkSettings{
						IPAddress: "", // empty — not on default bridge
					},
					Networks: map[string]*network.EndpointSettings{
						"podman": {
							IPAddress: "10.89.0.3",
						},
					},
				},
			}, nil
		},
	}

	resolver := podman.NewPodmanAddressResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "test-container")

	require.NoError(t, err)
	require.Equal(t, "10.89.0.3", addr.Host)
}

func TestPodmanAddressResolver_NoIP_ReturnsError(t *testing.T) {
	// Simulate a container with no IP at all — resolver must return a descriptive error.
	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return container.InspectResponse{
				NetworkSettings: &container.NetworkSettings{
					DefaultNetworkSettings: container.DefaultNetworkSettings{
						IPAddress: "",
					},
				},
			}, nil
		},
	}

	resolver := podman.NewPodmanAddressResolver()
	_, err := resolver.Resolve(context.Background(), cli, "no-ip-container")

	require.Error(t, err)
	require.Contains(t, err.Error(), "no-ip-container",
		"error should identify the container by name")
}
