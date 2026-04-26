package podman_test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/go-connections/nat"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/podman"
	"github.com/stretchr/testify/require"
)

// skipIfPodmanSocket skips the test when a Podman socket exists on the filesystem.
// The socket probe in detectResolver() has higher priority than the default Docker
// fallback, so tests that expect DockerAddressResolver would fail on Podman hosts.
func skipIfPodmanSocket(t *testing.T) {
	t.Helper()
	candidates := []string{
		fmt.Sprintf("/run/user/%d/podman/podman.sock", os.Getuid()),
		"/run/podman/podman.sock",
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		candidates = append(candidates, xdg+"/podman/podman.sock")
	}
	for _, p := range candidates {
		if _, err := os.Stat(p); err == nil {
			t.Skipf("Podman socket found at %s — socket probe supersedes DOCKER_HOST fallback on this host", p)
		}
	}
}

// podmanInspect returns an InspectResponse with a bridge IP, mimicking Podman rootless.
func podmanInspect(ip string) container.InspectResponse {
	return container.InspectResponse{
		NetworkSettings: &container.NetworkSettings{
			DefaultNetworkSettings: container.DefaultNetworkSettings{
				IPAddress: ip,
			},
		},
	}
}

// dockerInspect returns an InspectResponse with port bindings, mimicking Docker Engine.
func dockerInspect(nativeHost, httpHost string) container.InspectResponse {
	return container.InspectResponse{
		NetworkSettings: &container.NetworkSettings{
			NetworkSettingsBase: container.NetworkSettingsBase{
				Ports: nat.PortMap{
					nat.Port("9000/tcp"): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: nativeHost}},
					nat.Port("8123/tcp"): []nat.PortBinding{{HostIP: "0.0.0.0", HostPort: httpHost}},
				},
			},
		},
	}
}

func TestAutoDetectResolver_PodmanRuntime_UsesBridgeIP(t *testing.T) {
	// HOUSEKEEPER_RUNTIME=podman must select PodmanAddressResolver, which reads the
	// container's direct bridge IP (not the mapped host port).
	t.Setenv("HOUSEKEEPER_RUNTIME", "podman")
	t.Setenv("CONTAINER_HOST", "")
	t.Setenv("DOCKER_HOST", "")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return podmanInspect("10.88.0.5"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "ch-test")

	require.NoError(t, err)
	require.Equal(t, "10.88.0.5", addr.Host)
	require.Equal(t, docker.DefaultClickHousePort, addr.NativePort)
	require.Equal(t, docker.DefaultClickHouseHTTPPort, addr.HTTPPort)
}

func TestAutoDetectResolver_DockerRuntime_UsesPortBindings(t *testing.T) {
	// HOUSEKEEPER_RUNTIME=docker must select DockerAddressResolver, which reads
	// mapped host ports and returns localhost:<hostPort>.
	t.Setenv("HOUSEKEEPER_RUNTIME", "docker")
	t.Setenv("CONTAINER_HOST", "")
	t.Setenv("DOCKER_HOST", "")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return dockerInspect("54321", "54322"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "ch-test")

	require.NoError(t, err)
	require.Equal(t, "localhost", addr.Host)
	require.Equal(t, 54321, addr.NativePort)
	require.Equal(t, 54322, addr.HTTPPort)
}

func TestAutoDetectResolver_ContainerHostPodmanSocket_UsesBridgeIP(t *testing.T) {
	// CONTAINER_HOST pointing to a Podman socket activates the Podman resolver.
	t.Setenv("HOUSEKEEPER_RUNTIME", "")
	t.Setenv("CONTAINER_HOST", "unix:///run/user/1000/podman/podman.sock")
	t.Setenv("DOCKER_HOST", "")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return podmanInspect("10.89.0.2"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "ch-test")

	require.NoError(t, err)
	require.Equal(t, "10.89.0.2", addr.Host)
}

func TestAutoDetectResolver_DockerHostPodmanSocket_UsesBridgeIP(t *testing.T) {
	// DOCKER_HOST containing "podman" activates the Podman resolver.
	t.Setenv("HOUSEKEEPER_RUNTIME", "")
	t.Setenv("CONTAINER_HOST", "")
	t.Setenv("DOCKER_HOST", "unix:///run/user/1000/podman/podman.sock")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return podmanInspect("10.90.0.3"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "ch-test")

	require.NoError(t, err)
	require.Equal(t, "10.90.0.3", addr.Host)
}

func TestAutoDetectResolver_DockerSocket_UsesPortBindings(t *testing.T) {
	// A Docker socket URI in DOCKER_HOST does NOT activate Podman — falls back to Docker.
	// Skip on hosts where a Podman socket exists: the socket probe fires before the default
	// fallback and would select PodmanAddressResolver regardless of DOCKER_HOST.
	skipIfPodmanSocket(t)
	t.Setenv("HOUSEKEEPER_RUNTIME", "")
	t.Setenv("CONTAINER_HOST", "")
	t.Setenv("DOCKER_HOST", "unix:///var/run/docker.sock")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return dockerInspect("32768", "32769"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()
	addr, err := resolver.Resolve(context.Background(), cli, "ch-test")

	require.NoError(t, err)
	require.Equal(t, "localhost", addr.Host)
	require.Equal(t, 32768, addr.NativePort)
}

func TestAutoDetectResolver_CachesResult(t *testing.T) {
	// sync.Once guarantees detection fires exactly once; calls after the first
	// must reuse the cached resolver — verified by flipping the env var midway.
	t.Setenv("HOUSEKEEPER_RUNTIME", "podman")
	t.Setenv("CONTAINER_HOST", "")
	t.Setenv("DOCKER_HOST", "")

	cli := &mockDockerClient{
		inspectFn: func(_ context.Context, _ string) (container.InspectResponse, error) {
			return podmanInspect("10.88.0.9"), nil
		},
	}

	resolver := podman.NewAutoDetectResolver()

	// First call triggers detection (podman selected).
	addr1, err := resolver.Resolve(context.Background(), cli, "ch-test")
	require.NoError(t, err)
	require.Equal(t, "10.88.0.9", addr1.Host)

	// Switch env var — second call must still use the cached Podman resolver.
	t.Setenv("HOUSEKEEPER_RUNTIME", "docker")
	addr2, err := resolver.Resolve(context.Background(), cli, "ch-test")
	require.NoError(t, err)
	require.Equal(t, "10.88.0.9", addr2.Host, "cached resolver must ignore env change")
}
