package podman

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"

	F "github.com/IBM/fp-go/v2/function"
	O "github.com/IBM/fp-go/v2/option"
	"github.com/pseudomuto/housekeeper/pkg/docker"
)

const housekeeperRuntimeEnv = "HOUSEKEEPER_RUNTIME"

// AutoDetectResolver picks the right AddressResolver on first use and caches it.
//
// Detection order (highest to lowest priority):
//  1. HOUSEKEEPER_RUNTIME=podman|docker — explicit override, always wins.
//  2. CONTAINER_HOST env var — Podman sets this to its socket path.
//  3. DOCKER_HOST env var — if it contains "podman", use Podman resolver.
//  4. Well-known Podman socket paths on the filesystem.
//  5. Default: Docker resolver.
type AutoDetectResolver struct {
	once     sync.Once
	resolved docker.AddressResolver
}

// NewAutoDetectResolver creates an AddressResolver that detects the container runtime
// automatically on first use.
func NewAutoDetectResolver() docker.AddressResolver {
	return &AutoDetectResolver{}
}

func (r *AutoDetectResolver) Resolve(ctx context.Context, client docker.DockerClient, containerName string) (*docker.ContainerAddress, error) {
	r.once.Do(func() { r.resolved = detectResolver() })
	return r.resolved.Resolve(ctx, client, containerName)
}

func detectResolver() docker.AddressResolver {
	return F.Pipe4(
		resolverFromRuntime(),
		O.Alt(envToPodmanResolver("CONTAINER_HOST")),
		O.Alt(envToPodmanResolver("DOCKER_HOST")),
		O.Alt(func() O.Option[docker.AddressResolver] {
			return O.MonadMap(
				O.FromNonZero[bool]()(podmanSocketExists()),
				F.Constant1[bool, docker.AddressResolver](&PodmanAddressResolver{}),
			)
		}),
		O.GetOrElse(func() docker.AddressResolver { return &docker.DockerAddressResolver{} }),
	)
}

func resolverFromRuntime() O.Option[docker.AddressResolver] {
	switch strings.ToLower(os.Getenv(housekeeperRuntimeEnv)) {
	case "podman":
		return O.Some[docker.AddressResolver](&PodmanAddressResolver{})
	case "docker":
		return O.Some[docker.AddressResolver](&docker.DockerAddressResolver{})
	default:
		return O.None[docker.AddressResolver]()
	}
}

// envToPodmanResolver returns a lazy probe for envVar: Some(PodmanAddressResolver) when
// the value is a non-empty URI containing "podman", None otherwise.
func envToPodmanResolver(envVar string) func() O.Option[docker.AddressResolver] {
	return func() O.Option[docker.AddressResolver] {
		return O.MonadMap(
			O.FromPredicate(func(s string) bool {
				return s != "" && strings.Contains(strings.ToLower(s), "podman")
			})(os.Getenv(envVar)),
			func(_ string) docker.AddressResolver { return &PodmanAddressResolver{} },
		)
	}
}

// podmanSocketExists probes the well-known rootful, rootless, and XDG_RUNTIME_DIR
// socket paths for a live Podman socket.
func podmanSocketExists() bool {
	candidates := []string{
		fmt.Sprintf("/run/user/%d/podman/podman.sock", os.Getuid()),
		"/run/podman/podman.sock",
	}
	if xdg := os.Getenv("XDG_RUNTIME_DIR"); xdg != "" {
		candidates = append(candidates, xdg+"/podman/podman.sock")
	}
	for _, path := range candidates {
		if _, err := os.Stat(path); err == nil {
			return true
		}
	}
	return false
}
