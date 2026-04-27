package podman

import (
	"context"

	O "github.com/IBM/fp-go/v2/option"
	F "github.com/IBM/fp-go/v2/function"
	R "github.com/IBM/fp-go/v2/result"
	dockercontainer "github.com/docker/docker/api/types/container"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/docker"
)

// PodmanAddressResolver implements docker.AddressResolver for Podman environments.
//
// Under Podman rootless (slirp4netns / pasta), port bindings in the compat API are
// not reliably reachable on localhost from the host network namespace. The container
// is reachable via its direct bridge IP, which Podman populates even for rootless
// containers.
type PodmanAddressResolver struct{}

// NewPodmanAddressResolver creates an AddressResolver for Podman environments.
func NewPodmanAddressResolver() docker.AddressResolver {
	return &PodmanAddressResolver{}
}

func (r *PodmanAddressResolver) Resolve(ctx context.Context, client docker.DockerClient, containerName string) (*docker.ContainerAddress, error) {
	inspect, err := client.ContainerInspect(ctx, containerName)
	return R.UnwrapError(F.Pipe2(
		R.TryCatchError(inspect, errors.Wrapf(err, "failed to inspect container %q", containerName)),
		R.Chain(func(inspect dockercontainer.InspectResponse) R.Result[string] {
			return R.FromOption[string](func() error {
				return errors.Errorf(
					"container %q has no assigned IP address; "+
						"ensure the container is running and attached to a bridge network",
					containerName,
				)
			})(resolveContainerIP(inspect))
		}),
		R.Map(func(ip string) *docker.ContainerAddress {
			return &docker.ContainerAddress{
				Host:       ip,
				NativePort: docker.DefaultClickHousePort,
				HTTPPort:   docker.DefaultClickHouseHTTPPort,
			}
		}),
	))
}

// resolveContainerIP tries NetworkSettings.IPAddress first, then falls back to the
// first non-empty per-network entry (Podman CNI / netavark assigns IPs per network).
// Extracted as a pure function so it can be tested without a Docker client.
func resolveContainerIP(inspect dockercontainer.InspectResponse) O.Option[string] {
	ns := inspect.NetworkSettings
	if ns == nil {
		return O.None[string]()
	}
	primary := O.FromPredicate[string](func(s string) bool { return s != "" })(ns.IPAddress)
	return O.MonadAlt(primary, func() O.Option[string] {
		for _, n := range ns.Networks {
			if n != nil && n.IPAddress != "" {
				return O.Some(n.IPAddress)
			}
		}
		return O.None[string]()
	})
}
