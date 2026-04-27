// Package podman provides AddressResolver implementations for Podman container runtimes.
//
// Podman exposes a Docker-compatible REST API (the "compat" API), but its networking
// model differs from Docker in important ways for rootless containers:
//
//   - Docker maps container ports to random host ports on the loopback interface.
//     Clients connect to localhost:<randomHostPort>.
//
//   - Podman rootless uses the slirp4netns or pasta user-space networking stack.
//     The compat API reports port bindings in the inspect response, but those bindings
//     may not be reachable on localhost from within a container or a different network
//     namespace. The container is reachable directly via its bridge IP address.
//
// This package provides two implementations of docker.AddressResolver:
//
//   - PodmanAddressResolver: always uses the container's bridge IP + container port.
//     Use this when you know you are running under Podman.
//
//   - AutoDetectResolver: inspects the environment at runtime (CONTAINER_HOST /
//     DOCKER_HOST env vars, Podman socket paths, HOUSEKEEPER_RUNTIME override) and
//     selects the appropriate resolver transparently.
//
// # Usage
//
//	import (
//		"github.com/docker/docker/client"
//		"github.com/pseudomuto/housekeeper/pkg/docker"
//		"github.com/pseudomuto/housekeeper/pkg/podman"
//	)
//
//	cli, _ := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
//
//	// Explicit Podman resolver:
//	container, _ := docker.NewWithOptions(cli, docker.DockerOptions{
//		AddressResolver: podman.NewPodmanAddressResolver(),
//	})
//
//	// Auto-detect at runtime (recommended for tools that support both):
//	container, _ := docker.NewWithOptions(cli, docker.DockerOptions{
//		AddressResolver: podman.NewAutoDetectResolver(),
//	})
package podman
