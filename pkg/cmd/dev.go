package cmd

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/api/types/container"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/urfave/cli/v3"
)

const devContainerName = "housekeeper-dev"

type devConfig struct {
	version   string
	configDir string
	cluster   string
}

// dev creates a CLI command for managing a local ClickHouse development server.
func dev(cfg *config.Config, client docker.DockerClient) *cli.Command {
	return &cli.Command{
		Name:   "dev",
		Usage:  "Manage local ClickHouse development server",
		Before: requireConfig(cfg),
		Commands: []*cli.Command{
			devUp(cfg, client),
			devDown(client),
		},
	}
}

// devUp creates a CLI command for starting a ClickHouse development server.
func devUp(cfg *config.Config, client docker.DockerClient) *cli.Command {
	return &cli.Command{
		Name:  "up",
		Usage: "Start ClickHouse development server and apply migrations",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			config := loadDevConfigFromConfig(cfg)

			// Check if container is already running
			if isDevContainerRunning(ctx, client) {
				fmt.Fprintln(cmd.Writer, "ClickHouse development server is already running")
				fmt.Fprintln(cmd.Writer, "Use 'housekeeper dev down' to stop it first")
				return nil
			}

			// Start container, run migrations, get client
			container, client, err := runContainer(ctx, cmd.Writer, docker.DockerOptions{
				Version:   config.version,
				ConfigDir: config.configDir,
				Name:      devContainerName,
			}, cfg, client)
			if err != nil {
				return err
			}
			// Don't defer container.Stop() - we want it to keep running
			defer client.Close()

			// Get DSN for display
			dsn, err := container.GetDSN(ctx)
			if err != nil {
				return errors.Wrap(err, "failed to get container DSN")
			}

			// Print connection details and exit
			printConnectionDetails(ctx, cmd.Writer, container, dsn)

			return nil
		},
	}
}

// devDown creates a CLI command for stopping the ClickHouse development server.
func devDown(dockerClient docker.DockerClient) *cli.Command {
	return &cli.Command{
		Name:  "down",
		Usage: "Stop and remove ClickHouse development server",
		Action: func(ctx context.Context, cmd *cli.Command) error {
			if !isDevContainerRunning(ctx, dockerClient) {
				fmt.Fprintln(cmd.Writer, "No ClickHouse development server is currently running")
				return nil
			}

			// Stop the actual Docker container using docker commands
			if err := stopHousekeeperDevContainer(ctx, dockerClient); err != nil {
				fmt.Fprintf(cmd.ErrWriter, "Warning: failed to stop container: %v\n", err)
				fmt.Fprintln(cmd.ErrWriter, "You may need to manually stop the container with: docker stop $(docker ps -q --filter label=housekeeper.dev=true)")
			} else {
				fmt.Fprintln(cmd.Writer, "ClickHouse development server stopped")
			}

			return nil
		},
	}
}

// loadDevConfigFromConfig creates a devConfig from the project configuration,
// applying defaults for missing values.
func loadDevConfigFromConfig(cfg *config.Config) *devConfig {
	config := &devConfig{
		version: cfg.ClickHouse.Version,
		cluster: cfg.ClickHouse.Cluster,
	}

	if config.version == "" {
		config.version = "25.7"
	}

	if cfg.ClickHouse.ConfigDir != "" {
		// Use absolute path since we need the full path for docker mounting
		pwd, _ := os.Getwd()
		config.configDir = filepath.Join(pwd, cfg.ClickHouse.ConfigDir)
	}

	return config
}

// printConnectionDetails displays formatted connection information for the
// development ClickHouse server.
func printConnectionDetails(ctx context.Context, w io.Writer, container *docker.ClickHouseContainer, dsn string) {
	httpDSN, err := container.GetHTTPDSN(ctx)
	if err != nil {
		fmt.Fprintf(w, "Warning: could not get HTTP DSN: %v\n", err)
		httpDSN = "unavailable"
	}

	fmt.Fprintln(w, "\n"+strings.Repeat("=", 60))
	fmt.Fprintln(w, "ClickHouse Development Server Started")
	fmt.Fprintln(w, strings.Repeat("=", 60))
	fmt.Fprintf(w, "Native DSN:  %s\n", dsn)
	fmt.Fprintf(w, "HTTP DSN:    %s\n", httpDSN)
	fmt.Fprintln(w, strings.Repeat("=", 60))
	fmt.Fprintln(w, "\nUse 'housekeeper dev down' to stop the server")
	fmt.Fprintln(w, strings.Repeat("=", 60))
}

// isDevContainerRunning checks if a housekeeper-dev container is currently running.
func isDevContainerRunning(ctx context.Context, dockerClient docker.DockerClient) bool {
	// Try to inspect the housekeeper-dev container
	_, err := dockerClient.ContainerInspect(ctx, "housekeeper-dev")
	return err == nil
}

// stopHousekeeperDevContainer stops and removes the housekeeper-dev container
// with a 30-second timeout.
func stopHousekeeperDevContainer(ctx context.Context, dockerClient docker.DockerClient) error {
	// Stop the housekeeper-dev container
	timeout := 30
	if err := dockerClient.ContainerStop(ctx, "housekeeper-dev", container.StopOptions{
		Timeout: &timeout,
	}); err != nil {
		return errors.Wrap(err, "failed to stop housekeeper-dev container")
	}

	// Remove the container
	if err := dockerClient.ContainerRemove(ctx, "housekeeper-dev", container.RemoveOptions{
		Force: true,
	}); err != nil {
		return errors.Wrap(err, "failed to remove housekeeper-dev container")
	}

	return nil
}
