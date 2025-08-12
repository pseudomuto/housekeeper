package cmd

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/docker/docker/client"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/docker"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/urfave/cli/v3"
)

func dev() *cli.Command {
	return &cli.Command{
		Name:   "dev",
		Usage:  "Manage local ClickHouse development server",
		Before: requireProject,
		Commands: []*cli.Command{
			devUp(),
			devDown(),
		},
	}
}

const devContainerName = "housekeeper-dev"

type devConfig struct {
	version   string
	configDir string
	cluster   string
}

func devUp() *cli.Command {
	return &cli.Command{
		Name:   "up",
		Usage:  "Start ClickHouse development server and apply migrations",
		Action: runDevUpCommand,
	}
}

func devDown() *cli.Command {
	return &cli.Command{
		Name:   "down",
		Usage:  "Stop and remove ClickHouse development server",
		Action: runDevDownCommand,
	}
}

func runDevUpCommand(ctx context.Context, cmd *cli.Command) error {
	config, err := loadDevConfig()
	if err != nil {
		return err
	}

	// Check if container is already running
	if isDevContainerRunning(ctx) {
		fmt.Println("ClickHouse development server is already running")
		fmt.Println("Use 'housekeeper dev down' to stop it first")
		return nil
	}

	container, err := startClickHouseContainer(ctx, config)
	if err != nil {
		return err
	}
	// Don't defer container.Stop() - we want it to keep running

	client, dsn, err := connectToClickHouse(ctx, container)
	if err != nil {
		return err
	}
	defer client.Close()

	if err := applyAllMigrations(ctx, client); err != nil {
		return err
	}

	// Store container info for dev down command
	if err := saveDevContainerInfo(container); err != nil {
		fmt.Printf("Warning: failed to save container info: %v\n", err)
	}

	// Print connection details and exit
	printConnectionDetails(ctx, container, dsn, config.cluster)
	return nil
}

func runDevDownCommand(ctx context.Context, cmd *cli.Command) error {
	if !isDevContainerRunning(ctx) {
		fmt.Println("No ClickHouse development server is currently running")
		return nil
	}

	// Stop the actual Docker container using docker commands
	if err := stopHousekeeperDevContainer(ctx); err != nil {
		fmt.Printf("Warning: failed to stop container: %v\n", err)
		fmt.Println("You may need to manually stop the container with: docker stop $(docker ps -q --filter label=housekeeper.dev=true)")
	} else {
		fmt.Println("ClickHouse development server stopped")
	}

	// Clean up the tracking file
	if err := removeDevContainerInfo(); err != nil {
		return errors.Wrap(err, "failed to clean up container info")
	}

	return nil
}

func loadDevConfig() (*devConfig, error) {
	// Load configuration directly from file (we're already in the project directory)
	cfg, err := config.LoadConfigFile("housekeeper.yaml")
	if err != nil {
		return nil, errors.Wrap(err, "failed to load project configuration")
	}

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

	return config, nil
}

func startClickHouseContainer(ctx context.Context, config *devConfig) (*docker.ClickHouseContainer, error) {
	fmt.Printf("Starting ClickHouse %s container...\n", config.version)

	container, err := docker.NewWithOptions(docker.DockerOptions{
		Version:   config.version,
		ConfigDir: config.configDir,
	})
	if err != nil {
		return nil, errors.Wrap(err, "failed to create ClickHouse container")
	}

	if err := container.Start(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to start ClickHouse container")
	}

	return container, nil
}

func connectToClickHouse(ctx context.Context, container *docker.ClickHouseContainer) (*clickhouse.Client, string, error) {
	dsn, err := container.GetDSN(ctx)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to get container DSN")
	}

	fmt.Printf("ClickHouse server started: %s\n", dsn)

	client, err := clickhouse.NewClient(ctx, dsn)
	if err != nil {
		return nil, "", errors.Wrap(err, "failed to create clickhouse client")
	}

	fmt.Println("Connected to ClickHouse server")
	return client, dsn, nil
}

func applyAllMigrations(ctx context.Context, client *clickhouse.Client) error {
	// Load the migration set from the current project
	migrations, err := migrator.LoadMigrationDir(os.DirFS(currentProject.MigrationsDir()))
	if err != nil {
		// If the migrations directory doesn't exist, that's okay - just no migrations to apply
		if os.IsNotExist(errors.Cause(err)) {
			fmt.Println("No migrations directory found - skipping migrations")
			return nil
		}
		return errors.Wrap(err, "failed to load migration set")
	}

	// Get migration files from the set
	if len(migrations.Migrations) == 0 {
		fmt.Println("No migrations found")
		return nil
	}

	valid, err := migrations.Validate()
	if !valid || err != nil {
		fmt.Printf("Warning: could not validate migration set: %v\n", err)
	}

	fmt.Printf("Applying %d migrations...\n", len(migrations.Migrations))

	for i, migrationFile := range migrations.Migrations {
		fmt.Printf("  [%d/%d] Applying %s.sql\n", i+1, len(migrations.Migrations), migrationFile.Version)

		if err := applyMigration(ctx, client, migrationFile); err != nil {
			return errors.Wrapf(err, "failed to apply migration: %s.sql", migrationFile.Version)
		}
	}

	fmt.Println("All migrations applied successfully!")
	return nil
}

func printConnectionDetails(ctx context.Context, container *docker.ClickHouseContainer, dsn, cluster string) {
	httpDSN, err := container.GetHTTPDSN(ctx)
	if err != nil {
		fmt.Printf("Warning: could not get HTTP DSN: %v\n", err)
		httpDSN = "unavailable"
	}

	fmt.Println("\n" + strings.Repeat("=", 60))
	fmt.Println("ClickHouse Development Server Started")
	fmt.Println(strings.Repeat("=", 60))
	fmt.Printf("Native DSN:  %s\n", dsn)
	fmt.Printf("HTTP DSN:    %s\n", httpDSN)
	if cluster != "" {
		fmt.Printf("Cluster:     %s\n", cluster)
	}
	fmt.Println("\nUse 'housekeeper dev down' to stop the server")
	fmt.Println(strings.Repeat("=", 60))
}

// Container persistence functions
func getDevContainerInfoPath() string {
	return filepath.Join(os.TempDir(), "housekeeper-dev-container.json")
}

type containerInfo struct {
	ID string `json:"id"`
}

func isDevContainerRunning(ctx context.Context) bool {
	// Create a Docker engine to check if container is running
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return false
	}

	engine := docker.NewEngine(dockerClient)

	// Try to get the housekeeper-dev container
	_, err = engine.Get(ctx, "housekeeper-dev")
	return err == nil
}

func saveDevContainerInfo(container *docker.ClickHouseContainer) error {
	if !container.IsRunning() {
		return errors.New("container is not running")
	}

	// For now, we'll just create a marker file
	// In a real implementation, we might store container ID
	infoPath := getDevContainerInfoPath()
	info := containerInfo{ID: devContainerName}

	data, err := json.Marshal(info)
	if err != nil {
		return errors.Wrap(err, "failed to marshal container info")
	}

	if err := os.WriteFile(infoPath, data, consts.ModeFile); err != nil {
		return errors.Wrap(err, "failed to write container info")
	}

	return nil
}

func removeDevContainerInfo() error {
	infoPath := getDevContainerInfoPath()
	if err := os.Remove(infoPath); err != nil && !os.IsNotExist(err) {
		return errors.Wrap(err, "failed to remove container info")
	}
	return nil
}

func applyMigration(ctx context.Context, client *clickhouse.Client, migration *migrator.Migration) error {
	fmtr := format.New(format.Defaults)
	for _, stmt := range migration.Statements {
		// Execute the statement
		buf := new(bytes.Buffer)
		if err := fmtr.Format(buf, stmt); err != nil {
			return errors.Wrap(err, "failed to execute SQL statement")
		}

		if err := client.ExecuteMigration(ctx, buf.String()); err != nil {
			return errors.Wrapf(err, "failed to execute statement: %s", buf.String())
		}
	}

	return nil
}

func stopHousekeeperDevContainer(ctx context.Context) error {
	// Create a Docker engine to stop the container
	dockerClient, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		return errors.Wrap(err, "failed to create Docker client")
	}

	engine := docker.NewEngine(dockerClient)

	// Stop the housekeeper-dev container
	if err := engine.Stop(ctx, "housekeeper-dev"); err != nil {
		return errors.Wrap(err, "failed to stop housekeeper-dev container")
	}

	return nil
}
