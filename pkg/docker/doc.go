// Package docker provides Docker integration for running temporary ClickHouse instances
// for migration testing and schema validation workflows.
//
// The package enables developers to stand up ClickHouse containers with complete
// project configuration including cluster settings, keeper/zookeeper setup, and
// custom macros - ensuring the Docker environment matches the target production
// environment.
//
// # Key Features
//
//   - Temporary ClickHouse containers with project configuration volume mounting
//   - Support for ReplicatedMergeTree tables and distributed operations
//   - SQL execution and file processing within containers
//   - Schema dumping for validation and comparison
//   - Automatic container lifecycle management with cleanup
//   - Configurable ports, versions, and container names
//
// # Usage Example
//
//	import (
//		"context"
//		"github.com/pseudomuto/housekeeper/pkg/docker"
//		"github.com/pseudomuto/housekeeper/pkg/clickhouse"
//	)
//
//	// Create and configure ClickHouse container
//	container := docker.NewWithOptions(docker.DockerOptions{
//		Version:   "25.7",
//		ConfigDir: "/path/to/clickhouse/config.d",
//	})
//
//	ctx := context.Background()
//	defer container.Stop(ctx)
//
//	if err := container.Start(ctx); err != nil {
//		log.Fatal(err)
//	}
//
//	// Get connection details
//	dsn, _ := container.GetDSN()
//	httpDSN, _ := container.GetHTTPDSN()
//
//	// Connect using ClickHouse client
//	client, _ := clickhouse.NewClient(ctx, dsn)
//	defer client.Close()
//
//	// Execute SQL commands
//	err := client.ExecuteMigration(ctx, "SELECT version()")
//
//	// Dump complete schema
//	schema, _ := client.GetSchema(ctx)
//
// The Docker container automatically volume mounts your ClickHouse
// configuration directory, ensuring the containerized ClickHouse instance
// has access to cluster definitions, keeper settings, and other configuration
// required for advanced features like ReplicatedMergeTree tables.
package docker
