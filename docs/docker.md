# Docker Integration

Housekeeper provides comprehensive Docker integration for running temporary ClickHouse instances, primarily for migration testing and schema validation workflows. The Docker containers use your project's complete ClickHouse configuration, including keeper/zookeeper setup, enabling full support for ReplicatedMergeTree tables and distributed operations.

## Overview

The Docker integration allows you to:
- Stand up temporary ClickHouse servers with specific versions
- Volume mount project configuration to ensure proper ClickHouse setup
- Apply migrations to test their validity  
- Dump schemas for comparison and validation
- Execute SQL commands and files against the container
- Clean up containers automatically
- Support ReplicatedMergeTree and other advanced ClickHouse features

## Basic Usage

### Starting a ClickHouse Container

```go
import (
    "context"
    "github.com/pseudomuto/housekeeper/pkg/docker"
)

// Create Docker container with custom configuration
container := docker.NewWithOptions(docker.DockerOptions{
    Version:   "25.7",
    ConfigDir: "/path/to/clickhouse/config.d",
})

ctx := context.Background()

// Start ClickHouse (uses version from housekeeper.yaml)
if err := container.Start(ctx); err != nil {
    log.Fatal(err)
}
defer container.Stop(ctx)

// Get connection details
dsn, _ := container.GetDSN()        // TCP connection string
httpDSN, _ := container.GetHTTPDSN() // HTTP connection string
```

### Custom Configuration

```go
// Custom Docker options
opts := docker.DockerOptions{
    Version:   "24.8", // Override project version
    ConfigDir: "/path/to/config.d", // Custom config directory
}

container := docker.NewWithOptions(opts)
```

## Migration Testing Workflow

### Complete Migration Test

```go
// Start ClickHouse
if err := dm.Start(ctx); err != nil {
    log.Fatal(err)
}
defer dm.Stop(ctx)

// Apply all migrations from project  
if err := proj.ApplyMigrations(ctx, dm); err != nil {
    log.Fatal("Migration failed:", err)
}

// Dump schema using ClickHouse client
client, _ := clickhouse.NewClient(ctx, dm.GetDSN())
defer client.Close()

schema, _ := clickhouse.DumpSchema(ctx, client)
parsedSchema := format.FormatSQL(schema) // Format for comparison

// Save or compare schema
os.WriteFile("current_schema.sql", []byte(parsedSchema), consts.ModeFile)
```

### Execute Custom SQL

```go
// Execute single query
output, err := dm.Exec(ctx, "SELECT version()")
if err != nil {
    log.Fatal(err)
}

// Execute SQL file
if err := dm.ExecFile(ctx, "setup.sql"); err != nil {
    log.Fatal(err)
}
```

## Configuration

### Version Selection

The Docker manager uses ClickHouse versions in this order:
1. Explicitly set `DockerOptions.Version`
2. Version from `housekeeper.yaml` config
3. Default version from project constants

### Configuration Directory Mounting

The Docker integration automatically volume mounts your project's ClickHouse configuration:

- Mounts `db/config.d` from your project to `/etc/clickhouse-server/config.d` in the container
- Uses your project's complete ClickHouse configuration including:
  - Cluster definitions for distributed operations
  - Keeper/Zookeeper setup for ReplicatedMergeTree tables
  - Custom macros and settings
  - Network and logging configuration
- Requires project to be initialized with `Initialize()` to create the config directory
- Ensures Docker ClickHouse behaves identically to your target environment

### Container Lifecycle

Containers are automatically:
- Named with configurable names (default: `housekeeper-clickhouse`)
- Exposed on configurable ports (default: 9000 TCP, 8123 HTTP)
- Started in detached mode with project config volume mounted
- Cleaned up on `Stop()` with container removal

### Resource Management

The Docker manager handles:
- Container naming conflicts (returns error if already running)
- Automatic container removal on stop
- Port binding management
- Wait-for-ready logic with timeout
- Configuration directory validation
- Volume mount setup and verification

## Advanced Features

### Schema Operations

Use the existing ClickHouse client with the Docker manager's DSN:

```go
// Connect to Docker ClickHouse instance
client, err := clickhouse.NewClient(ctx, dm.GetDSN())
if err != nil {
    log.Fatal(err)
}
defer client.Close()

// Dump complete schema
schema, err := clickhouse.DumpSchema(ctx, client)
// Returns parsed SQL with all non-system objects

// Get specific object types
databases, _ := client.GetDatabases(ctx)
tables, _ := client.GetTables(ctx)
views, _ := client.GetViews(ctx)
dictionaries, _ := client.GetDictionaries(ctx)
```

### Integration with Migration System

```go
// Apply migrations using project integration
if err := proj.ApplyMigrations(ctx, dm); err != nil {
    log.Fatal("Failed to apply migrations:", err)
}

// Or apply specific migration files
migrationFiles := []string{
    "/path/to/001_init.sql",
    "/path/to/002_tables.sql",
}
if err := dm.ApplyMigrationFiles(ctx, migrationFiles); err != nil {
    log.Fatal("Failed to apply migration files:", err)
}

// Validate migration set integrity
migrationDir, _ := migrator.LoadMigrationDir(os.DirFS("db/migrations"))
isValid, _ := migrationDir.IsValid()
if !isValid {
    log.Println("Warning: Migration files modified")
}
```

## Error Handling

The Docker integration provides detailed error messages for:
- Docker daemon connectivity issues
- Container startup failures
- ClickHouse readiness timeouts
- SQL execution errors
- File copying failures

```go
if err := dm.Start(ctx); err != nil {
    // Handle specific error types
    if strings.Contains(err.Error(), "already running") {
        log.Println("Container already started")
    } else if strings.Contains(err.Error(), "timeout") {
        log.Println("ClickHouse failed to start in time")
    } else {
        log.Fatal("Unexpected error:", err)
    }
}
```

## Best Practices

### Testing Environment

```go
func TestMigrations(t *testing.T) {
    // Skip if Docker not available
    if _, err := exec.LookPath("docker"); err != nil {
        t.Skip("Docker not available")
    }
    
    proj := project.New(project.ProjectParams{
        Dir:       t.TempDir(),
        Formatter: format.New(format.Defaults),
    })
    
    // Initialize project to create config directory
    if err := proj.Initialize(project.InitOptions{}); err != nil {
        t.Fatal(err)
    }
    
    dm := proj.NewDockerManager()
    
    ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
    defer cancel()
    
    // Always clean up
    defer dm.Stop(ctx)
    
    if err := dm.Start(ctx); err != nil {
        t.Fatal(err)
    }
    
    // Test migrations with full ClickHouse features...
}
```

### Production Workflows

```go
// Initialize project with full configuration
proj := project.New(project.ProjectParams{
    Dir:       "/path/to/project",
    Formatter: format.New(format.Defaults),
})
if err := proj.Initialize(project.InitOptions{}); err != nil {
    return fmt.Errorf("project initialization failed: %w", err)
}

// Use specific version for consistency
opts := project.DockerOptions{
    Version: "25.7", // Pin to specific version
    ContainerName: fmt.Sprintf("migration-test-%d", time.Now().Unix()),
}

dm := proj.NewDockerManagerWithOptions(opts)

// Apply migrations with full ClickHouse configuration
if err := dm.ApplyMigrations(ctx); err != nil {
    return fmt.Errorf("migration validation failed: %w", err)
}

// Test ReplicatedMergeTree functionality
_, err := dm.Exec(ctx, `
    CREATE TABLE test_replicated (
        id UInt64,
        data String
    ) ENGINE = ReplicatedMergeTree('/clickhouse/tables/cluster/test', 'replica_1')
    ORDER BY id
`)
if err != nil {
    return fmt.Errorf("ReplicatedMergeTree test failed: %w", err)
}

// Compare with target schema
targetSchema, _ := os.ReadFile("target_schema.sql")
currentSchema, _ := dm.DumpSchema(ctx)

if string(targetSchema) != currentSchema {
    return errors.New("schema mismatch detected")
}
```

## Limitations

- Requires Docker daemon running
- Network connectivity for image pulls
- Container port conflicts if multiple instances
- Limited to Linux containers (ClickHouse official images)
- No persistent storage (containers are ephemeral)

## Troubleshooting

### Common Issues

**Container won't start:**
- Check Docker daemon is running: `docker ps`
- Verify port availability: `netstat -an | grep :9000`
- Check Docker image exists: `docker images clickhouse/clickhouse-server`

**ClickHouse not ready:**
- Increase timeout in context
- Check container logs: `docker logs housekeeper-clickhouse`
- Verify memory/resource limits

**Migration failures:**
- Check SQL syntax in migration files
- Verify file permissions for copying to container
- Review ClickHouse error logs for detailed messages