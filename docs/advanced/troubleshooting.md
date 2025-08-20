# Troubleshooting

This guide helps you diagnose and resolve common issues when using Housekeeper with ClickHouse.

## Installation Issues

### Binary Download Problems

**Issue**: Cannot download or execute Housekeeper binary

**Solutions**:

```bash
# Check if binary is executable
chmod +x housekeeper

# Verify binary integrity
housekeeper --version

# Download specific version
curl -L https://github.com/pseudomuto/housekeeper/releases/download/v1.0.0/housekeeper_linux_amd64 -o housekeeper
```

### Go Installation Issues

**Issue**: Build from source fails

**Solutions**:

```bash
# Verify Go version (requires 1.21+)
go version

# Clean module cache
go clean -modcache

# Rebuild with verbose output
go build -v ./cmd/housekeeper
```

## Connection Issues

### DSN Format Problems

**Issue**: Cannot connect to ClickHouse

**Common Formats**:

```bash
# Start development server and generate diff
housekeeper dev up
housekeeper diff

# For schema extraction, use schema dump with different connection strings:
# Basic host:port
housekeeper schema dump --url "localhost:9000"

# With authentication
housekeeper schema dump --url "clickhouse://user:password@host:9000/database"

# TCP protocol
housekeeper schema dump --url "tcp://host:9000?username=user&password=pass"

# Secure connection
housekeeper schema dump --url "clickhouse://user:pass@host:9440/db?secure=true"
```

### Network Connectivity

**Issue**: Connection timeouts or refused connections

**Diagnosis**:

```bash
# Test basic connectivity
telnet clickhouse-host 9000

# Check ClickHouse status
curl http://clickhouse-host:8123/ping

# Test authentication
curl -u user:password http://clickhouse-host:8123/ping
```

### SSL/TLS Issues

**Issue**: SSL connection failures

**Solutions**:

```bash
# Skip SSL verification (not recommended for production)
housekeeper schema dump --url "clickhouse://user:pass@host:9440/db?secure=true&skip_verify=true"

# Use proper certificates
housekeeper schema dump --url "clickhouse://user:pass@host:9440/db?secure=true&ca_cert=/path/to/ca.pem"
```

## Parsing Errors

### Syntax Errors

**Issue**: SQL parsing fails

**Example Error**:
```
Error: failed to parse SQL: unexpected token "TEMPORARY" at line 15, column 8
```

**Solutions**:

1. **Check ClickHouse version compatibility**:
   ```bash
   # Some features require specific ClickHouse versions
   SELECT version() FROM system.one;
   ```

2. **Validate SQL syntax**:
   ```bash
   # Test SQL directly in ClickHouse
   clickhouse-client --query "CREATE TABLE test (...)"
   ```

3. **Common syntax issues**:
   ```sql
   -- ❌ Incorrect - missing backticks for reserved words
   CREATE TABLE user (id UInt64, name String);
   
   -- ✅ Correct - use backticks
   CREATE TABLE `user` (id UInt64, name String);
   ```

### Unsupported Features

**Issue**: Parser doesn't recognize specific ClickHouse syntax

**Workarounds**:

1. **Check supported features** in the project documentation
2. **File an issue** on GitHub for missing features
3. **Use alternative syntax** if available

### Import Resolution

**Issue**: Schema imports not found

**Example Error**:
```
Error: failed to resolve import: schemas/tables/users.sql not found
```

**Solutions**:

```sql
-- ❌ Incorrect - wrong path
-- housekeeper:import ../missing/file.sql

-- ✅ Correct - relative to current file
-- housekeeper:import ./tables/users.sql

-- ✅ Correct - absolute path from project root
-- housekeeper:import schemas/tables/users.sql
```

## Migration Issues

### No Differences Detected

**Issue**: Expected changes not generating migrations

**Diagnosis**:

```bash
# Debug schema parsing
housekeeper schema --input ./db/main.sql --debug

# Compare parsed schemas
housekeeper fmt --input ./current.sql > current_formatted.sql
housekeeper fmt --input ./target.sql > target_formatted.sql
diff current_formatted.sql target_formatted.sql
```

**Common Causes**:

1. **Whitespace differences**: Use `fmt` command to normalize
2. **Comment differences**: Comments don't affect migrations
3. **Order differences**: Statement order matters for comparison

### Validation Errors

**Issue**: Migration validation fails

**Example Errors**:

```
Error: unsupported operation: dictionary ALTER operations not supported
Error: cluster configuration changes not supported
Error: engine type changes not supported
```

**Solutions**:

1. **Dictionary changes**: Use CREATE OR REPLACE instead of ALTER
   ```sql
   -- ❌ Not supported
   ALTER DICTIONARY users_dict MODIFY SOURCE(...);
   
   -- ✅ Supported
   CREATE OR REPLACE DICTIONARY users_dict (...);
   ```

2. **Cluster changes**: Keep cluster configuration consistent
   ```sql
   -- ❌ Cannot change cluster
   -- Current: CREATE TABLE users (...) ENGINE = MergeTree();
   -- Target:  CREATE TABLE users (...) ON CLUSTER prod ENGINE = MergeTree();
   
   -- ✅ Keep cluster consistent
   -- Both: CREATE TABLE users (...) ON CLUSTER prod ENGINE = MergeTree();
   ```

3. **Engine changes**: Use DROP+CREATE manually
   ```sql
   -- Manual migration for engine changes
   DROP TABLE old_table;
   CREATE TABLE old_table (...) ENGINE = NewEngine();
   ```

### Migration File Issues

**Issue**: Generated migration files have problems

**Solutions**:

```bash
# Validate migration syntax
housekeeper fmt --input ./db/migrations/20240806143022.sql

# Test migration on staging
clickhouse-client --queries-file ./db/migrations/20240806143022.sql

# Check migration integrity
housekeeper status --migrations ./db/migrations/
```

### Partial Migration Failures & Recovery

Housekeeper automatically handles partial migration failures, but you may need to troubleshoot issues with the recovery process.

#### Understanding Partial Failures

**What happens when a migration fails partway through?**

```sql
-- Migration: 20240101120000_setup_analytics.sql
CREATE DATABASE analytics;              -- ✅ Statement 1: Success
CREATE TABLE analytics.events (...);    -- ✅ Statement 2: Success  
CREATE TABLE analytics.users (...);     -- ❌ Statement 3: Failed
CREATE VIEW analytics.summary (...);    -- ⏸  Statement 4: Not executed
```

Housekeeper automatically:
1. Records successful statements (1-2) in revision table
2. Records the error at statement 3
3. Stores statement hashes for integrity validation
4. Sets up automatic resume for next migration run

#### Automatic Recovery Process

**Issue**: You want to understand what will be resumed

**Diagnosis**:

```bash
# Check status to see partial migrations
housekeeper status --url localhost:9000 --verbose

# See what would be resumed with dry-run
housekeeper migrate --url localhost:9000 --dry-run

# Examine revision table directly
echo "SELECT version, applied, total, error, executed_at FROM housekeeper.revisions WHERE error IS NOT NULL ORDER BY executed_at DESC;" | clickhouse-client
```

**Example Output**:
```bash
Found 1 partially applied migration(s) that will be resumed:

  ⚠️  20240101120000_setup_analytics: 2/4 statements applied
     Last error: Table 'users' already exists
     Will resume with 2 remaining statement(s)
```

#### Troubleshooting Resume Issues

**Issue**: Automatic resume fails with validation errors

**Common Errors**:

1. **Hash Mismatch (Migration File Modified)**:
   ```
   Error: statement 2 hash mismatch: migration file may have been modified since partial execution
   Expected: h1:abc123=, Got: h1:def456=
   ```
   
   **Solutions**:
   ```bash
   # ❌ Don't modify migration files after partial execution
   # ✅ Either restore original file or create new migration
   
   # Option 1: Restore original migration file
   git checkout db/migrations/20240101120000_setup_analytics.sql
   
   # Option 2: Create new migration with remaining changes
   housekeeper diff  # Generates new migration with remaining changes
   ```

2. **Statement Count Mismatch**:
   ```
   Error: migration statement count changed: expected 4 statements, found 6 in migration file
   ```
   
   **Solutions**:
   ```bash
   # Migration file had statements added/removed after partial execution
   # Restore original file or clean up partial state
   
   # Option 1: Restore original file
   git checkout db/migrations/20240101120000_setup_analytics.sql
   
   # Option 2: Manually clean up partial state (advanced)
   DELETE FROM housekeeper.revisions WHERE version = '20240101120000_setup_analytics';
   ```

3. **Dependency Issues During Resume**:
   ```
   Error: Table 'events' doesn't exist (needed for view creation)
   ```
   
   **Solution**: Check if dependent objects were created successfully
   ```sql
   -- Check which objects exist
   SELECT name FROM system.tables WHERE database = 'analytics';
   
   -- Verify expected objects are present before resuming
   ```

#### Manual Recovery (Advanced)

**Issue**: Automatic recovery is not working and you need manual intervention

**⚠️ Warning**: Only use manual recovery if automatic resume fails

```bash
# 1. Examine current state
housekeeper status --url localhost:9000 --verbose
housekeeper schema dump --url localhost:9000

# 2. Check revision table
echo "SELECT * FROM housekeeper.revisions WHERE version = '20240101120000_setup_analytics';" | clickhouse-client --vertical

# 3. Options for manual recovery:

# Option A: Delete partial revision and restart migration
echo "DELETE FROM housekeeper.revisions WHERE version = '20240101120000_setup_analytics';" | clickhouse-client
housekeeper migrate --url localhost:9000  # Restarts from beginning

# Option B: Fix the underlying issue and resume automatically
# (Fix whatever caused the original failure, then run)
housekeeper migrate --url localhost:9000  # Will resume automatically

# Option C: Mark migration as completed manually (if statements were applied outside Housekeeper)
echo "UPDATE housekeeper.revisions SET applied = total, error = NULL WHERE version = '20240101120000_setup_analytics';" | clickhouse-client
```

#### Prevention Best Practices

1. **Test migrations in development first**
2. **Don't modify migration files after they've been partially executed**
3. **Monitor ClickHouse logs during migration execution**
4. **Ensure sufficient resources (disk space, memory) before running large migrations**
5. **Use `--dry-run` to preview what will be executed**

#### Recovery Verification

After successful recovery:

```bash
# Verify all migrations are completed
housekeeper status --url localhost:9000

# Check that schema matches expectations
housekeeper diff  # Should show "No differences found"

# Verify specific objects exist
housekeeper schema dump --url localhost:9000 | grep "CREATE TABLE"
```

## Docker Integration Issues

### Container Startup Problems

**Issue**: ClickHouse container fails to start

**Solutions**:

```go
// Increase startup timeout
container := docker.NewWithOptions(docker.DockerOptions{
    Version: "25.7",
    StartupTimeout: 60 * time.Second,
})

// Check container logs
logs, err := container.GetLogs()
fmt.Println(logs)
```

### Volume Mount Issues

**Issue**: Configuration not loaded in container

**Solutions**:

```go
// Ensure config directory exists
if err := os.MkdirAll("./config.d", 0755); err != nil {
    log.Fatal(err)
}

// Mount with proper permissions
container := docker.NewWithOptions(docker.DockerOptions{
    ConfigDir: "./config.d", // Must contain _clickhouse.xml
})
```

### Port Conflicts

**Issue**: Port already in use errors

**Solutions**:

```bash
# Find processes using ClickHouse ports
lsof -i :9000
lsof -i :8123

# Kill existing ClickHouse processes
pkill clickhouse-server

# Use Docker port mapping
docker run -p 9001:9000 -p 8124:8123 clickhouse/clickhouse-server
```

## Performance Issues

### Slow Parsing

**Issue**: Large schema files take too long to parse

**Solutions**:

1. **Split large files**:
   ```sql
   -- main.sql
   -- housekeeper:import ./databases/analytics.sql
   -- housekeeper:import ./tables/events.sql
   -- housekeeper:import ./views/aggregations.sql
   ```

2. **Remove unnecessary comments and whitespace**:
   ```bash
   # Format and clean schema
   housekeeper fmt --input messy.sql --output clean.sql
   ```

3. **Use streaming for very large files**:
   ```go
   // Parse from file stream
   file, err := os.Open("large_schema.sql")
   defer file.Close()
   
   sql, err := parser.Parse(file)
   ```

### Memory Usage

**Issue**: High memory consumption during parsing

**Solutions**:

1. **Process files individually**
2. **Increase system memory**
3. **Use pagination for very large schemas**

## ClickHouse-Specific Issues

### Permission Errors

**Issue**: Insufficient privileges for DDL operations

**Solutions**:

```sql
-- Grant necessary permissions
GRANT CREATE ON *.* TO user;
GRANT DROP ON *.* TO user;
GRANT ALTER ON *.* TO user;

-- For cluster operations
GRANT CLUSTER ON *.* TO user;
```

### Cluster Issues

**Issue**: Distributed DDL operations fail

**Diagnosis**:

```sql
-- Check cluster configuration
SELECT * FROM system.clusters WHERE cluster = 'production';

-- Monitor distributed DDL queue
SELECT * FROM system.distributed_ddl_queue;

-- Check replication status
SELECT * FROM system.replicas;
```

**Solutions**:

1. **Increase timeouts**:
   ```xml
   <distributed_ddl_task_timeout>300</distributed_ddl_task_timeout>
   ```

2. **Check ZooKeeper connectivity**:
   ```bash
   echo "ruok" | nc zookeeper-host 2181
   ```

3. **Verify network connectivity between nodes**

### Disk Space Issues

**Issue**: Operations fail due to insufficient disk space

**Solutions**:

```sql
-- Check disk usage
SELECT * FROM system.disks;

-- Clean up old data
OPTIMIZE TABLE table_name FINAL;

-- Drop unused tables/databases
DROP TABLE IF EXISTS unused_table;
```

## Debug Commands

### Schema Analysis

```bash
# Parse and format schema
housekeeper fmt schema.sql

# Compile and show schema structure
housekeeper schema compile

# Generate migration (runs against development server)
housekeeper diff
```

### Connection Testing

```bash
# Test basic connection
housekeeper schema dump --url localhost:9000

# Test cluster connection with cluster injection
housekeeper schema dump --url localhost:9000 --cluster test_cluster
```

### Migration Debugging

```bash
# Validate migration files
housekeeper status --migrations ./db/migrations/

# Rehash migration files
housekeeper rehash --migrations ./db/migrations/

# Test migration syntax
clickhouse-client --dry-run --queries-file migration.sql
```

## Getting Help

### Log Collection

When reporting issues, include:

1. **Housekeeper version**: `housekeeper --version`
2. **ClickHouse version**: `SELECT version()`
3. **Full command and error output**
4. **Relevant schema files** (sanitized)
5. **System information**: OS, architecture

### Common Solutions

1. **Update to latest version**
2. **Check GitHub issues** for similar problems
3. **Verify ClickHouse compatibility**
4. **Test with minimal reproduction case**

### Filing Issues

When filing GitHub issues:

1. **Use issue templates**
2. **Provide minimal reproduction**
3. **Include version information**
4. **Describe expected vs actual behavior**
5. **Share relevant logs and configurations**

### Community Resources

- **GitHub Discussions**: General questions and usage help
- **GitHub Issues**: Bug reports and feature requests
- **Documentation**: Complete API and usage guides
- **Examples**: Reference implementations and patterns