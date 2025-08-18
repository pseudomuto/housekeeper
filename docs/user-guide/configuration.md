# Configuration

Learn how to configure Housekeeper for your environment and customize its behavior.

## Configuration File

Housekeeper uses a YAML configuration file (`housekeeper.yaml`) to define project settings:

```yaml
# Basic configuration
clickhouse:
  version: "25.7"                    # ClickHouse version for Docker
  config_dir: "db/config.d"         # ClickHouse configuration directory
  cluster: "cluster"                 # Default cluster name

entrypoint: db/main.sql              # Main schema file
dir: db/migrations                   # Migration output directory
```

## Configuration Sections

### ClickHouse Settings

Configure ClickHouse-specific options:

```yaml
clickhouse:
  # Docker container version
  version: "25.7"                    # Specific version for development containers
  
  # Configuration directory for cluster setup
  config_dir: "db/config.d"         # Relative to project root
  
  # Default cluster name for ON CLUSTER operations
  cluster: "cluster"                 # Used for distributed DDL statements
```

### Schema Configuration

Configure schema-related settings:

```yaml
# Schema entrypoint and organization
entrypoint: db/main.sql              # Main schema file with imports
dir: db/migrations                   # Migration output directory
```

The configuration is intentionally simple - Housekeeper follows convention over configuration principles.

## Environment-Specific Configuration

### Development Configuration

```yaml
# development.yaml
clickhouse:
  version: "25.7"
  cluster: "dev_cluster"

connection:
  host: localhost
  port: 9000
  database: dev_db

migration:
  auto_approve: true                 # Auto-approve in development
  backup_before: false               # Skip backups in development
  dry_run_first: false               # Skip dry runs for speed

logging:
  level: debug                       # Verbose logging in development
```

### Staging Configuration

```yaml
# staging.yaml
clickhouse:
  version: "25.7"
  cluster: "staging_cluster"

connection:
  host: clickhouse-staging.internal
  port: 9440
  database: staging_db
  secure: true                       # Use TLS in staging
  username: staging_user
  password: "${CH_STAGING_PASSWORD}" # Environment variable

migration:
  auto_approve: false                # Manual approval in staging
  backup_before: true                # Always backup staging
  timeout: 600s                      # Longer timeout for staging

logging:
  level: info
  output: /var/log/housekeeper.log   # Log to file
```

### Production Configuration

```yaml
# production.yaml
clickhouse:
  version: "25.7"                    # Pin specific version
  cluster: "production_cluster"

connection:
  host: clickhouse-prod.example.com
  port: 9440
  database: production_db
  secure: true                       # Always use TLS in production
  username: migration_user
  password: "${CH_PRODUCTION_PASSWORD}"
  
  # Production connection settings
  max_open_conns: 5                  # Limit connections in production
  conn_max_lifetime: 600s            # Shorter lifetime in production

migration:
  auto_approve: false                # Never auto-approve in production
  backup_before: true                # Always backup production
  timeout: 1800s                     # Longer timeout for large migrations
  verify_checksums: true             # Always verify in production

logging:
  level: warn                        # Minimal logging in production
  format: json                       # Structured logging
  output: /var/log/housekeeper/app.log
```

## Environment Variables

Use environment variables for sensitive configuration:

### Connection Credentials

```bash
# Database connection
export CH_HOST=localhost
export CH_PORT=9000
export CH_DATABASE=mydb
export CH_USERNAME=myuser
export CH_PASSWORD=secretpassword
export CH_CLUSTER=mycluster

# TLS settings
export CH_SECURE=true
export CH_SKIP_VERIFY=false
```

### Configuration in YAML

Reference environment variables in your configuration:

```yaml
connection:
  host: "${CH_HOST:-localhost}"                    # Default to localhost
  port: "${CH_PORT:-9000}"                         # Default to 9000
  database: "${CH_DATABASE:-default}"              # Default to default
  username: "${CH_USERNAME:-default}"              # Default to default
  password: "${CH_PASSWORD}"                       # Required from environment
  cluster: "${CH_CLUSTER}"                         # Optional cluster
  secure: "${CH_SECURE:-false}"                    # Default to false
```

### Environment-Specific Variables

```bash
# Development
export HOUSEKEEPER_ENV=development
export CH_HOST=localhost
export CH_PASSWORD=devpassword

# Staging
export HOUSEKEEPER_ENV=staging
export CH_HOST=staging-clickhouse.internal
export CH_PASSWORD=stagingpassword

# Production
export HOUSEKEEPER_ENV=production
export CH_HOST=prod-clickhouse.example.com
export CH_PASSWORD=productionpassword
```

## Command-Line Overrides

Override configuration values via command-line flags:

```bash
# Override connection settings
housekeeper diff \
  --host clickhouse-prod.example.com \
  --port 9440 \
  --database analytics \
  --username admin \
  --cluster production

# Override migration settings
housekeeper migrate \
  --timeout 600s \
  --auto-approve \
  --no-backup

# Override configuration file
housekeeper diff --config production.yaml

# Override schema files
housekeeper diff \
  --entrypoint schemas/production.sql \
  --dir migrations/production
```

## ClickHouse Configuration

### Cluster Configuration

Configure ClickHouse clusters in `db/config.d/_clickhouse.xml`:

```xml
<clickhouse>
    <!-- Cluster configuration -->
    <remote_servers>
        <my_cluster>
            <shard>
                <replica>
                    <host>clickhouse-1.example.com</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse-2.example.com</host>
                    <port>9000</port>
                </replica>
            </shard>
            <shard>
                <replica>
                    <host>clickhouse-3.example.com</host>
                    <port>9000</port>
                </replica>
                <replica>
                    <host>clickhouse-4.example.com</host>
                    <port>9000</port>
                </replica>
            </shard>
        </my_cluster>
    </remote_servers>

    <!-- Zookeeper configuration for ReplicatedMergeTree -->
    <zookeeper>
        <node>
            <host>zookeeper-1.example.com</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-2.example.com</host>
            <port>2181</port>
        </node>
        <node>
            <host>zookeeper-3.example.com</host>
            <port>2181</port>
        </node>
    </zookeeper>

    <!-- Macros for replica names -->
    <macros>
        <cluster>my_cluster</cluster>
        <shard>01</shard>
        <replica>replica1</replica>
    </macros>
</clickhouse>
```

### User Configuration

Configure users and permissions:

```xml
<clickhouse>
    <users>
        <!-- Migration user with DDL permissions -->
        <migration_user>
            <password>your_secure_password</password>
            <profile>default</profile>
            <quota>default</quota>
            <allow_databases>
                <database>analytics</database>
                <database>reporting</database>
            </allow_databases>
            <access_management>1</access_management>
        </migration_user>
        
        <!-- Read-only user for applications -->
        <app_user>
            <password>app_password</password>
            <profile>readonly</profile>
            <quota>default</quota>
            <allow_databases>
                <database>analytics</database>
            </allow_databases>
        </app_user>
    </users>
</clickhouse>
```

## Validation

Validate your configuration:

```bash
# Validate configuration file syntax
housekeeper config validate

# Test database connection
housekeeper config test-connection

# Show effective configuration (with environment variables resolved)
housekeeper config show

# Validate specific configuration file
housekeeper config validate --config production.yaml
```

## Configuration Best Practices

### Security

1. **Use Environment Variables**: Never commit passwords to version control
2. **Principle of Least Privilege**: Use dedicated migration users with minimal permissions
3. **TLS in Production**: Always use secure connections in production
4. **Certificate Validation**: Don't skip TLS certificate verification in production

### Environment Management

1. **Separate Configurations**: Use different config files for each environment
2. **Version Control**: Commit configuration templates, not actual credentials
3. **Documentation**: Document configuration changes and their purpose
4. **Validation**: Always validate configuration before deployment

### Performance

1. **Connection Pooling**: Configure appropriate connection pool sizes
2. **Timeouts**: Set reasonable timeouts for your environment
3. **Batch Sizes**: Optimize batch sizes for your migration patterns
4. **Resource Limits**: Set appropriate memory and connection limits

### Maintenance

1. **Regular Reviews**: Review and update configurations regularly
2. **Monitoring**: Monitor configuration-related metrics
3. **Backup**: Backup configuration files along with schemas
4. **Change Management**: Track configuration changes like code changes

## Next Steps

- **[Schema Management](schema-management.md)** - Learn about schema design patterns
- **[Migration Process](migration-process.md)** - Understand how migrations work
- **[Cluster Management](../advanced/cluster-management.md)** - Configure distributed ClickHouse
- **[Troubleshooting](../advanced/troubleshooting.md)** - Solve configuration issues