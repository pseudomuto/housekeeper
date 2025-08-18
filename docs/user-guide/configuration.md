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
  config_dir: "db/config.d"
  cluster: "dev_cluster"

entrypoint: db/main.sql
dir: db/migrations
```

### Staging Configuration

```yaml
# staging.yaml
clickhouse:
  version: "25.7"
  config_dir: "db/config.d"
  cluster: "staging_cluster"

entrypoint: db/main.sql
dir: db/migrations
```

### Production Configuration

```yaml
# production.yaml  
clickhouse:
  version: "25.7"                    # Pin specific version
  config_dir: "db/config.d"
  cluster: "production_cluster"

entrypoint: db/main.sql
dir: db/migrations
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

Environment variables are used by the CLI tools for connection parameters, but are not part of the YAML configuration file. Connection details are provided via command-line flags or environment variables when running commands.

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

# Start development server with specific project directory
housekeeper dev up -d production

# Use specific project directory (which contains housekeeper.yaml)
housekeeper diff -d production
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
# Validate schema syntax
housekeeper schema compile

# Test database connection by dumping schema
housekeeper schema dump --url localhost:9000

# Show available commands and options
housekeeper --help
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