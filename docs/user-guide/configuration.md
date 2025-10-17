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
  ignore_databases: []               # Databases to exclude from schema operations

entrypoint: db/main.sql              # Main schema file
dir: db/migrations                   # Migration output directory
```

## Configuration Sections

### Format Options

Configure SQL formatting preferences to customize how Housekeeper generates and formats SQL output:

```yaml
format_options:
  # Basic formatting settings
  indent_size: 4                         # Number of spaces for indentation (default: 4)
  max_line_length: 120                   # Maximum line length before wrapping (default: 120)
  uppercase_keywords: true               # Use uppercase SQL keywords (default: true)
  align_columns: true                    # Align column definitions in tables (default: true)
  
  # Function formatting settings
  multiline_functions: true              # Enable multi-line function formatting (default: true)
  function_arg_threshold: 4              # Arguments needed to trigger multi-line (default: 4)
  function_indent_size: 4                # Extra indentation for function args (default: 4)
  
  # Advanced function formatting
  smart_function_pairing: true           # Enable intelligent argument pairing (default: true)
  pair_size: 2                           # Arguments per pair for conditional functions (default: 2)
  
  # Function names for specific formatting behavior
  multiline_function_names:              # Functions that should always be multi-line
    - "multiIf"
    - "case"
    - "transform"
    - "multiSearchAllPositions"
  paired_function_names:                 # Functions that use paired argument formatting
    - "multiIf"
    - "if"
    - "case"
    - "transform"
```

#### Format Option Details

**Basic Formatting**:
- `indent_size`: Controls the number of spaces used for each level of indentation in formatted SQL
- `max_line_length`: Suggests when to break long lines (0 = no limit)
- `uppercase_keywords`: When true, SQL keywords like `CREATE`, `TABLE`, `SELECT` are formatted in uppercase
- `align_columns`: When true, column definitions in `CREATE TABLE` statements are aligned for readability

**Function Formatting**:
- `multiline_functions`: Enables breaking complex function calls across multiple lines
- `function_arg_threshold`: Number of arguments that triggers multi-line formatting for functions
- `function_indent_size`: Additional indentation applied to function arguments (defaults to `indent_size`)

**Smart Function Pairing** (New):
- `smart_function_pairing`: Enables intelligent pairing of arguments for conditional functions like `multiIf`
- `pair_size`: How many arguments constitute a logical pair (typically 2 for condition-value pairs)
- `multiline_function_names`: Function names that should always use multi-line formatting
- `paired_function_names`: Function names that should use paired argument formatting

#### Smart Function Pairing Example

With smart function pairing enabled, conditional functions are formatted with logical argument groupings:

```sql
-- Before (each argument on separate line):
CREATE FUNCTION calculate_discount AS (price, category) -> multiIf(
  category = 'premium',
  multiply(price, 0.9),
  category = 'standard',
  multiply(price, 0.95),
  price
);

-- After (condition-value pairs on same line):
CREATE FUNCTION calculate_discount AS (price, category) -> multiIf(
  category = 'premium', multiply(price, 0.9),
  category = 'standard', multiply(price, 0.95),
  price
);
```

#### Configuration Inheritance

Format options follow a merging strategy where user values override defaults:

```yaml
# Only specify the options you want to change
format_options:
  indent_size: 2                         # Override default of 4
  uppercase_keywords: false              # Override default of true
  # All other options will use their default values
```

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
  
  # Databases to exclude from schema operations
  ignore_databases:                  # Useful for test/staging databases
    - testing_db
    - temp_analytics
```

### Schema Configuration

Configure schema-related settings:

```yaml
# Schema entrypoint and organization
entrypoint: db/main.sql              # Main schema file with imports
dir: db/migrations                   # Migration output directory
```

The configuration is intentionally simple - Housekeeper follows convention over configuration principles.

### Ignoring Databases

The `ignore_databases` configuration allows you to exclude specific databases from schema operations like `diff` and `dump`. This is particularly useful for:

- **Testing databases**: Keep test databases separate from production schemas
- **Temporary databases**: Exclude temporary or experimental databases
- **System databases**: Additional system databases beyond the defaults

```yaml
clickhouse:
  ignore_databases:
    - testing_db        # Development testing database
    - staging_temp      # Temporary staging experiments
    - analytics_v1      # Old version being phased out
```

Ignored databases will be completely excluded from:
- Schema dumps (`housekeeper schema dump`)
- Migration generation (`housekeeper diff`)
- Bootstrap operations (`housekeeper bootstrap`)

You can also specify ignored databases via the command line for one-off operations:

```bash
# Exclude databases when dumping schema
housekeeper schema dump --url localhost:9000 \
  --ignore-databases testing_db \
  --ignore-databases temp_db
```

Note: System databases (`default`, `system`, `information_schema`, `INFORMATION_SCHEMA`) are always excluded automatically.

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

The recommended way to configure database connections is using the `HOUSEKEEPER_DATABASE_URL` environment variable:

```bash
# Recommended: Single connection URL (used by all commands)
export HOUSEKEEPER_DATABASE_URL="localhost:9000"

# Or with full DSN including authentication
export HOUSEKEEPER_DATABASE_URL="clickhouse://myuser:secretpassword@localhost:9000/mydb"

# Or with TCP protocol and parameters
export HOUSEKEEPER_DATABASE_URL="tcp://localhost:9000?username=myuser&password=secretpassword&database=mydb"
```

Once set, all commands will use this connection automatically:

```bash
# No need to specify --url flag when environment variable is set
housekeeper migrate
housekeeper status
housekeeper schema dump
housekeeper bootstrap
```

### Alternative: Command-Line Flags

You can also specify the connection directly using the `--url` flag:

```bash
housekeeper migrate --url localhost:9000
housekeeper status --url "clickhouse://user:pass@host:9000/db"
```

### Configuration in YAML

Environment variables are used by the CLI tools for connection parameters, but are not part of the YAML configuration file. The `HOUSEKEEPER_DATABASE_URL` environment variable or `--url` flag provides the connection details when running commands.

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

## Format Options in Practice

### Development vs Production Formatting

Consider different formatting preferences for different environments:

```yaml
# development.yaml - More compact for quick reading
format_options:
  indent_size: 2
  align_columns: true
  multiline_functions: false          # Keep functions compact during development
  function_arg_threshold: 5           # Higher threshold for multi-line

# production.yaml - More readable for reviews and documentation
format_options:
  indent_size: 4                      # Default, but explicit for team clarity
  align_columns: true
  uppercase_keywords: true            # Default, formal appearance
  multiline_functions: true
  function_arg_threshold: 3           # Lower threshold for better readability
  smart_function_pairing: true        # Default, improve conditional function readability
```

### Team Formatting Standards

Establish consistent formatting across your team by committing format options to version control:

```yaml
# .housekeeper/format.yaml - Team-wide formatting standards
format_options:
  # Consistent indentation
  indent_size: 2
  max_line_length: 100
  
  # Readable function formatting
  multiline_functions: true
  function_arg_threshold: 3
  smart_function_pairing: true
  
  # Standard function lists
  multiline_function_names:
    - "multiIf"
    - "transform"
    - "arrayMap"
  paired_function_names:
    - "multiIf"
    - "if"
    - "case"
```

### Migration File Formatting

Format options affect all generated SQL, including migration files:

```bash
# Generate migration with custom formatting
housekeeper diff --config custom-format.yaml

# The resulting migration will use your format preferences:
# - Proper indentation for readability
# - Aligned columns in CREATE TABLE statements  
# - Smart pairing for conditional functions
# - Consistent keyword casing
```

## Configuration Best Practices

### Formatting

1. **Team Consistency**: Establish team-wide formatting standards in your main configuration
2. **Environment Specific**: Consider different formatting for development vs production
3. **Function Readability**: Use smart function pairing for complex conditional logic
4. **Line Length**: Set appropriate `max_line_length` for your code review tools
5. **Documentation**: Comment your format choices in the configuration file

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