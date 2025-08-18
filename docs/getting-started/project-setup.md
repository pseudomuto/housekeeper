# Project Setup

Learn how to organize and structure your Housekeeper projects for maintainability and team collaboration.

## Project Structure

A well-organized Housekeeper project follows this recommended structure:

```
my-clickhouse-project/
├── housekeeper.yaml          # Project configuration
├── db/
│   ├── main.sql              # Main schema entrypoint
│   ├── config.d/             # ClickHouse configuration files
│   │   ├── _clickhouse.xml   # Generated cluster configuration
│   │   └── custom.xml        # Your custom ClickHouse config
│   ├── migrations/           # Generated migration files
│   │   ├── 20240101120000.sql
│   │   ├── 20240101130000.sql
│   │   └── housekeeper.sum   # Migration integrity checksum
│   └── schemas/              # Modular schema organization
│       ├── analytics/
│       │   ├── schema.sql    # Database definition
│       │   ├── tables/       # Table definitions
│       │   │   ├── events.sql
│       │   │   ├── users.sql
│       │   │   └── products.sql
│       │   ├── dictionaries/ # Dictionary definitions
│       │   │   ├── countries.sql
│       │   │   └── categories.sql
│       │   └── views/        # View definitions
│       │       ├── daily_stats.sql
│       │       └── user_summary.sql
│       └── logs/
│           ├── schema.sql
│           └── tables/
│               └── access_logs.sql
├── environments/             # Environment-specific configs
│   ├── development.yaml
│   ├── staging.yaml
│   └── production.yaml
└── README.md                 # Project documentation
```

## Configuration File

The `housekeeper.yaml` file defines your project configuration:

```yaml
# ClickHouse Configuration
clickhouse:
  version: "25.7"             # Version for Docker containers
  config_dir: "db/config.d"   # ClickHouse configuration directory
  cluster: "my_cluster"       # Default cluster name for ON CLUSTER operations

# Schema Configuration  
entrypoint: db/main.sql       # Main schema file with import directives
dir: db/migrations            # Directory for generated migration files

# Connection Settings (optional - can be overridden by CLI)
connection:
  host: localhost
  port: 9000
  database: default
  username: default
  password: ""
  cluster: my_cluster         # For cluster-aware operations

# Migration Settings
migration:
  auto_approve: false         # Require manual approval for migrations
  backup_before: true         # Create backups before applying migrations
  timeout: 300s               # Timeout for migration operations
```

## Schema Import System

The import system allows you to organize complex schemas into manageable, modular files.

### Main Schema File

Your `db/main.sql` serves as the entrypoint:

```sql
-- Main schema file
-- This file coordinates all schema imports

-- Import database definitions first
-- housekeeper:import schemas/analytics/schema.sql
-- housekeeper:import schemas/logs/schema.sql

-- Import table definitions
-- housekeeper:import schemas/analytics/tables/users.sql
-- housekeeper:import schemas/analytics/tables/events.sql
-- housekeeper:import schemas/analytics/tables/products.sql

-- Import dictionaries
-- housekeeper:import schemas/analytics/dictionaries/countries.sql
-- housekeeper:import schemas/analytics/dictionaries/categories.sql

-- Import views (depends on tables and dictionaries)
-- housekeeper:import schemas/analytics/views/daily_stats.sql
-- housekeeper:import schemas/analytics/views/user_summary.sql
```

### Database Schema Files

`db/schemas/analytics/schema.sql`:
```sql
-- Analytics database definition
CREATE DATABASE analytics 
ON CLUSTER my_cluster 
ENGINE = Atomic 
COMMENT 'Analytics and user behavior database';
```

### Table Definition Files

`db/schemas/analytics/tables/users.sql`:
```sql
-- User profiles table
CREATE TABLE analytics.users ON CLUSTER my_cluster (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now(),
    status LowCardinality(String) DEFAULT 'active',
    metadata Map(String, String) DEFAULT map(),
    
    -- Materialized columns for analytics
    signup_month UInt32 MATERIALIZED toYYYYMM(created_at),
    email_domain String MATERIALIZED splitByChar('@', email)[2]
) 
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY id
PARTITION BY signup_month
SETTINGS index_granularity = 8192;
```

`db/schemas/analytics/tables/events.sql`:
```sql
-- User event tracking table
CREATE TABLE analytics.events ON CLUSTER my_cluster (
    id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    session_id String,
    properties Map(String, String) DEFAULT map(),
    
    -- Derived columns
    date Date MATERIALIZED toDate(timestamp),
    hour UInt8 MATERIALIZED toHour(timestamp)
)
ENGINE = MergeTree()
PARTITION BY (toYYYYMM(timestamp), event_type)
ORDER BY (timestamp, user_id, event_type)
TTL timestamp + INTERVAL 365 DAY
SETTINGS index_granularity = 8192;
```

### Dictionary Definition Files

`db/schemas/analytics/dictionaries/countries.sql`:
```sql
-- Country lookup dictionary
CREATE DICTIONARY analytics.countries_dict ON CLUSTER my_cluster (
    country_code String,
    country_name String,
    continent String,
    population UInt64 DEFAULT 0
)
PRIMARY KEY country_code
SOURCE(HTTP(
    url 'https://api.example.com/countries'
    format 'JSONEachRow'
    headers('Authorization' 'Bearer YOUR_TOKEN')
))
LAYOUT(HASHED())
LIFETIME(MIN 3600 MAX 7200)
COMMENT 'Country reference data from external API';
```

### View Definition Files

`db/schemas/analytics/views/daily_stats.sql`:
```sql
-- Daily analytics summary view
CREATE MATERIALIZED VIEW analytics.daily_stats ON CLUSTER my_cluster
ENGINE = MergeTree() 
ORDER BY (date, event_type)
POPULATE
AS SELECT 
    date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users,
    uniq(session_id) as unique_sessions,
    countIf(user_id = 0) as anonymous_events,
    quantile(0.5)(length(JSONExtractString(properties, 'page_url'))) as median_url_length
FROM analytics.events
WHERE date >= today() - INTERVAL 90 DAY
GROUP BY date, event_type;
```

## Import Resolution

The import system works as follows:

1. **Relative Paths**: Import paths are resolved relative to the importing file
2. **Recursive Processing**: Imported files can import other files
3. **Ordering**: Imports are processed in the order they appear
4. **Single Output**: All imports are combined into a single SQL stream

### Example Import Resolution

Given this structure:
```
db/
├── main.sql
└── schemas/
    └── analytics/
        ├── schema.sql
        └── tables/
            └── users.sql
```

In `db/main.sql`:
```sql
-- housekeeper:import schemas/analytics/schema.sql
```

In `db/schemas/analytics/schema.sql`:
```sql
CREATE DATABASE analytics ENGINE = Atomic;
-- housekeeper:import tables/users.sql
```

The path `tables/users.sql` is resolved relative to `db/schemas/analytics/schema.sql`, resulting in `db/schemas/analytics/tables/users.sql`.

## Environment-Specific Configuration

For different environments, create separate configuration files:

### `environments/development.yaml`
```yaml
clickhouse:
  version: "25.7"
  cluster: "dev_cluster"

connection:
  host: localhost
  port: 9000
  database: default

migration:
  auto_approve: true  # Auto-approve in development
  backup_before: false
```

### `environments/production.yaml`
```yaml
clickhouse:
  version: "25.7"
  cluster: "production_cluster"

connection:
  host: clickhouse-prod.example.com
  port: 9440
  database: default
  username: migration_user
  password: "${CH_MIGRATION_PASSWORD}"  # From environment variable

migration:
  auto_approve: false  # Require manual approval
  backup_before: true
  timeout: 600s
```

Use environment-specific configs:
```bash
# Development
housekeeper diff --config environments/development.yaml

# Production  
housekeeper diff --config environments/production.yaml
```

## Best Practices

### File Organization

1. **One Object Per File**: Each table, dictionary, or view in its own file
2. **Logical Grouping**: Group related objects in directories
3. **Descriptive Names**: Use clear, descriptive filenames
4. **Consistent Structure**: Maintain consistent directory structure across projects

### Import Guidelines

1. **Dependency Order**: Import dependencies before dependents
2. **Database First**: Always import database definitions first
3. **Tables Before Views**: Import tables before views that depend on them
4. **Dictionaries Before Tables**: Import dictionaries before tables that reference them

### Schema Design

1. **Use Comments**: Document your schema with SQL comments
2. **Cluster Awareness**: Include `ON CLUSTER` clauses for distributed deployments
3. **Performance Optimization**: Choose appropriate partition keys and ordering
4. **Data Lifecycle**: Use TTL for data retention policies

### Version Control

1. **Track Everything**: Commit all schema files, configurations, and migrations
2. **Meaningful Commits**: Use descriptive commit messages for schema changes
3. **Branch Strategy**: Use feature branches for schema changes
4. **Code Reviews**: Review schema changes like application code

## Migration Management

### Migration Integrity

Housekeeper generates a `housekeeper.sum` file that tracks migration integrity:

```
h1:TotalHashOfAllMigrations=
20240101120000.sql h1:HashOfMigration1=
20240101130000.sql h1:ChainedHashWithPrevious=
```

This prevents unauthorized migration modifications and ensures consistency across environments.

### Migration Workflow

1. **Make Schema Changes**: Edit your schema files
2. **Generate Migration**: Run `housekeeper diff`
3. **Review Migration**: Examine the generated SQL
4. **Test Migration**: Apply to development environment
5. **Commit Changes**: Commit schema files and migration together
6. **Deploy**: Apply migration to staging/production

## Next Steps

- **[Schema Management](../user-guide/schema-management.md)** - Learn best practices for writing schemas
- **[Migration Process](../user-guide/migration-process.md)** - Understand the migration workflow
- **[Cluster Management](../advanced/cluster-management.md)** - Configure for distributed ClickHouse
- **[Examples](../examples/ecommerce-demo.md)** - See a complete example project