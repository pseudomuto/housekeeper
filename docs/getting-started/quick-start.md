# Quick Start

This guide will get you up and running with Housekeeper in under 10 minutes.

## Step 1: Initialize a New Project

Create a new directory for your ClickHouse project and initialize it:

```bash
# Create and enter project directory
mkdir my-clickhouse-project
cd my-clickhouse-project

# Initialize the project structure
housekeeper init
```

This creates the following structure:

```
my-clickhouse-project/
├── housekeeper.yaml        # Project configuration
├── db/
│   ├── main.sql            # Main schema entrypoint
│   ├── config.d/           # ClickHouse configuration files
│   │   └── _clickhouse.xml # Generated cluster configuration
│   ├── migrations/         # Generated migration files (empty initially)
│   └── schemas/            # Organized schema files (optional)
```

## Step 2: Define Your Schema

Edit the `db/main.sql` file to define your ClickHouse schema:

```sql
-- Create analytics database
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';

-- Create users table
CREATE TABLE analytics.users (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    metadata Map(String, String) DEFAULT map()
) ENGINE = ReplacingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;

-- Create events table  
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    properties Map(String, String) DEFAULT map()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_type)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Create daily summary view
CREATE MATERIALIZED VIEW analytics.daily_summary
ENGINE = MergeTree() ORDER BY date
AS SELECT 
    toDate(timestamp) as date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY date, event_type;
```

## Step 3: Compile and Validate

Compile your schema to ensure it's valid:

```bash
# Compile schema and check for syntax errors
housekeeper schema compile

# View the compiled output
housekeeper schema compile --out compiled_schema.sql
cat compiled_schema.sql
```

!!! tip "Modular Schemas"
    For larger projects, you can split your schema into multiple files using import directives. See [Project Setup](project-setup.md) for details.

## Step 4: Start Development Server and Generate Migration

Housekeeper uses a development workflow where you start a local ClickHouse server, apply existing migrations, then compare your schema:

```bash
# Start local ClickHouse development server
housekeeper dev up
```

This starts a Docker container with ClickHouse and applies any existing migrations. Since this is your first time, there are no migrations yet.

Now generate a migration by comparing your schema:

```bash
# Generate migration by comparing schema with current database state
housekeeper diff
```

Since this is your first migration, Housekeeper will create a migration file containing all your schema definitions. The file will be created in `db/migrations/` with a timestamp prefix like `20240101120000.sql`.

Example migration content:
```sql
-- Schema migration generated at 2024-01-01 12:00:00 UTC

-- Create database 'analytics'
CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';

-- Create table 'analytics.users'
CREATE TABLE analytics.users (
    id UInt64,
    email String,
    name String,
    created_at DateTime DEFAULT now(),
    metadata Map(String, String) DEFAULT map()
) ENGINE = ReplacingMergeTree()
ORDER BY id
SETTINGS index_granularity = 8192;

-- Create table 'analytics.events'
CREATE TABLE analytics.events (
    id UUID DEFAULT generateUUIDv4(),
    user_id UInt64,
    event_type LowCardinality(String),
    timestamp DateTime DEFAULT now(),
    properties Map(String, String) DEFAULT map()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (timestamp, event_type)
TTL timestamp + INTERVAL 90 DAY
SETTINGS index_granularity = 8192;

-- Create materialized view 'analytics.daily_summary'
CREATE MATERIALIZED VIEW analytics.daily_summary
ENGINE = MergeTree() ORDER BY date
AS SELECT 
    toDate(timestamp) as date,
    event_type,
    count() as event_count,
    uniq(user_id) as unique_users
FROM analytics.events
GROUP BY date, event_type;
```

## Step 5: Development Workflow

The development workflow in Housekeeper is:

1. **Start dev server** - Starts ClickHouse with existing migrations applied
2. **Make schema changes** - Edit your schema files  
3. **Generate migration** - Compare schema with database state
4. **Restart dev server** - Apply new migration and continue

```bash
# Stop the current dev server
housekeeper dev down

# Start fresh with all migrations (including the new one)
housekeeper dev up
```

Your new migration is automatically applied when the dev server starts.

## Step 6: Make Changes and Generate Another Migration

Let's add a new table to demonstrate the migration workflow:

Add this to your `db/main.sql`:

```sql
-- Add after the events table
CREATE TABLE analytics.products (
    id UInt64,
    name String,
    category_id UInt32,
    price Decimal64(2),
    created_at DateTime DEFAULT now()
) ENGINE = ReplacingMergeTree()
ORDER BY id;
```

Generate a new migration:

```bash
# Generate migration for the new changes
housekeeper diff
```

Housekeeper will detect that you've added a new table and generate a migration containing just the new `CREATE TABLE` statement.

## Next Steps

Congratulations! You've successfully:

✅ Created a Housekeeper project  
✅ Defined a ClickHouse schema  
✅ Generated your first migration  
✅ Applied changes to ClickHouse  
✅ Created an incremental migration  

### Learn More

- **[Project Setup](project-setup.md)** - Organize complex schemas with imports and modular structure
- **[Schema Management](../user-guide/schema-management.md)** - Learn best practices for schema design
- **[Migration Process](../user-guide/migration-process.md)** - Understand how migrations work under the hood
- **[Configuration](../user-guide/configuration.md)** - Customize Housekeeper for your environment

### Working with Existing Databases

If you have an existing ClickHouse database, use the bootstrap workflow:

```bash
# Step 1: Extract schema from existing database
housekeeper bootstrap --url localhost:9000

# Step 2: Create initial snapshot (no migrations required)
housekeeper snapshot --bootstrap --description "Initial baseline"

# Step 3: Now you can generate migrations normally
housekeeper diff
```

The `--bootstrap` flag creates a snapshot from your project schema instead of existing migrations, solving the chicken-and-egg problem when starting with an existing database.

### Common Next Steps

1. **Set up CI/CD**: Integrate Housekeeper into your deployment pipeline
2. **Cluster Configuration**: Configure for distributed ClickHouse deployments
3. **Migration Consolidation**: Use `housekeeper snapshot` to consolidate old migrations
4. **Team Workflow**: Establish schema change processes with your team

### Getting Help

- Check out the [Examples](../examples/basic-schema.md) for more schema patterns
- Read the [Troubleshooting Guide](../advanced/troubleshooting.md) for common issues
- [Open an issue](https://github.com/pseudomuto/housekeeper/issues) if you need help