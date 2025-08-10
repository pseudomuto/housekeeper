# Housekeeper Demo Project

This directory contains a complete example of a Housekeeper project for managing ClickHouse schemas. It demonstrates a typical e-commerce analytics setup with various ClickHouse features and objects.

## Project Structure

```
examples/demo/
├── housekeeper.yaml          # Project configuration
├── db/
│   ├── main.sql              # Main entrypoint schema file
│   ├── config.d/             # ClickHouse configuration files
│   │   └── _clickhouse.xml   # Complete cluster, keeper, macros, and user configuration
│   ├── schemas/              # Schema definitions
│   │   └── ecommerce/        # Database-specific schema files
│   │       ├── schema.sql    # Database schema coordinator
│   │       ├── tables/       # Individual table definitions
│   │       │   ├── users.sql
│   │       │   ├── events.sql
│   │       │   ├── products.sql
│   │       │   ├── orders.sql
│   │       │   └── order_items.sql
│   │       ├── dictionaries/ # Individual dictionary definitions
│   │       │   ├── countries.sql
│   │       │   ├── categories.sql
│   │       │   └── user_segments.sql
│   │       └── views/        # Individual view definitions
│   │           ├── daily_sales.sql
│   │           ├── user_activity.sql
│   │           ├── top_products.sql
│   │           ├── mv_product_stats.sql
│   │           └── mv_hourly_events.sql
│   └── migrations/           # Example migration files
│       ├── 20240801120000.sql
│       ├── 20240801120100.sql
│       └── 20240801120200.sql
└── README.md                # This file
```

## Schema Overview

The demo schema represents an e-commerce analytics platform with the following components:

### Database
- `ecommerce` - Main analytics database using Atomic engine

### Tables
- `events` - User interaction tracking (MergeTree with partitioning)
- `users` - Customer profiles (ReplacingMergeTree with versioning)
- `products` - Product catalog (ReplacingMergeTree with versioning)
- `orders` - Transaction records (MergeTree with TTL)
- `order_items` - Order line items (MergeTree)

### Dictionaries
- `countries_dict` - Country lookup from external API (HTTP source)
- `categories_dict` - Product category hierarchy (ClickHouse source with hierarchical support)
- `user_segments_dict` - ML-based user segmentation (HTTP source with authentication)

### Views
- `daily_sales` - Daily sales summary (regular view)
- `user_activity` - User activity status analysis (regular view)
- `top_products` - Top products by category with ranking (regular view)
- `mv_product_stats` - Product performance metrics (materialized view)
- `mv_hourly_events` - Real-time event aggregation (materialized view)

## Key Features Demonstrated

1. **Schema Import System**: Uses `-- housekeeper:import` directives to modularize schema files
2. **Cluster Configuration**: Complete setup for distributed ClickHouse deployment
3. **Various Table Engines**: MergeTree, ReplacingMergeTree with different configurations
4. **Advanced Column Types**: Map, Array, Nullable, LowCardinality, Decimal64
5. **Column Attributes**: DEFAULT, MATERIALIZED, CODEC, TTL
6. **Table Features**: Partitioning, TTL, custom settings
7. **Dictionary Sources**: HTTP, ClickHouse with various layouts and lifetimes
8. **View Types**: Regular views, materialized views with different engines
9. **Complex Queries**: JOINs, CTEs, window functions, aggregations
10. **Cluster Deployment**: All DDL statements include `ON CLUSTER demo` for distributed execution

## Usage Examples

### Initialize a New Project
```bash
# Copy this demo as a starting point
cp -r examples/demo my-project
cd my-project

# Customize the schema files for your use case
# Edit db/schemas/*.sql files
# Update housekeeper.yaml configuration
```

### Parse the Schema
```bash
# From the housekeeper root directory
go run cmd/housekeeper/main.go parse examples/demo/db/main.sql
```

### Generate Migration
```bash
# Compare current schema with a modified version
go run cmd/housekeeper/main.go diff \
  --current examples/demo/db/main.sql \
  --target examples/demo/db/main_modified.sql \
  --output examples/demo/db/migrations/
```

### Validate Migration Files
```bash
# Check that migration files are valid
go run cmd/housekeeper/main.go validate examples/demo/db/migrations/
```

## Best Practices Demonstrated

1. **Modular Schema Organization**: Separate files for different object types
2. **Meaningful Naming**: Descriptive names for tables, columns, and views
3. **Proper Data Types**: Appropriate use of ClickHouse-specific types
4. **Performance Optimization**: Partitioning, ordering keys, and materialized columns
5. **Data Lifecycle**: TTL policies for data retention
6. **External Integration**: Dictionary sources from APIs and other databases
7. **Real-time Analytics**: Materialized views for continuous aggregation

## Schema Evolution

The migration files show a typical evolution:
1. **Initial**: Basic user events and profiles
2. **Expansion**: Add products and orders for e-commerce
3. **Analytics**: Add dictionaries and analytical views

This demonstrates how schemas grow over time and how migrations manage these changes safely.