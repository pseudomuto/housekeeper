# Role Management

Housekeeper provides comprehensive support for managing ClickHouse roles and their associated privileges. Roles are global objects that exist at the cluster level and provide a powerful way to manage access control across your ClickHouse deployment.

## Overview

ClickHouse roles enable you to:
- Define reusable sets of permissions
- Assign roles to users for consistent access control
- Manage privileges at scale across multiple databases and tables
- Implement role-based access control (RBAC) patterns

Housekeeper treats roles as **global objects** and processes them first in migrations, ensuring they're available when other database objects that might reference them are created.

## Role Organization

Roles are stored in the `db/schemas/_global/roles/` directory structure:

```
db/schemas/_global/roles/
├── main.sql              # Main entry point for role definitions
├── analytics_roles.sql   # Analytics team roles
├── admin_roles.sql       # Administrative roles
└── data_access_roles.sql # Data access roles
```

Your main schema file imports roles first:

```sql
-- db/main.sql
-- Import global objects first (roles are processed before databases)
-- housekeeper:import schemas/_global/roles/main.sql
-- housekeeper:import schemas/analytics/schema.sql
```

## Supported Role Operations

Housekeeper supports all ClickHouse role DDL operations:

| Operation | Syntax | Migration Support |
|-----------|--------|-------------------|
| **CREATE ROLE** | `CREATE ROLE [IF NOT EXISTS] name [SETTINGS ...]` | ✅ Full support |
| **ALTER ROLE** | `ALTER ROLE [IF EXISTS] name [RENAME TO new_name] [SETTINGS ...]` | ✅ Settings and rename |
| **DROP ROLE** | `DROP ROLE [IF EXISTS] name [,...]` | ✅ Full support |
| **GRANT** | `GRANT privilege[,...] [ON target] TO role [WITH GRANT OPTION]` | ✅ Full support |
| **REVOKE** | `REVOKE [GRANT OPTION FOR] privilege[,...] [ON target] FROM role` | ✅ Full support |
| **SET ROLE** | `SET ROLE {DEFAULT\|NONE\|ALL\|role[,...]}` | ✅ Session management |
| **SET DEFAULT ROLE** | `SET DEFAULT ROLE role[,...] TO user[,...]` | ✅ User defaults |

## Basic Role Examples

### Creating Roles

```sql
-- Read-only analyst role
CREATE ROLE IF NOT EXISTS analytics_reader;
GRANT SELECT ON analytics.* TO analytics_reader;

-- Data writer with limited permissions  
CREATE ROLE IF NOT EXISTS data_writer;
GRANT SELECT, INSERT ON analytics.events TO data_writer;
GRANT SELECT ON analytics.users TO data_writer;

-- Admin role with full permissions
CREATE ROLE IF NOT EXISTS db_admin;
GRANT ALL ON *.* TO db_admin WITH GRANT OPTION;

-- Role with specific settings
CREATE ROLE IF NOT EXISTS developer 
SETTINGS max_memory_usage = 10000000000, readonly = 0;
```

### Granting Privileges

```sql
-- Grant table-specific permissions
GRANT SELECT, INSERT ON analytics.events TO data_writer;

-- Grant database-wide permissions
GRANT SELECT ON analytics.* TO analytics_reader;

-- Grant global permissions
GRANT SHOW TABLES ON *.* TO analytics_reader;

-- Grant with options
GRANT SELECT ON analytics.sensitive_data TO senior_analyst WITH GRANT OPTION;
```

### Role Hierarchies

```sql
-- Create role hierarchy
CREATE ROLE IF NOT EXISTS junior_analyst;
CREATE ROLE IF NOT EXISTS senior_analyst;
CREATE ROLE IF NOT EXISTS lead_analyst;

-- Grant basic permissions to junior role
GRANT SELECT ON analytics.public_data TO junior_analyst;

-- Grant junior role permissions plus additional access to senior role
GRANT junior_analyst TO senior_analyst;
GRANT SELECT ON analytics.detailed_reports TO senior_analyst;

-- Grant senior role permissions plus admin capabilities to lead role
GRANT senior_analyst TO lead_analyst;
GRANT SELECT, INSERT, UPDATE ON analytics.* TO lead_analyst;
```

## Advanced Role Patterns

### Environment-Specific Roles

```sql
-- Development environment roles
CREATE ROLE IF NOT EXISTS dev_reader;
CREATE ROLE IF NOT EXISTS dev_writer;
GRANT SELECT ON dev_analytics.* TO dev_reader;
GRANT SELECT, INSERT, UPDATE, DELETE ON dev_analytics.* TO dev_writer;

-- Production environment roles (more restrictive)
CREATE ROLE IF NOT EXISTS prod_reader;
CREATE ROLE IF NOT EXISTS prod_writer;
GRANT SELECT ON prod_analytics.events, prod_analytics.users TO prod_reader;
GRANT INSERT ON prod_analytics.events TO prod_writer;
```

### Application-Specific Roles

```sql
-- API service role
CREATE ROLE IF NOT EXISTS api_service 
SETTINGS max_concurrent_queries_for_user = 100;
GRANT SELECT ON analytics.events TO api_service;
GRANT INSERT ON analytics.api_logs TO api_service;

-- ETL pipeline role
CREATE ROLE IF NOT EXISTS etl_pipeline
SETTINGS max_memory_usage = 50000000000;
GRANT SELECT ON raw_data.* TO etl_pipeline;
GRANT INSERT, SELECT ON analytics.* TO etl_pipeline;
GRANT CREATE TABLE ON analytics.* TO etl_pipeline;

-- Reporting service role
CREATE ROLE IF NOT EXISTS reporting_service;
GRANT SELECT ON analytics.aggregated_views TO reporting_service;
GRANT CREATE VIEW ON analytics.* TO reporting_service;
```

### Modular Role Organization

You can organize roles across multiple files and import them:

```sql
-- db/schemas/_global/roles/main.sql
-- Import role definitions by category
-- housekeeper:import analytics_roles.sql
-- housekeeper:import admin_roles.sql  
-- housekeeper:import application_roles.sql
```

```sql
-- db/schemas/_global/roles/analytics_roles.sql
-- Analytics team roles
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS senior_data_analyst;
CREATE ROLE IF NOT EXISTS analytics_admin;

GRANT SELECT ON analytics.* TO data_analyst;
GRANT data_analyst TO senior_data_analyst;
GRANT SELECT, INSERT, UPDATE ON analytics.config TO senior_data_analyst;
GRANT senior_data_analyst TO analytics_admin;
GRANT CREATE TABLE, DROP TABLE ON analytics.* TO analytics_admin;
```

## Cluster-Aware Roles

When using ClickHouse clusters, Housekeeper automatically adds `ON CLUSTER` clauses:

```sql
-- Your schema definition
CREATE ROLE IF NOT EXISTS analytics_reader;
GRANT SELECT ON analytics.* TO analytics_reader;

-- Generated migration (with cluster configured)
CREATE ROLE IF NOT EXISTS `analytics_reader` ON CLUSTER `production`;
GRANT `SELECT` ON `analytics`.* TO `analytics_reader` ON CLUSTER `production`;
```

Configure cluster support in `housekeeper.yaml`:

```yaml
clickhouse:
  cluster: production
  version: "24.3"
```

## Migration Behavior

### Processing Order

Roles are processed **first** in migrations to ensure they're available when other objects need them:

1. **Roles** (CREATE → ALTER → RENAME → GRANT → REVOKE → DROP)
2. **Databases** (CREATE → ALTER → RENAME → DROP)  
3. **Tables** (CREATE → ALTER → RENAME → DROP)
4. **Dictionaries** (CREATE → REPLACE → RENAME → DROP)
5. **Views** (CREATE → ALTER → RENAME → DROP)

### Intelligent Operations

Housekeeper generates efficient migrations for role changes:

```sql
-- Role rename (uses ALTER ROLE...RENAME TO instead of DROP+CREATE)
-- Current: CREATE ROLE old_analyst;
-- Target:  CREATE ROLE data_analyst;
-- Generated: ALTER ROLE `old_analyst` RENAME TO `data_analyst`;

-- Permission changes (generates precise GRANT/REVOKE)
-- Current: GRANT SELECT ON analytics.events TO analyst;
-- Target:  GRANT SELECT ON analytics.* TO analyst;  
-- Generated: GRANT `SELECT` ON `analytics`.* TO `analyst`;
--            REVOKE `SELECT` ON `analytics`.`events` FROM `analyst`;
```

### Schema Extraction

Housekeeper can extract existing roles from live ClickHouse instances:

```bash
# Extract current schema including roles
housekeeper schema dump --url localhost:9000 > current_schema.sql

# The extracted schema will include:
# - All role definitions with their settings
# - All grants and permissions
# - Proper ON CLUSTER clauses if configured
```

## Best Practices

### 1. Role Naming Conventions

Use consistent naming patterns:

```sql
-- Environment prefixes
CREATE ROLE IF NOT EXISTS prod_data_reader;
CREATE ROLE IF NOT EXISTS dev_data_writer;

-- Functional groupings  
CREATE ROLE IF NOT EXISTS analytics_readonly;
CREATE ROLE IF NOT EXISTS reporting_readwrite;
CREATE ROLE IF NOT EXISTS etl_admin;

-- Permission levels
CREATE ROLE IF NOT EXISTS viewer;
CREATE ROLE IF NOT EXISTS editor;  
CREATE ROLE IF NOT EXISTS admin;
```

### 2. Principle of Least Privilege

Start with minimal permissions and add as needed:

```sql
-- Start restrictive
CREATE ROLE IF NOT EXISTS app_service;
GRANT SELECT ON app.public_data TO app_service;

-- Add permissions incrementally
GRANT INSERT ON app.user_events TO app_service;
GRANT SELECT ON app.configuration TO app_service;
```

### 3. Role Hierarchies

Use role inheritance to reduce duplication:

```sql
-- Base permissions
CREATE ROLE IF NOT EXISTS base_user;
GRANT SHOW TABLES ON *.* TO base_user;

-- Specialized roles inherit base permissions
CREATE ROLE IF NOT EXISTS data_viewer;
GRANT base_user TO data_viewer;
GRANT SELECT ON analytics.* TO data_viewer;

CREATE ROLE IF NOT EXISTS data_editor;  
GRANT data_viewer TO data_editor;
GRANT INSERT, UPDATE ON analytics.mutable_tables TO data_editor;
```

### 4. Documentation

Document role purposes and permissions:

```sql
-- Analytics team read-only access
-- Permissions: SELECT on all analytics tables and views
-- Use case: Dashboard queries, report generation
-- Assigned to: analysts, data scientists
CREATE ROLE IF NOT EXISTS analytics_reader;
GRANT SELECT ON analytics.* TO analytics_reader;
GRANT SHOW TABLES ON analytics.* TO analytics_reader;
```

### 5. Regular Auditing

Regularly review and audit role permissions:

```sql
-- Query to review role permissions
SELECT 
    role_name,
    database,
    table,
    privilege,
    grant_option
FROM system.grants 
WHERE grantee_type = 'ROLE'
ORDER BY role_name, database, table;
```

## Common Patterns

### Service Account Roles

```sql
-- Microservice roles with specific resource limits
CREATE ROLE IF NOT EXISTS user_service
SETTINGS max_memory_usage = 1000000000, max_execution_time = 30;
GRANT SELECT ON users.profiles, users.preferences TO user_service;
GRANT INSERT ON users.audit_log TO user_service;

CREATE ROLE IF NOT EXISTS order_service  
SETTINGS max_memory_usage = 2000000000, max_execution_time = 60;
GRANT SELECT ON orders.*, inventory.* TO order_service;
GRANT INSERT, UPDATE ON orders.orders TO order_service;
```

### Team-Based Access

```sql
-- Engineering team
CREATE ROLE IF NOT EXISTS engineering_team;
GRANT SELECT ON system.* TO engineering_team;  -- System monitoring
GRANT SELECT ON application_logs.* TO engineering_team;
GRANT CREATE VIEW ON dashboard.* TO engineering_team;

-- Data science team
CREATE ROLE IF NOT EXISTS data_science_team;
GRANT SELECT ON analytics.*, raw_data.* TO data_science_team;
GRANT CREATE TABLE ON experiments.* TO data_science_team;
GRANT INSERT, UPDATE, DELETE ON experiments.* TO data_science_team;

-- Business intelligence team
CREATE ROLE IF NOT EXISTS bi_team;
GRANT SELECT ON analytics.aggregated_* TO bi_team;
GRANT CREATE VIEW ON reports.* TO bi_team;
```

## Troubleshooting

### Common Issues

1. **Role not found errors**
   ```
   Error: Role 'analytics_reader' not found
   ```
   - Ensure roles are created before they're referenced
   - Check that role imports come before database imports

2. **Permission denied errors**
   ```  
   Error: Not enough privileges for user 'app_user' 
   ```
   - Verify the user has been granted the required role
   - Check that the role has the necessary permissions

3. **Cluster synchronization**
   ```
   Error: Role exists on some cluster nodes but not others
   ```
   - Ensure all role operations include `ON CLUSTER` clauses
   - Run migrations on all cluster nodes

### Debugging Role Issues

```sql
-- Check role existence
SELECT name FROM system.roles WHERE name = 'your_role_name';

-- Check role permissions
SELECT * FROM system.grants WHERE grantee_name = 'your_role_name';

-- Check user role assignments  
SELECT * FROM system.role_grants WHERE granted_role_name = 'your_role_name';

-- Check effective permissions for a user
SHOW GRANTS FOR your_username;
```

## Migration Examples

See the [Role Examples](../examples/role-management.md) page for complete examples of role-based schema migrations and real-world usage patterns.