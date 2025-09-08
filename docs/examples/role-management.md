# Role Management Examples

This page provides practical examples of using Housekeeper for ClickHouse role management in real-world scenarios.

## Example 1: E-commerce Platform Roles

### Project Structure

```
db/schemas/_global/roles/
├── main.sql
├── customer_service_roles.sql
├── analytics_roles.sql
├── engineering_roles.sql
└── admin_roles.sql
```

### Role Definitions

**db/schemas/_global/roles/main.sql**
```sql
-- E-commerce platform role management
-- Import roles by department for better organization

-- housekeeper:import customer_service_roles.sql
-- housekeeper:import analytics_roles.sql  
-- housekeeper:import engineering_roles.sql
-- housekeeper:import admin_roles.sql

-- Service account roles defined directly
CREATE ROLE IF NOT EXISTS api_gateway
SETTINGS max_concurrent_queries_for_user = 500, max_memory_usage = 2000000000;

CREATE ROLE IF NOT EXISTS payment_processor  
SETTINGS max_concurrent_queries_for_user = 100, max_execution_time = 10;

CREATE ROLE IF NOT EXISTS recommendation_engine
SETTINGS max_memory_usage = 8000000000, max_execution_time = 120;
```

**db/schemas/_global/roles/customer_service_roles.sql**
```sql
-- Customer service team roles
CREATE ROLE IF NOT EXISTS cs_viewer;
CREATE ROLE IF NOT EXISTS cs_agent;  
CREATE ROLE IF NOT EXISTS cs_supervisor;
CREATE ROLE IF NOT EXISTS cs_manager;

-- Base permissions for all customer service staff
GRANT SELECT ON customers.profiles, customers.orders TO cs_viewer;
GRANT SELECT ON products.catalog TO cs_viewer;
GRANT SHOW TABLES ON customers.*, products.* TO cs_viewer;

-- Agent permissions (viewer + limited updates)
GRANT cs_viewer TO cs_agent;
GRANT INSERT ON customers.support_tickets TO cs_agent;
GRANT UPDATE ON customers.support_tickets TO cs_agent;
GRANT SELECT ON customers.payment_methods TO cs_agent;

-- Supervisor permissions (agent + order management)  
GRANT cs_agent TO cs_supervisor;
GRANT UPDATE ON customers.orders TO cs_supervisor;
GRANT INSERT ON customers.refunds TO cs_supervisor;
GRANT SELECT ON analytics.customer_insights TO cs_supervisor;

-- Manager permissions (supervisor + reporting)
GRANT cs_supervisor TO cs_manager;
GRANT SELECT ON analytics.cs_performance TO cs_manager;
GRANT CREATE VIEW ON reports.customer_service TO cs_manager;
```

**db/schemas/_global/roles/analytics_roles.sql**  
```sql
-- Analytics and data science roles
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS senior_analyst;
CREATE ROLE IF NOT EXISTS data_scientist;
CREATE ROLE IF NOT EXISTS analytics_engineer;

-- Basic analyst permissions
GRANT SELECT ON analytics.* TO data_analyst;
GRANT SELECT ON customers.aggregated_* TO data_analyst;
GRANT SELECT ON products.performance_metrics TO data_analyst;
GRANT SHOW TABLES ON analytics.*, customers.*, products.* TO data_analyst;

-- Senior analyst permissions (analyst + detailed customer data)
GRANT data_analyst TO senior_analyst;  
GRANT SELECT ON customers.detailed_profiles TO senior_analyst;
GRANT SELECT ON customers.purchase_history TO senior_analyst;
GRANT CREATE VIEW ON analytics.custom_reports TO senior_analyst;

-- Data scientist permissions (senior analyst + experimental access)
GRANT senior_analyst TO data_scientist;
GRANT SELECT ON raw_data.* TO data_scientist;
GRANT CREATE TABLE ON experiments.* TO data_scientist;
GRANT INSERT, UPDATE, DELETE ON experiments.* TO data_scientist;
GRANT SELECT ON ml_features.* TO data_scientist;

-- Analytics engineer permissions (data scientist + ETL)
GRANT data_scientist TO analytics_engineer;
GRANT INSERT, UPDATE ON analytics.* TO analytics_engineer;
GRANT CREATE TABLE, DROP TABLE ON analytics.staging TO analytics_engineer;
GRANT ALL ON etl_workspace.* TO analytics_engineer;
```

### Service Account Permissions

```sql
-- API Gateway role - handles customer-facing queries
GRANT SELECT ON customers.public_profiles TO api_gateway;
GRANT SELECT ON products.catalog, products.pricing TO api_gateway;
GRANT INSERT ON customers.activity_log TO api_gateway;
GRANT UPDATE ON customers.last_activity TO api_gateway;

-- Payment processor - secure financial operations  
GRANT SELECT ON customers.payment_methods TO payment_processor;
GRANT INSERT ON transactions.payments TO payment_processor;
GRANT UPDATE ON customers.orders TO payment_processor;
GRANT INSERT ON audit.payment_events TO payment_processor;

-- Recommendation engine - ML model serving
GRANT SELECT ON customers.preferences, customers.purchase_history TO recommendation_engine;
GRANT SELECT ON products.features, products.similarity_matrix TO recommendation_engine;
GRANT INSERT ON analytics.recommendation_events TO recommendation_engine;
```

## Example 2: SaaS Multi-Tenant Platform

### Tenant-Isolated Roles

```sql
-- db/schemas/_global/roles/tenant_roles.sql

-- Template for tenant-specific roles
-- These would be created dynamically per tenant

-- Tenant admin role template
CREATE ROLE IF NOT EXISTS tenant_admin_template;
-- Note: Actual tenant roles would be like 'tenant_123_admin'

-- Tenant user role template  
CREATE ROLE IF NOT EXISTS tenant_user_template;
-- Note: Actual tenant roles would be like 'tenant_123_user'

-- Platform-level roles
CREATE ROLE IF NOT EXISTS platform_admin;
CREATE ROLE IF NOT EXISTS platform_support;
CREATE ROLE IF NOT EXISTS platform_billing;
CREATE ROLE IF NOT EXISTS platform_analytics;

-- Platform admin has access to all tenant data
GRANT ALL ON *.* TO platform_admin WITH GRANT OPTION;

-- Platform support has read access to all tenant data
GRANT SELECT ON tenant_data.* TO platform_support;
GRANT SELECT ON platform_logs.* TO platform_support;
GRANT INSERT ON support.tickets TO platform_support;

-- Platform billing has access to usage and billing data
GRANT SELECT ON tenant_data.usage_metrics TO platform_billing;
GRANT SELECT, INSERT, UPDATE ON billing.* TO platform_billing;
GRANT SELECT ON tenant_data.subscription_info TO platform_billing;

-- Platform analytics has aggregated access across tenants
GRANT SELECT ON analytics.tenant_aggregates TO platform_analytics;
GRANT SELECT ON analytics.platform_metrics TO platform_analytics;
GRANT CREATE VIEW ON analytics.reports TO platform_analytics;
```

### Dynamic Tenant Role Creation

```sql
-- Example of tenant-specific roles (these would be generated dynamically)
-- For tenant ID 123:

CREATE ROLE IF NOT EXISTS tenant_123_admin;
CREATE ROLE IF NOT EXISTS tenant_123_user;
CREATE ROLE IF NOT EXISTS tenant_123_readonly;

-- Tenant admin has full control over their data
GRANT ALL ON tenant_data.tenant_123_* TO tenant_123_admin;
GRANT CREATE TABLE, DROP TABLE ON tenant_data.* TO tenant_123_admin WHERE table LIKE 'tenant_123_%';

-- Tenant user has read/write on their data
GRANT SELECT, INSERT, UPDATE ON tenant_data.tenant_123_* TO tenant_123_user;

-- Tenant readonly has only read access
GRANT SELECT ON tenant_data.tenant_123_* TO tenant_123_readonly;
```

## Example 3: Financial Services Compliance

### Compliance-First Role Design

```sql
-- db/schemas/_global/roles/compliance_roles.sql

-- Segregation of duties - no single person has complete access
CREATE ROLE IF NOT EXISTS trade_input_clerk;
CREATE ROLE IF NOT EXISTS trade_validator; 
CREATE ROLE IF NOT EXISTS risk_analyst;
CREATE ROLE IF NOT EXISTS compliance_officer;
CREATE ROLE IF NOT EXISTS auditor;

-- Trade input - can create but not modify trades
CREATE ROLE IF NOT EXISTS trade_input_clerk;
GRANT INSERT ON trades.pending_trades TO trade_input_clerk;
GRANT SELECT ON reference_data.instruments, reference_data.counterparties TO trade_input_clerk;

-- Trade validation - can validate but not create trades
GRANT SELECT ON trades.pending_trades TO trade_validator;
GRANT UPDATE ON trades.pending_trades TO trade_validator; -- Only status updates
GRANT INSERT ON trades.validated_trades TO trade_validator;

-- Risk analyst - read access to trades, write to risk assessments
GRANT SELECT ON trades.validated_trades TO risk_analyst;
GRANT SELECT ON market_data.* TO risk_analyst;
GRANT INSERT, UPDATE ON risk.assessments TO risk_analyst;

-- Compliance officer - broad read access, compliance updates
GRANT SELECT ON trades.*, risk.* TO compliance_officer;
GRANT INSERT, UPDATE ON compliance.violations TO compliance_officer;
GRANT INSERT ON compliance.audit_trail TO compliance_officer;

-- Auditor - read-only access to everything, audit trail writes
GRANT SELECT ON *.* TO auditor;
GRANT INSERT ON audit.access_log TO auditor;
GRANT INSERT ON audit.data_lineage TO auditor;
```

### Audit Trail Integration

```sql
-- Audit roles with automatic logging
CREATE ROLE IF NOT EXISTS logged_trader
SETTINGS log_queries = 1, log_query_settings = 1;

GRANT SELECT ON trades.*, market_data.current_prices TO logged_trader;
GRANT INSERT ON trades.orders TO logged_trader;

-- Automatic audit trail for sensitive operations
CREATE ROLE IF NOT EXISTS sensitive_data_access  
SETTINGS log_queries = 1, log_query_settings = 1, log_query_views = 1;

GRANT SELECT ON customers.pii, customers.financial_data TO sensitive_data_access;
```

## Example 4: Data Pipeline Roles

### ETL Pipeline Roles

```sql
-- db/schemas/_global/roles/pipeline_roles.sql

-- Raw data ingestion
CREATE ROLE IF NOT EXISTS data_ingester
SETTINGS max_memory_usage = 10000000000, max_execution_time = 3600;
GRANT INSERT ON raw_data.* TO data_ingester;
GRANT CREATE TABLE ON raw_data.* TO data_ingester;
GRANT DROP TABLE ON raw_data.staging_* TO data_ingester; -- Cleanup staging tables

-- Data transformation  
CREATE ROLE IF NOT EXISTS data_transformer
SETTINGS max_memory_usage = 20000000000, max_execution_time = 7200;
GRANT SELECT ON raw_data.* TO data_transformer;
GRANT INSERT, UPDATE ON processed_data.* TO data_transformer;
GRANT CREATE TABLE, DROP TABLE ON processed_data.staging_* TO data_transformer;

-- Data quality checker
CREATE ROLE IF NOT EXISTS data_quality_checker;
GRANT SELECT ON raw_data.*, processed_data.* TO data_quality_checker;
GRANT INSERT ON data_quality.validation_results TO data_quality_checker;
GRANT UPDATE ON data_quality.table_status TO data_quality_checker;

-- Production data publisher
CREATE ROLE IF NOT EXISTS data_publisher;
GRANT SELECT ON processed_data.validated_* TO data_publisher;
GRANT INSERT ON analytics.*, dashboard_data.* TO data_publisher;
GRANT CREATE VIEW ON analytics.*, dashboard_data.* TO data_publisher;
```

### Scheduled Job Roles

```sql
-- Daily ETL job  
CREATE ROLE IF NOT EXISTS daily_etl_job
SETTINGS max_memory_usage = 50000000000, max_execution_time = 14400; -- 4 hours
GRANT data_ingester, data_transformer, data_quality_checker TO daily_etl_job;

-- Hourly aggregation job
CREATE ROLE IF NOT EXISTS hourly_aggregator  
SETTINGS max_memory_usage = 5000000000, max_execution_time = 600; -- 10 minutes
GRANT SELECT ON analytics.events TO hourly_aggregator;
GRANT INSERT, UPDATE ON analytics.hourly_metrics TO hourly_aggregator;

-- Cleanup job
CREATE ROLE IF NOT EXISTS cleanup_job;
GRANT SELECT ON system.parts TO cleanup_job; -- Check partition info
GRANT DROP PARTITION ON raw_data.* TO cleanup_job;
GRANT DROP PARTITION ON processed_data.staging_* TO cleanup_job;
```

## Example 5: Development Workflow Roles

### Environment-Based Access

```sql
-- db/schemas/_global/roles/environment_roles.sql

-- Development environment - permissive
CREATE ROLE IF NOT EXISTS dev_developer;
GRANT ALL ON dev_*.* TO dev_developer;
GRANT CREATE DATABASE ON *.* TO dev_developer WHERE database LIKE 'dev_%';

-- Staging environment - production-like restrictions
CREATE ROLE IF NOT EXISTS staging_developer;  
GRANT SELECT ON staging_*.* TO staging_developer;
GRANT INSERT, UPDATE ON staging_*.test_data TO staging_developer;
GRANT CREATE VIEW ON staging_*.* TO staging_developer;

-- Production environment - read-only for developers
CREATE ROLE IF NOT EXISTS prod_developer;
GRANT SELECT ON prod_analytics.aggregated_* TO prod_developer; -- Only aggregated data
GRANT SELECT ON prod_logs.application_* TO prod_developer; -- Application logs only
GRANT SHOW TABLES ON prod_*.* TO prod_developer;
```

### CI/CD Pipeline Roles

```sql
-- Automated testing role
CREATE ROLE IF NOT EXISTS ci_test_runner;
GRANT ALL ON test_*.* TO ci_test_runner;
GRANT CREATE DATABASE ON *.* TO ci_test_runner WHERE database LIKE 'test_%';
GRANT DROP DATABASE ON *.* TO ci_test_runner WHERE database LIKE 'test_%';

-- Migration deployment role
CREATE ROLE IF NOT EXISTS migration_deployer;
GRANT CREATE TABLE, ALTER TABLE, DROP TABLE ON *.* TO migration_deployer;
GRANT CREATE VIEW, DROP VIEW ON *.* TO migration_deployer; 
GRANT CREATE DICTIONARY, DROP DICTIONARY ON *.* TO migration_deployer;
GRANT CREATE ROLE, ALTER ROLE, DROP ROLE ON *.* TO migration_deployer;

-- Schema validation role  
CREATE ROLE IF NOT EXISTS schema_validator;
GRANT SELECT ON system.tables, system.columns TO schema_validator;
GRANT SELECT ON system.dictionaries, system.views TO schema_validator;
GRANT SHOW TABLES ON *.* TO schema_validator;
GRANT DESCRIBE TABLE ON *.* TO schema_validator;
```

## Migration Workflow Examples

### Initial Role Setup

```sql
-- Step 1: Create base roles
CREATE ROLE IF NOT EXISTS base_user;
CREATE ROLE IF NOT EXISTS base_service;

-- Step 2: Create department roles
CREATE ROLE IF NOT EXISTS engineering_team;
CREATE ROLE IF NOT EXISTS analytics_team;
CREATE ROLE IF NOT EXISTS business_team;

-- Step 3: Create application roles
CREATE ROLE IF NOT EXISTS web_app;
CREATE ROLE IF NOT EXISTS mobile_app;
CREATE ROLE IF NOT EXISTS data_pipeline;

-- Step 4: Assign base permissions
GRANT SHOW TABLES ON *.* TO base_user;
GRANT SELECT ON system.settings TO base_service;
```

### Role Evolution Migration

```sql
-- Migration: Split analytics_user into specialized roles
-- Old role (to be deprecated):
-- CREATE ROLE analytics_user;
-- GRANT SELECT ON analytics.* TO analytics_user;

-- New specialized roles:
CREATE ROLE IF NOT EXISTS data_analyst;
CREATE ROLE IF NOT EXISTS data_scientist;  
CREATE ROLE IF NOT EXISTS analytics_engineer;

-- Migrate permissions from old role to new roles
GRANT SELECT ON analytics.reports, analytics.dashboards TO data_analyst;
GRANT SELECT ON analytics.*, raw_data.* TO data_scientist;
GRANT SELECT, INSERT, UPDATE ON analytics.* TO analytics_engineer;
GRANT CREATE TABLE ON analytics.experiments TO data_scientist;

-- Grant old role to new roles for backward compatibility (temporary)
GRANT data_analyst TO analytics_user; -- Will be removed in next migration
```

### Role Consolidation Migration  

```sql
-- Consolidate multiple service roles into organized hierarchy
-- Before: app_service_read, app_service_write, app_service_admin
-- After: app_service with inherited roles

CREATE ROLE IF NOT EXISTS app_service_base;
CREATE ROLE IF NOT EXISTS app_service_writer;  
CREATE ROLE IF NOT EXISTS app_service_admin;

-- Base permissions
GRANT SELECT ON app.public_data TO app_service_base;
GRANT INSERT ON app.audit_log TO app_service_base;

-- Writer inherits base + write permissions
GRANT app_service_base TO app_service_writer;
GRANT INSERT, UPDATE ON app.user_data TO app_service_writer;

-- Admin inherits writer + admin permissions  
GRANT app_service_writer TO app_service_admin;
GRANT CREATE TABLE, DROP TABLE ON app.* TO app_service_admin;

-- Migrate existing services to new hierarchy
-- (This would be done through configuration management)
```

## Testing Role Changes

### Role Permission Testing

```sql
-- Test role permissions before applying to production
-- Create test user with role
CREATE USER test_analyst;
GRANT data_analyst TO test_analyst;

-- Test queries as that user (run with different connection)
-- SELECT * FROM analytics.reports; -- Should work
-- INSERT INTO analytics.reports VALUES (...); -- Should fail
-- SELECT * FROM raw_data.sensitive; -- Should fail

-- Cleanup test user
DROP USER test_analyst;
```

### Migration Validation Scripts

```bash
#!/bin/bash
# validate_roles.sh - Validate role migrations

# Check that all expected roles exist
expected_roles=("data_analyst" "data_scientist" "analytics_engineer")
for role in "${expected_roles[@]}"; do
  if ! clickhouse-client -q "SELECT name FROM system.roles WHERE name = '$role'" | grep -q "$role"; then
    echo "ERROR: Role $role not found"
    exit 1
  fi
done

# Check that roles have expected permissions
clickhouse-client -q "
SELECT grantee_name, privilege, database, table 
FROM system.grants 
WHERE grantee_name IN ('data_analyst', 'data_scientist', 'analytics_engineer')
ORDER BY grantee_name, database, table
" > role_permissions.txt

echo "Role validation completed successfully"
```

These examples demonstrate how to structure and manage roles in real-world scenarios using Housekeeper's role management capabilities. The key principles are:

1. **Organization**: Use directory structure and imports for maintainable role definitions
2. **Security**: Follow principle of least privilege and segregation of duties  
3. **Scalability**: Design role hierarchies that can grow with your organization
4. **Compliance**: Implement audit trails and access controls as needed
5. **Testing**: Validate role changes thoroughly before production deployment