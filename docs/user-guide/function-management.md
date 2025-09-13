# Function Management

User-Defined Functions (UDFs) in ClickHouse allow you to create reusable computational logic using lambda expressions. Housekeeper provides comprehensive support for managing functions across your ClickHouse deployment.

## Overview

ClickHouse User-Defined Functions are:
- **Global objects** that exist at the cluster level
- **Lambda expressions** using the `->` syntax
- **Automatically replicated** across cluster nodes
- **Applied early** in the migration dependency order (after roles, before databases)

## Function Syntax

```sql
CREATE FUNCTION function_name AS (param1, param2, ...) -> expression;
```

### Examples

```sql
-- Mathematical utilities
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
CREATE FUNCTION safe_divide AS (a, b) -> if(b = 0, 0, a / b);

-- String processing
CREATE FUNCTION truncate_string AS (str, max_len) -> 
    if(length(str) > max_len, concat(substring(str, 1, max_len - 3), '...'), str);

-- Business logic
CREATE FUNCTION calculate_discount AS (price, discount_rate) -> 
    price * (1 - discount_rate / 100);

-- Date validation
CREATE FUNCTION is_valid_date_range AS (start_date, end_date) -> 
    start_date <= end_date AND start_date >= '1900-01-01' AND end_date <= '2100-12-31';
```

## Project Structure

Functions are organized in the `schemas/_global/functions/` directory:

```
db/
├── schemas/
│   └── _global/
│       ├── roles/
│       │   └── main.sql
│       └── functions/           # ← Function definitions
│           ├── main.sql         # Primary function definitions
│           ├── math.sql         # Mathematical utilities
│           ├── strings.sql      # String processing
│           └── business.sql     # Business logic functions
└── main.sql                     # Imports global schemas
```

### Main Functions File

The `schemas/_global/functions/main.sql` serves as the entrypoint:

```sql
-- Import organized function categories
-- housekeeper:import math.sql
-- housekeeper:import strings.sql  
-- housekeeper:import business.sql

-- Or define functions directly here for simple cases
CREATE FUNCTION version_info AS () -> '1.0.0';
```

### Organized Function Files

**math.sql:**
```sql
-- Mathematical utility functions
CREATE FUNCTION linear_equation AS (x, k, b) -> k*x + b;
CREATE FUNCTION safe_divide AS (a, b) -> if(b = 0, 0, a / b);
CREATE FUNCTION clamp AS (value, min_val, max_val) -> 
    greatest(min_val, least(max_val, value));
```

**strings.sql:**
```sql  
-- String processing functions
CREATE FUNCTION truncate_string AS (str, max_len) -> 
    if(length(str) > max_len, concat(substring(str, 1, max_len - 3), '...'), str);

CREATE FUNCTION format_phone AS (phone) ->
    concat('(', substring(phone, 1, 3), ') ', 
           substring(phone, 4, 3), '-', 
           substring(phone, 7, 4));
```

**business.sql:**
```sql
-- Business logic functions  
CREATE FUNCTION calculate_tax AS (amount, rate) -> amount * rate;
CREATE FUNCTION apply_discount AS (price, discount_pct) -> 
    price * (1 - discount_pct / 100);
```

## Migration Workflow

### 1. Development Process

```bash
# 1. Add function to appropriate file
vim db/schemas/_global/functions/math.sql

# 2. Generate migration
housekeeper diff

# 3. Review generated migration
cat db/migrations/202401151430_add_math_functions.sql

# 4. Apply migration
housekeeper migrate
```

### 2. Migration Ordering

Functions are applied in the correct dependency order:

```
1. Roles           (user management)
2. Functions       (reusable logic)      ← Functions applied here
3. Databases       (schema containers)  
4. Collections     (named collections)
5. Tables          (data structures)
6. Dictionaries    (external data)
7. Views           (computed results)
```

This ensures functions are available when referenced by views, materialized views, or table defaults.

### 3. Generated Migrations

Housekeeper automatically handles function operations:

```sql
-- Adding a new function
CREATE FUNCTION calculate_tax AS (amount, rate) -> amount * rate;

-- Modifying a function (uses DROP+CREATE)
DROP FUNCTION IF EXISTS calculate_tax;
CREATE FUNCTION calculate_tax AS (amount, tax_rate) -> 
    amount * (tax_rate / 100);

-- Removing a function
DROP FUNCTION IF EXISTS old_function;
```

## Usage in Tables and Views

Once defined, functions can be used throughout your schema:

### Table Defaults

```sql
CREATE TABLE orders (
    id UUID DEFAULT generateUUIDv4(),
    amount Decimal(10,2),
    tax_amount Decimal(10,2) DEFAULT calculate_tax(amount, 0.08),
    discounted_price Decimal(10,2) DEFAULT apply_discount(amount, 5.0)
) ENGINE = MergeTree()
ORDER BY id;
```

### Views and Materialized Views

```sql
CREATE VIEW sales_summary AS
SELECT 
    date,
    sum(amount) as total_sales,
    sum(calculate_tax(amount, 0.08)) as total_tax,
    truncate_string(customer_name, 20) as display_name
FROM orders
GROUP BY date, customer_name;
```

### Query Usage

```sql
-- Use functions in ad-hoc queries
SELECT 
    customer_name,
    apply_discount(order_total, customer_discount_pct) as final_amount,
    safe_divide(profit, revenue) as profit_margin
FROM customer_orders;
```

## Cluster Support

### Automatic ON CLUSTER Injection

When using clusters, Housekeeper automatically adds `ON CLUSTER` clauses:

**Your Schema:**
```sql
CREATE FUNCTION calculate_tax AS (amount, rate) -> amount * rate;
```

**Generated Migration (with cluster):**
```sql
CREATE FUNCTION `calculate_tax` ON CLUSTER `production` AS (`amount`, `rate`) -> amount * rate;
```

Configure cluster in `housekeeper.yaml`:
```yaml
clickhouse:
  cluster: "production"
```

## Function Limitations

### ClickHouse Constraints

- **No ALTER FUNCTION**: All changes use DROP+CREATE strategy
- **No recursion**: Functions cannot call themselves
- **Parameter scope**: All variables must be in parameter list
- **Lambda expressions only**: No procedural logic or loops

### Housekeeper Behavior

- **DROP+CREATE strategy**: Function modifications recreate the function
- **Dependency ordering**: Functions created before they're used
- **Name uniqueness**: Function names must be unique across the cluster
- **No RENAME support**: Renames use DROP+CREATE pattern

## Testing Functions

### Unit Testing in Migrations

```sql
-- Test function in migration (development/staging)
CREATE FUNCTION calculate_tax AS (amount, rate) -> amount * rate;

-- Verify behavior
SELECT calculate_tax(100.0, 0.08) as result; -- Should return 8.0

-- Insert test case
INSERT INTO function_tests VALUES 
    ('calculate_tax', 'calculate_tax(100.0, 0.08)', 8.0);
```

### Integration Testing

```sql
-- Test function with real data
SELECT 
    order_id,
    amount,
    calculate_tax(amount, 0.08) as expected_tax,
    tax_amount as actual_tax,
    abs(expected_tax - actual_tax) as difference
FROM orders 
WHERE difference > 0.01; -- Should return no rows
```

## Best Practices

### 1. Organization

- **Categorize functions** by domain (math, strings, business)
- **Use imports** to organize complex function libraries
- **Document function purpose** and expected parameters
- **Keep functions simple** and focused on single responsibility

### 2. Naming Conventions

```sql
-- Good: descriptive, clear purpose
CREATE FUNCTION calculate_sales_tax AS (amount, rate) -> amount * rate;
CREATE FUNCTION format_currency_usd AS (amount) -> concat('$', toString(round(amount, 2)));

-- Avoid: ambiguous or overly generic
CREATE FUNCTION calc AS (a, b) -> a * b;  -- What does this calculate?
CREATE FUNCTION process AS (x) -> x + 1;  -- Process what?
```

### 3. Testing Strategy

```sql
-- Include validation in function definitions
CREATE FUNCTION safe_divide AS (numerator, denominator) -> 
    if(denominator = 0, NULL, numerator / denominator);

-- Use assertions for critical functions  
CREATE FUNCTION calculate_compound_interest AS (principal, rate, time) ->
    if(principal <= 0 OR rate < 0 OR time <= 0, 
       NULL, 
       principal * pow(1 + rate, time));
```

### 4. Migration Safety

- **Test functions** in development before production
- **Use meaningful names** to avoid conflicts
- **Document breaking changes** in migration descriptions
- **Consider backwards compatibility** when modifying existing functions

### 5. Performance Considerations

```sql
-- Efficient: minimal computation
CREATE FUNCTION is_weekend AS (date_val) -> 
    toDayOfWeek(date_val) IN (6, 7);

-- Less efficient: complex nested operations
CREATE FUNCTION complex_calc AS (x) ->
    if(x > 0, 
       sqrt(log(abs(x) + 1)) * sin(x / 100), 
       exp(x * -1) / (x * x + 1));
```

Functions are evaluated for every row, so keep them lightweight and avoid expensive operations when possible.

## Migration Examples

### Adding Functions

```sql
-- 001_add_utility_functions.sql
CREATE FUNCTION safe_divide AS (a, b) -> if(b = 0, 0, a / b);
CREATE FUNCTION percentage AS (part, total) -> safe_divide(part * 100, total);
```

### Modifying Functions

```sql  
-- 002_update_tax_calculation.sql
-- Update function to handle different tax brackets
DROP FUNCTION IF EXISTS calculate_tax;
CREATE FUNCTION calculate_tax AS (income, bracket) ->
    multiIf(
        bracket = 'low', income * 0.10,
        bracket = 'medium', income * 0.15, 
        bracket = 'high', income * 0.25,
        income * 0.10  -- default
    );
```

### Function Dependencies

```sql
-- 003_advanced_calculations.sql  
-- Functions can use other functions
CREATE FUNCTION gross_to_net AS (gross_pay, tax_bracket) ->
    gross_pay - calculate_tax(gross_pay, tax_bracket);

CREATE FUNCTION annual_salary AS (monthly_pay, tax_bracket) ->
    gross_to_net(monthly_pay * 12, tax_bracket);
```

User-Defined Functions provide powerful reusable logic for your ClickHouse deployment. With Housekeeper's comprehensive function management, you can organize, version, and deploy functions safely across your entire cluster.