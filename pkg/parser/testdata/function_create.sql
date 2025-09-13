-- Basic function creation
CREATE FUNCTION linear_equation AS (x, k, b) -> plus(multiply(k, x), b);

-- Function with single parameter
CREATE FUNCTION parity_str AS (n) -> if(modulo(n, 2), 'odd', 'even');

-- Function with multiple parameters
CREATE FUNCTION safe_divide AS (a, b) -> if(equals(b, 0), 0, divide(a, b));

-- Function with ON CLUSTER
CREATE FUNCTION truncate_string ON CLUSTER production AS (str, max_len) -> if(greater(length(str), max_len), concat(substring(str, 1, minus(max_len, 3)), '...'), str);

-- Complex function with multiple conditions
CREATE FUNCTION is_valid_date_range ON CLUSTER staging AS (start_date, end_date) -> and(and(lessOrEquals(start_date, end_date), greaterOrEquals(start_date, '1900-01-01')), lessOrEquals(end_date, '2100-12-31'));

-- Function with no parameters
CREATE FUNCTION current_timestamp_utc AS () -> now();

-- Function with backticked name
CREATE FUNCTION `my-special-function` AS (value) -> multiply(value, 2);

-- Function with backticked cluster name
CREATE FUNCTION calc_percentage ON CLUSTER `prod-cluster` AS (part, total) -> if(equals(total, 0), 0, divide(multiply(part, 100.0), total));