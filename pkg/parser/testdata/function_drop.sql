-- Basic function drop
DROP FUNCTION linear_equation;

-- Drop function with IF EXISTS
DROP FUNCTION IF EXISTS parity_str;

-- Drop function with ON CLUSTER
DROP FUNCTION IF EXISTS safe_divide ON CLUSTER production;

-- Drop function with backticked name
DROP FUNCTION IF EXISTS `my-special-function`;

-- Drop function with backticked cluster
DROP FUNCTION truncate_string ON CLUSTER `prod-cluster`;

-- Multiple function drops
DROP FUNCTION is_valid_date_range;
DROP FUNCTION current_timestamp_utc;
DROP FUNCTION calc_percentage ON CLUSTER staging;