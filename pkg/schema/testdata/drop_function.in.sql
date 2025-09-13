-- Current state: functions exist
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE FUNCTION safe_divide AS (a, b) -> if(equals(b, 0), 0, divide(a, b));
CREATE FUNCTION current_timestamp_utc AS () -> now();
CREATE FUNCTION old_function AS (value) -> add(value, 1);
-- Target state: drop some functions
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE FUNCTION current_timestamp_utc AS () -> now();