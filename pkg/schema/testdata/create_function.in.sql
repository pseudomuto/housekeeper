-- Current state: no functions exist
;
-- Target state: create new functions
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE FUNCTION safe_divide AS (a, b) -> if(equals(b, 0), 0, divide(a, b));
CREATE FUNCTION current_timestamp_utc AS () -> now();
CREATE FUNCTION truncate_string ON CLUSTER production AS (str, max_len) -> if(greater(length(str), max_len), concat(substring(str, 1, minus(max_len, 3)), '...'), str);