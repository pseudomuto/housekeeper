-- Current state: function with old name
CREATE FUNCTION old_multiply AS (x) -> multiply(x, 2);
CREATE FUNCTION calculate_percentage AS (part, total) -> if(equals(total, 0), 0, divide(multiply(part, 100.0), total));
-- Target state: function with new name (same expression should trigger rename detection)
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE FUNCTION calc_percentage AS (part, total) -> if(equals(total, 0), 0, divide(multiply(part, 100.0), total));