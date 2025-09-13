-- Current state: function with old expression
CREATE FUNCTION calculate_tax AS (amount) -> multiply(amount, 0.08);
CREATE FUNCTION format_currency AS (value) -> concat('$', toString(value));
-- Target state: same function names but different expressions (should trigger DROP+CREATE)
CREATE FUNCTION calculate_tax AS (amount) -> multiply(amount, 0.10);
CREATE FUNCTION format_currency AS (value, currency) -> concat(currency, ' ', toString(value));