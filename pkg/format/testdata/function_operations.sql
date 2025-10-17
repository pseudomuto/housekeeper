-- Function operations with various formatting scenarios
-- Simple function with few arguments (should stay single-line)

CREATE FUNCTION `simple_func` AS (`x`) -> multiply(`x`, 2);

-- Function with no parameters

CREATE FUNCTION `current_time` AS () -> now();

-- Function with moderate arguments (should stay single-line under threshold)

CREATE FUNCTION `safe_divide` AS (`a`, `b`) -> if(equals(`b`, 0), 0, divide(`a`, `b`));

-- Complex multiIf function with many arguments (should trigger multi-line formatting)

CREATE FUNCTION `normalizedBrowser` AS (`br`) -> multiIf(
    lower(`br`) = 'firefox', 'Firefox',
    lower(`br`) = 'edge', 'Edge',
    lower(`br`) = 'safari', 'Safari',
    lower(`br`) = 'chrome', 'Chrome',
    lower(`br`) = 'webview', 'Webview',
    'Other'
);

-- Another complex function with ON CLUSTER

CREATE FUNCTION `normalizedOS` ON CLUSTER `warehouse` AS (`os`) -> multiIf(
    startsWith(lower(`os`), 'windows'), 'Windows',
    startsWith(lower(`os`), 'mac'), 'Mac',
    lower(`os`) IN ('ios', 'iphone'), 'iOS',
    lower(`os`) = 'android', 'Android',
    'Other'
);

-- Function with backticked names and special characters

CREATE FUNCTION `special-name` AS (`param-1`, `param-2`) -> if(`param-1` > 0, `param-1`, `param-2`);

-- Function with ON CLUSTER and backticks

CREATE FUNCTION `cluster-func` ON CLUSTER `production-cluster` AS (`input-value`) -> plus(`input-value`, 1);

-- Complex case expression (should trigger multi-line)

CREATE FUNCTION `categorizeValue` AS (`val`) -> multiIf(
    `val` < 0, 'negative',
    `val` = 0, 'zero',
    `val` < 100, 'small',
    `val` < 1000, 'medium',
    'large'
);

-- Function with nested function calls

CREATE FUNCTION `complexCalc` AS (`a`, `b`, `c`, `d`, `e`) -> plus(multiply(`a`, `b`), divide(subtract(`c`, `d`), `e`));

-- DROP FUNCTION operations
-- Basic DROP FUNCTION

DROP FUNCTION `simple_func`;

-- DROP FUNCTION with IF EXISTS

DROP FUNCTION IF EXISTS `old_function`;

-- DROP FUNCTION with ON CLUSTER

DROP FUNCTION `normalizedBrowser` ON CLUSTER `warehouse`;

-- DROP FUNCTION with backticked names

DROP FUNCTION IF EXISTS `special-name`;

-- DROP FUNCTION with ON CLUSTER and backticks

DROP FUNCTION `cluster-func` ON CLUSTER `production-cluster`;
