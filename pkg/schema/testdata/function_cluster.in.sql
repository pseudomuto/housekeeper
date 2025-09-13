-- Current state: function without cluster
CREATE FUNCTION local_function AS (x) -> multiply(x, 2);
-- Target state: function with cluster (should trigger error - cluster changes not supported)
CREATE FUNCTION local_function ON CLUSTER production AS (x) -> multiply(x, 2);