-- Current state: mixed scenario with functions and other objects
CREATE DATABASE analytics ENGINE = Atomic;
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE TABLE analytics.events (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
CREATE FUNCTION old_function AS (value) -> add(value, 1);
-- Target state: mixed scenario with function changes
CREATE DATABASE analytics ENGINE = Atomic;
CREATE FUNCTION multiply_by_two AS (x) -> multiply(x, 2);
CREATE TABLE analytics.events (id UInt64, name String, timestamp DateTime) ENGINE = MergeTree() ORDER BY id;
CREATE FUNCTION new_function AS (value) -> multiply(value, 2);