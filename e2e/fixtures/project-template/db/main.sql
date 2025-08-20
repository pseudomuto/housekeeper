-- Main schema file for E2E testing
-- This will be used as the baseline schema

-- Analytics database will be created via migrations
CREATE DATABASE IF NOT EXISTS analytics ENGINE = Atomic COMMENT 'E2E test analytics database';