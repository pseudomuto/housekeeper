-- First create a source table for the dictionary
CREATE TABLE IF NOT EXISTS analytics.countries (
    id UInt64,
    country_code String,
    country_name String,
    population UInt64
) ENGINE = MergeTree()
ORDER BY id;

-- Insert test data
INSERT INTO analytics.countries VALUES
(1, 'US', 'United States', 331000000),
(2, 'CA', 'Canada', 38000000),
(3, 'GB', 'United Kingdom', 67000000);

-- Complex dictionary with local table source
CREATE DICTIONARY analytics.geo_data (
    id UInt64,
    country_code String,
    country_name String,
    population UInt64
) PRIMARY KEY id
SOURCE(
    CLICKHOUSE(
        db 'analytics'
        table 'countries'
    )
)
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 3600)
COMMENT 'Geographic reference data';