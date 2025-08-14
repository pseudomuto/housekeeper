CREATE TABLE `ecommerce`.`categories_source` ON CLUSTER `demo` (
    `id`        UInt64,
    `name`      String,
    `parent_id` UInt64,
    `level`     UInt8,
    `is_active` Bool
)
ENGINE = Memory()
COMMENT 'Source data for categories dictionary';

CREATE TABLE `ecommerce`.`user_segments_source` ON CLUSTER `demo` (
    `user_id`      UInt64,
    `segment`      String,
    `score`        Float32,
    `last_updated` DateTime
)
ENGINE = Memory()
COMMENT 'Source data for user segments dictionary';

CREATE DICTIONARY `ecommerce`.`categories_dict` ON CLUSTER `demo` (
    `id`        UInt64,
    `name`      String,
    `parent_id` UInt64 DEFAULT 0 HIERARCHICAL,
    `level`     UInt8 DEFAULT 0,
    `is_active` Bool DEFAULT true
)
PRIMARY KEY `id`
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'ecommerce' table 'categories_source'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 3600)
COMMENT 'Product category hierarchy with parent relationships';

CREATE DICTIONARY `ecommerce`.`countries_dict` ON CLUSTER `demo` (
    `cca2`     String,
    `region`   String,
    `unMember` UInt8
)
PRIMARY KEY `cca2`
SOURCE(HTTP(url 'https://restcountries.com/v3.1/all?fields=cca2,region,unMember' format 'JSONEachRow' headers(header(name 'Content-Type' value 'application/json'))))
LAYOUT(HASHED())
LIFETIME(86400)
COMMENT 'Country codes with regions and UN membership status FROM REST Countries API using HTTP source';

CREATE DICTIONARY `ecommerce`.`user_segments_dict` ON CLUSTER `demo` (
    `user_id`      UInt64,
    `segment`      String,
    `score`        Float32 DEFAULT 0.0,
    `last_updated` DateTime
)
PRIMARY KEY `user_id`
SOURCE(CLICKHOUSE(host 'localhost' port 9000 user 'default' password '' db 'ecommerce' table 'user_segments_source'))
LAYOUT(FLAT())
LIFETIME(1800)
SETTINGS(max_threads = 2)
COMMENT 'User segmentation FROM ML service';