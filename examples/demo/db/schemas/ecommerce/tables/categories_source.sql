-- Source table for categories dictionary
CREATE TABLE ecommerce.categories_source ON CLUSTER demo (
    id UInt64,
    name String,
    parent_id UInt64,
    level UInt8,
    is_active Bool
) ENGINE = Memory()
COMMENT 'Source data for categories dictionary';