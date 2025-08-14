-- Product category hierarchy dictionary
CREATE DICTIONARY ecommerce.categories_dict ON CLUSTER demo (
    id UInt64,
    name String,
    parent_id UInt64 DEFAULT 0 HIERARCHICAL,
    level UInt8 DEFAULT 0,
    is_active Bool DEFAULT true
) PRIMARY KEY id
SOURCE(CLICKHOUSE(
    host 'localhost'
    port 9000
    user 'default'
    password ''
    db 'ecommerce'
    table 'categories_source'
))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 3600)
COMMENT 'Product category hierarchy with parent relationships';
