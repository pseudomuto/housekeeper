-- Dictionary operations with complex configurations
CREATE DICTIONARY analytics.users_dict (
id UInt64 IS_OBJECT_ID,
name String INJECTIVE,
email String,
department String DEFAULT 'unknown',
manager_id UInt64 DEFAULT 0 HIERARCHICAL
) PRIMARY KEY id
SOURCE(HTTP(url 'https://api.company.com/users', format 'JSONEachRow'))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 300 MAX 3600)
SETTINGS(max_threads = 4, http_connection_timeout = 10)
COMMENT 'User directory with hierarchical structure';

CREATE OR REPLACE DICTIONARY warehouse.product_categories (
category_id UInt32,
category_name String,
parent_id UInt32 DEFAULT 0
) PRIMARY KEY category_id
SOURCE(CLICKHOUSE(host 'localhost', port 9000, db 'warehouse', table 'categories', user 'default', password ''))
LAYOUT(HASHED())
LIFETIME(3600);