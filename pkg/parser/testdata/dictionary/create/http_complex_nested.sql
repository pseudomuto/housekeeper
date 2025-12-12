CREATE DICTIONARY `complex_api_dict` (
    `entity_id` UInt64,
    `metadata`  String,
    `timestamp` DateTime
)
PRIMARY KEY `entity_id`
SOURCE(HTTP(url 'http://internal-api:9000/entities' format 'CSV' timeout 30 credentials(user 'service' password 'pass') headers(header(name 'Authorization' value 'Bearer token123') header(name 'User-Agent' value 'ClickHouse-Dictionary/1.0'))))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 60 MAX 3600);
