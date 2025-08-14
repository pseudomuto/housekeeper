-- Dictionary with complex HTTP SOURCE DSL
CREATE DICTIONARY user_segments_dict
(
    user_id UInt64,
    segment String,
    score Float64
)
PRIMARY KEY user_id
SOURCE(HTTP(url 'http://ml-service:8080/user-segments' format 'TabSeparated' credentials(user 'user' password 'password') headers(header(name 'API-KEY' value 'key'))))
LAYOUT(HASHED())
LIFETIME(3600);

-- Dictionary with multiple headers and authentication
CREATE DICTIONARY analytics_dict
(
    id UInt64,
    data String
)
PRIMARY KEY id
SOURCE(HTTP(url 'https://api.analytics.com/data' format 'JSONEachRow' credentials(user 'api_user' password 'secret123') headers(header(name 'Content-Type' value 'application/json') header(name 'X-Custom-Header' value 'custom-value'))))
LAYOUT(FLAT())
LIFETIME(MIN 300 MAX 1800);

-- Dictionary with complex nested structure
CREATE DICTIONARY complex_api_dict
(
    entity_id UInt64,
    metadata String,
    timestamp DateTime
)
PRIMARY KEY entity_id
SOURCE(HTTP(url 'http://internal-api:9000/entities' format 'CSV' timeout 30 credentials(user 'service' password 'pass') headers(header(name 'Authorization' value 'Bearer token123') header(name 'User-Agent' value 'ClickHouse-Dictionary/1.0'))))
LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000))
LIFETIME(MIN 60 MAX 3600);