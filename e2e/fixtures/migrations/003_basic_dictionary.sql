-- Basic dictionary for user status mapping
CREATE DICTIONARY analytics.user_status_dict (
    id UInt64 IS_OBJECT_ID,
    status String INJECTIVE,
    description String DEFAULT 'Unknown'
) PRIMARY KEY id
SOURCE(HTTP(url 'http://api.example.com/user_status' format 'JSONEachRow'))
LAYOUT(HASHED())
LIFETIME(MIN 300 MAX 3600)
COMMENT 'User status lookup dictionary';