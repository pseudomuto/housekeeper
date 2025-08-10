-- Current state: no dictionary exists
;
-- Target state: create new users dictionary with HTTP source
CREATE DICTIONARY analytics.users_dict (
    id UInt64 IS_OBJECT_ID,
    name String,
    email String DEFAULT ''
) PRIMARY KEY id
SOURCE(HTTP(url 'http://api.example.com/users'))
LAYOUT(HASHED())
LIFETIME(3600);