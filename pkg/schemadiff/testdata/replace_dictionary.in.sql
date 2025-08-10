-- Current state: dictionary with basic columns and 1-hour lifetime
CREATE DICTIONARY analytics.users_dict (id UInt64, name String) PRIMARY KEY id SOURCE(HTTP(url 'http://api.com/users')) LAYOUT(HASHED()) LIFETIME(3600)
;
-- Target state: same dictionary with additional email column and 2-hour lifetime
CREATE DICTIONARY analytics.users_dict (id UInt64, name String, email String DEFAULT '') PRIMARY KEY id SOURCE(HTTP(url 'http://api.com/users')) LAYOUT(HASHED()) LIFETIME(7200);