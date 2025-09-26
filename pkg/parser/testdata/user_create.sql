-- Basic CREATE USER
CREATE USER name1;

-- Basic CREATE USER
CREATE USER `special-user`;

-- with if not exists
CREATE USER IF NOT EXISTS name2;

-- with or replace
CREATE USER OR REPLACE name3;

-- with if not exists on cluster
CREATE USER IF NOT EXISTS name4 ON CLUSTER production;

-- not identified
CREATE USER name5 NOT IDENTIFIED;

-- identified with plaintext password
CREATE USER name6 IDENTIFIED WITH plaintext_password BY 'my_password';

-- identification method omitted
CREATE USER name7 IDENTIFIED BY 'qwerty';

-- identified with sha256_hash password
CREATE USER name8 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';

-- identified with http server
CREATE USER name9 IDENTIFIED WITH http SERVER 'test_http_server';

--
CREATE USER name10 IDENTIFIED BY 'qwerty';

--
CREATE USER name11 IDENTIFIED WITH ldap SERVER 'server_name';

--
CREATE USER name12 IDENTIFIED WITH kerberos;

--
CREATE USER name13 IDENTIFIED WITH kerberos REALM 'test_realm';

--
CREATE USER name14 IDENTIFIED WITH ssl_certificate CN 'mysite.com:user';

-- HOST IP with subnet
CREATE USER name15 HOST IP '192.168.0.0/16';

-- HOST IP with IPv6 subnet
CREATE USER name16 HOST IP '2001:DB8::/32';

-- HOST ANY
CREATE USER name17 HOST ANY;

-- HOST LOCAL
CREATE USER name18 HOST LOCAL;

-- HOST NAME with FQDN
CREATE USER name19 HOST NAME 'mysite.com';

-- HOST REGEXP with pattern
CREATE USER name20 HOST REGEXP '.*\.mysite\.com';

-- HOST LIKE with template
CREATE USER name21 HOST LIKE '%';

-- HOST LIKE with domain filter
CREATE USER name22 HOST LIKE '%.mysite.com';

-- VALID UNTIL standalone with date only
CREATE USER name23 VALID UNTIL '2025-01-01';

-- VALID UNTIL standalone with date and time
CREATE USER name24 VALID UNTIL '2025-01-01 12:00:00 UTC';

-- VALID UNTIL standalone with infinity
CREATE USER name25 VALID UNTIL 'infinity';

-- VALID UNTIL standalone with backtick timezone
CREATE USER name26 VALID UNTIL '2025-01-01 12:00:00 `Asia/Tokyo`';

-- VALID UNTIL with identification method
CREATE USER name27 IDENTIFIED BY 'password' VALID UNTIL '2025-01-01';

-- VALID UNTIL at identification method level
CREATE USER name28 IDENTIFIED BY 'hash_value' VALID UNTIL '2025-12-31 23:59:59 UTC';

-- GRANTEES with user list
CREATE USER name29 GRANTEES user1, user2, 'special-user';

-- GRANTEES with ANY
CREATE USER name31 GRANTEES ANY;

-- GRANTEES with NONE
CREATE USER name32 GRANTEES NONE;

-- Simple test with user name
CREATE USER name30 GRANTEES simple_user;

-- User with access storage type
CREATE USER name33 IN local_directory;

-- User with valid until and access storage type
CREATE USER name34 VALID UNTIL '2025-12-31 23:59:59 UTC' IN ldap_directory;

-- User with identification, valid until, and access storage type
CREATE USER name35 IDENTIFIED BY 'password123' VALID UNTIL '2025-06-01' IN memory;

-- User with default single role
CREATE USER name36 DEFAULT ROLE admin;

-- User with default multiple roles
CREATE USER name37 DEFAULT ROLE admin, user, reader;

-- User with default roles using quotes
CREATE USER name38 DEFAULT ROLE 'special-role', guest;

-- User with default all roles except some
CREATE USER name39 DEFAULT ROLE ALL EXCEPT role1, role2;

-- User with default all roles except quoted roles
CREATE USER name40 DEFAULT ROLE ALL EXCEPT 'admin-role', guest;

-- User with default database
CREATE USER name41 DEFAULT DATABASE analytics;

-- User with default database using quotes
CREATE USER name42 DEFAULT DATABASE 'special-db';

-- User with default database set to NONE
CREATE USER name43 DEFAULT DATABASE NONE;
