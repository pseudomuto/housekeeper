package parser_test

import "testing"

func TestCreateDictionary(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `CREATE DICTIONARY users_dict (id UInt64, name String, email String DEFAULT '') PRIMARY KEY id SOURCE(HTTP(url 'http://localhost/users.json' format 'JSONEachRow')) LAYOUT(FLAT()) LIFETIME(MIN 0 MAX 1000);`},
		{name: "or_replace", sql: `CREATE OR REPLACE DICTIONARY users_dict (id UInt64, name String DEFAULT 'Unknown', email String, department String EXPRESSION 'IT') PRIMARY KEY id SOURCE(MySQL(host 'localhost' port 3306 user 'root' password '' db 'test' table 'users')) LAYOUT(HASHED()) LIFETIME(3600) SETTINGS(max_threads = 4) COMMENT 'User directory';`},
		{name: "if_not_exists_on_cluster", sql: `CREATE DICTIONARY IF NOT EXISTS analytics.user_mapping ON CLUSTER production (user_id UInt64 IS_OBJECT_ID, user_name String, group_id UInt32 HIERARCHICAL, status String INJECTIVE) PRIMARY KEY user_id, group_id SOURCE(ClickHouse(host 'localhost' port 9000 user 'default' password '' db 'users' table 'mapping')) LAYOUT(COMPLEX_KEY_HASHED()) LIFETIME(MIN 60 MAX 3600) SETTINGS(max_block_size = 8192, max_threads = 2) COMMENT 'Complex user mapping dictionary';`},
		{name: "with_file_source", sql: `CREATE DICTIONARY simple_dict (key1 String, key2 UInt32, value1 String DEFAULT 'N/A', value2 Float64 EXPRESSION '0.0') PRIMARY KEY key1, key2 SOURCE(File(path '/data/dict.csv' format 'CSV')) LAYOUT(COMPLEX_KEY_CACHE(size_in_cells 1000000)) LIFETIME(MAX 1200 MIN 300);`},
		{name: "with_backticks", sql: "CREATE DICTIONARY `user-dict`.`order-lookup` (`user-id` UInt64 IS_OBJECT_ID, `order` String INJECTIVE, `select` String DEFAULT 'default_value') PRIMARY KEY `user-id` SOURCE(HTTP(url 'http://api.example.com/orders')) LAYOUT(HASHED()) LIFETIME(3600);"},
		{name: "reordered_clauses", sql: `CREATE DICTIONARY reordered_dict (id UInt64, name String) LAYOUT(FLAT()) SOURCE(HTTP(url 'http://localhost/data.json' format 'JSONEachRow')) PRIMARY KEY id LIFETIME(MIN 300 MAX 3600) SETTINGS(max_threads = 2) COMMENT 'Dictionary with flexible clause ordering';`},
		{name: "http_with_headers_function", sql: `CREATE DICTIONARY users_dict (id UInt64, name String) PRIMARY KEY id SOURCE(HTTP(url 'http://localhost/users' format 'JSONEachRow' headers(header('X-API-Key')))) LAYOUT(FLAT()) LIFETIME(3600);`},
		{name: "http_with_nested_functions", sql: `CREATE DICTIONARY complex_dict (id UInt64, data String) PRIMARY KEY id SOURCE(HTTP(url 'http://api.example.com/data' headers(list(header('auth-token'))))) LAYOUT(HASHED()) LIFETIME(MIN 300 MAX 1800);`},
		{name: "http_with_credentials", sql: `CREATE DICTIONARY user_segments_dict (user_id UInt64, segment String, score Float64) PRIMARY KEY user_id SOURCE(HTTP(url 'http://ml-service:8080/user-segments' format 'TabSeparated' credentials(user 'user' password 'password') headers(header(name 'API-KEY' value 'key')))) LAYOUT(HASHED()) LIFETIME(3600);`},
		{name: "http_with_multiple_headers", sql: `CREATE DICTIONARY analytics_dict (id UInt64, data String) PRIMARY KEY id SOURCE(HTTP(url 'https://api.analytics.com/data' format 'JSONEachRow' credentials(user 'api_user' password 'secret123') headers(header(name 'Content-Type' value 'application/json') header(name 'X-Custom-Header' value 'custom-value')))) LAYOUT(FLAT()) LIFETIME(MIN 300 MAX 1800);`},
		{name: "http_complex_nested", sql: `CREATE DICTIONARY complex_api_dict (entity_id UInt64, metadata String, timestamp DateTime) PRIMARY KEY entity_id SOURCE(HTTP(url 'http://internal-api:9000/entities' format 'CSV' timeout 30 credentials(user 'service' password 'pass') headers(header(name 'Authorization' value 'Bearer token123') header(name 'User-Agent' value 'ClickHouse-Dictionary/1.0')))) LAYOUT(COMPLEX_KEY_HASHED(size_in_cells 1000000)) LIFETIME(MIN 60 MAX 3600);`},
	}

	runStatementTests(t, "dictionary/create", tests)
}

func TestAttachDictionary(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `ATTACH DICTIONARY attach_basic_dict;`},
		{name: "if_not_exists", sql: `ATTACH DICTIONARY IF NOT EXISTS analytics.attach_ifnotexists_dict;`},
		{name: "on_cluster", sql: `ATTACH DICTIONARY attach_cluster_dict ON CLUSTER production;`},
		{name: "full_options", sql: `ATTACH DICTIONARY IF NOT EXISTS analytics.attach_full_dict ON CLUSTER production;`},
	}

	runStatementTests(t, "dictionary/attach", tests)
}

func TestDetachDictionary(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DETACH DICTIONARY detach_basic_dict;`},
		{name: "if_exists", sql: `DETACH DICTIONARY IF EXISTS analytics.detach_ifexists_dict;`},
		{name: "permanently", sql: `DETACH DICTIONARY detach_permanently_dict PERMANENTLY;`},
		{name: "sync", sql: `DETACH DICTIONARY detach_sync_dict SYNC;`},
		{name: "full_options", sql: `DETACH DICTIONARY IF EXISTS analytics.detach_full_dict ON CLUSTER production PERMANENTLY SYNC;`},
	}

	runStatementTests(t, "dictionary/detach", tests)
}

func TestDropDictionary(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "basic", sql: `DROP DICTIONARY drop_basic_dict;`},
		{name: "if_exists", sql: `DROP DICTIONARY IF EXISTS analytics.drop_ifexists_dict;`},
		{name: "on_cluster", sql: `DROP DICTIONARY drop_cluster_dict ON CLUSTER production;`},
		{name: "full_options", sql: `DROP DICTIONARY IF EXISTS analytics.drop_full_dict SYNC;`},
	}

	runStatementTests(t, "dictionary/drop", tests)
}

func TestRenameDictionary(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "single", sql: `RENAME DICTIONARY old_dict TO new_dict;`},
		{name: "with_database", sql: `RENAME DICTIONARY db.old_dict TO db.new_dict;`},
		{name: "multiple", sql: `RENAME DICTIONARY dict1 TO dict2, db.dict3 TO db.dict4;`},
		{name: "on_cluster", sql: `RENAME DICTIONARY old_dict TO new_dict ON CLUSTER my_cluster;`},
		{name: "multiple_on_cluster", sql: `RENAME DICTIONARY db1.dict1 TO db2.dict2, dict3 TO dict4 ON CLUSTER production;`},
	}

	runStatementTests(t, "dictionary/rename", tests)
}
