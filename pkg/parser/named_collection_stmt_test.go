package parser_test

import "testing"

func TestCreateNamedCollection(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "s3", sql: `CREATE NAMED COLLECTION my_s3_collection AS access_key_id = 'AKIAIOSFODNN7EXAMPLE', secret_access_key = 'wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY', endpoint = 'https://s3.amazonaws.com/', region = 'us-east-1' NOT OVERRIDABLE;`},
		{name: "kafka", sql: `CREATE NAMED COLLECTION kafka_config AS kafka_broker_list = 'localhost:9092', kafka_topic_list = 'events', kafka_group_name = 'clickhouse', kafka_format = 'JSONEachRow', kafka_max_block_size = 1048576, kafka_skip_broken_messages = 1 OVERRIDABLE;`},
	}

	runStatementTests(t, "named_collection/create", tests)
}

func TestAlterNamedCollection(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "set_delete", sql: `ALTER NAMED COLLECTION kafka_config SET kafka_topic_list = 'events,logs' OVERRIDABLE, kafka_max_block_size = 2097152 NOT OVERRIDABLE DELETE kafka_skip_broken_messages;`},
	}

	runStatementTests(t, "named_collection/alter", tests)
}

func TestDropNamedCollection(t *testing.T) {
	t.Parallel()

	tests := []statementTest{
		{name: "if_exists", sql: `DROP NAMED COLLECTION IF EXISTS old_s3_config;`},
	}

	runStatementTests(t, "named_collection/drop", tests)
}
