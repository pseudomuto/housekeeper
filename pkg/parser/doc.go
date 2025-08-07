// Package parser provides a comprehensive participle-based parser for ClickHouse DDL statements.
//
// This package implements a modern, robust parser using github.com/alecthomas/participle/v2
// that can parse and understand all major ClickHouse Data Definition Language (DDL) statements.
// It provides complete support for databases, tables, dictionaries, and views with full
// ClickHouse syntax compatibility including advanced features like complex data types,
// expressions, and cluster operations.
//
// Supported DDL Operations:
//   - Database operations: CREATE, ALTER, ATTACH, DETACH, DROP, RENAME DATABASE
//   - Table operations: CREATE, ALTER, ATTACH, DETACH, DROP, RENAME TABLE
//   - Dictionary operations: CREATE, ATTACH, DETACH, DROP, RENAME DICTIONARY
//   - View operations: CREATE, ATTACH, DETACH, DROP VIEW and MATERIALIZED VIEW
//   - Expression parsing: Complex expressions with proper operator precedence
//   - Data types: All ClickHouse types including Nullable, Array, Tuple, Map, Nested
//
// Key features:
//   - Complete ClickHouse DDL syntax support with all modern features
//   - Advanced expression engine with proper operator precedence
//   - Structured error messages with line and column information
//   - Type-safe AST representation of all parsed statements
//   - Support for all engines, data types, and their parameters
//   - ON CLUSTER support for distributed operations
//   - Comprehensive test coverage with testdata-driven tests
//   - Maintainable grammar rules instead of complex regex patterns
//
// Basic usage:
//
//	// Parse SQL string with comprehensive DDL support
//	grammar, err := parser.ParseSQL(`
//	    CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';
//	    CREATE TABLE analytics.events (
//	        id UUID DEFAULT generateUUIDv4(),
//	        timestamp DateTime,
//	        user_id UInt64,
//	        properties Map(String, String) DEFAULT map(),
//	        metadata Nullable(String) CODEC(ZSTD(3))
//	    ) ENGINE = MergeTree()
//	    PARTITION BY toYYYYMM(timestamp)
//	    ORDER BY (timestamp, user_id)
//	    TTL timestamp + INTERVAL 90 DAY;
//	    CREATE DICTIONARY users_dict (
//	        id UInt64 IS_OBJECT_ID,
//	        name String INJECTIVE
//	    ) PRIMARY KEY id
//	    SOURCE(HTTP(url 'http://api.example.com/users'))
//	    LAYOUT(HASHED())
//	    LIFETIME(3600);
//	    CREATE MATERIALIZED VIEW daily_stats
//	    ENGINE = MergeTree() ORDER BY date
//	    POPULATE
//	    AS SELECT toDate(timestamp) as date, count() as events
//	    FROM analytics.events GROUP BY date;
//	`)
//
//	// Parse from file
//	grammar, err := parser.ParseSQLFromFile("schema.sql")
//
//	// Parse from directory (combines all .sql files)
//	grammar, err := parser.ParseSQLFromDirectory("schemas/")
//
// The parser returns a SQL struct containing all parsed statements,
// which can be used for schema analysis, migration generation, validation,
// or any other DDL processing needs.
package parser
