// Package parser provides a robust participle-based parser for ClickHouse DDL statements.
//
// This package implements a modern parser using github.com/alecthomas/participle/v2
// that can parse and understand ClickHouse Data Definition Language (DDL) statements.
// Currently, it supports database operations and dictionary management, providing complete support
// for CREATE, ALTER, ATTACH, DETACH, and DROP DATABASE statements, as well as CREATE, ATTACH, DETACH, 
// and DROP DICTIONARY statements.
//
// Key features:
//   - Full support for ClickHouse database DDL syntax
//   - Structured error messages with line and column information
//   - Type-safe AST representation of parsed statements
//   - Support for all database engines and their parameters
//   - ON CLUSTER support for distributed operations
//   - Comprehensive test coverage with testdata-driven tests
//
// Basic usage:
//
//	// Parse SQL string
//	grammar, err := parser.ParseSQL(`
//	    CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';
//	    CREATE DATABASE logs ON CLUSTER production;
//	    CREATE DICTIONARY users_dict (id UInt64, name String) PRIMARY KEY id SOURCE(HTTP(url 'test')) LAYOUT(FLAT()) LIFETIME(600);
//	    ATTACH DICTIONARY IF NOT EXISTS analytics.sales_dict ON CLUSTER production;
//	    DETACH DICTIONARY old_dict PERMANENTLY;
//	`)
//	
//	// Parse from file
//	grammar, err := parser.ParseSQLFromFile("schema.sql")
//	
//	// Parse from directory (combines all .sql files)
//	grammar, err := parser.ParseSQLFromDirectory("schemas/")
//
// The parser returns a Grammar struct containing all parsed statements,
// which can be used for schema analysis, migration generation, or validation.
package parser