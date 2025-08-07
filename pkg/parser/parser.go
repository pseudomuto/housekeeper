package parser

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var (
	// clickhouseLexer defines the lexer for ClickHouse DDL
	clickhouseLexer = lexer.MustSimple([]lexer.SimpleRule{
		{Name: "Comment", Pattern: `--[^\r\n]*`},
		{Name: "MultilineComment", Pattern: `/\*[^*]*\*+([^/*][^*]*\*+)*/`},
		{Name: "String", Pattern: `'([^'\\]|\\.)*'`},
		{Name: "BacktickIdent", Pattern: "`([^`\\\\]|\\\\.)*`"},
		{Name: "Number", Pattern: `\d+(\.\d*)?`},
		{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
		{Name: "NotEq", Pattern: `!=|<>`},
		{Name: "LtEq", Pattern: `<=`},
		{Name: "GtEq", Pattern: `>=`},
		{Name: "Punct", Pattern: `[(),.;=+\-*/%<>\[\]!]`},
		{Name: "Whitespace", Pattern: `\s+`},
	})

	// parser is the participle parser instance for ClickHouse DDL
	parser = participle.MustBuild[SQL](
		participle.Lexer(clickhouseLexer),
		participle.Elide("Comment", "MultilineComment", "Whitespace"),
		participle.CaseInsensitive("CREATE", "ALTER", "ATTACH", "DETACH", "DROP", "RENAME", "DATABASE", "DICTIONARY",
			"IF", "NOT", "EXISTS", "ON", "CLUSTER", "ENGINE", "COMMENT", "MODIFY", "PERMANENTLY", "SYNC",
			"OR", "REPLACE", "PRIMARY", "KEY", "SOURCE", "LAYOUT", "LIFETIME", "SETTINGS", "MIN", "MAX",
			"DEFAULT", "EXPRESSION", "IS_OBJECT_ID", "HIERARCHICAL", "INJECTIVE", "TO", "VIEW", "MATERIALIZED",
			"POPULATE", "AS", "SELECT", "TABLE", "NULLABLE", "ARRAY", "TUPLE", "NESTED", "MAP", "LOWCARDINALITY",
			"CODEC", "TTL", "EPHEMERAL", "ALIAS", "ORDER", "PARTITION", "SAMPLE", "BY", "INTERVAL",
			"ADD", "COLUMN", "AFTER", "FIRST", "REMOVE", "CLEAR", "IN", "DELETE", "WHERE", "UPDATE",
			"FREEZE", "WITH", "NAME", "FROM", "MOVE", "DISK", "VOLUME", "FETCH", "RESET", "SETTING",
			"INDEX", "TYPE", "GRANULARITY", "CONSTRAINT", "CHECK", "PROJECTION", "GROUP", "AND", "LIKE", "BETWEEN", "IS", "NULL",
			"TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "EXTRACT", "OVER",
			"ROWS", "RANGE", "UNBOUNDED", "PRECEDING", "CURRENT", "ROW", "FOLLOWING", "NULLS", "LAST",
			"SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR", "NOW", "TODAY",
			"YESTERDAY", "DESC", "ASC", "BLOOM_FILTER", "MINMAX", "HYPOTHESIS", "SET", "TOKENBF_V1", "NGRAMBF_V1",
			"HAVING", "LIMIT", "OFFSET", "DISTINCT", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS", "USING",
			"CUBE", "ROLLUP", "TOTALS", "COLLATE"),
		participle.UseLookahead(4),
	)
)

type (
	// SQL defines the complete ClickHouse DDL/DML SQL structure
	SQL struct {
		Statements []*Statement `parser:"@@*"`
	}

	// Statement represents any DDL or DML statement
	Statement struct {
		CreateDatabase   *CreateDatabaseStmt      `parser:"@@"`
		AlterDatabase    *AlterDatabaseStmt       `parser:"| @@"`
		AttachDatabase   *AttachDatabaseStmt      `parser:"| @@"`
		DetachDatabase   *DetachDatabaseStmt      `parser:"| @@"`
		DropDatabase     *DropDatabaseStmt        `parser:"| @@"`
		RenameDatabase   *RenameDatabaseStmt      `parser:"| @@"`
		CreateTable      *CreateTableStmt         `parser:"| @@"`
		AlterTable       *AlterTableStmt          `parser:"| @@"`
		CreateDictionary *CreateDictionaryStmt    `parser:"| @@"`
		CreateView       *CreateViewStmt          `parser:"| @@"`
		AttachView       *AttachViewStmt          `parser:"| @@"`
		AttachDictionary *AttachDictionaryStmt    `parser:"| @@"`
		DetachView       *DetachViewStmt          `parser:"| @@"`
		DetachDictionary *DetachDictionaryStmt    `parser:"| @@"`
		DropView         *DropViewStmt            `parser:"| @@"`
		DropDictionary   *DropDictionaryStmt      `parser:"| @@"`
		AttachTable      *AttachTableStmt         `parser:"| @@"`
		DetachTable      *DetachTableStmt         `parser:"| @@"`
		DropTable        *DropTableStmt           `parser:"| @@"`
		RenameTable      *RenameTableStmt         `parser:"| @@"`
		RenameDictionary *RenameDictionaryStmt    `parser:"| @@"`
		SelectStatement  *TopLevelSelectStatement `parser:"| @@"`
	}
)

// GetLexer returns the ClickHouse lexer definition used by the parser.
// This function is primarily useful for testing and debugging purposes,
// allowing access to the underlying lexer configuration.
//
// The returned lexer includes rules for ClickHouse-specific tokens including:
//   - Comments (single-line with -- and multi-line with /* */)
//   - String literals with escape sequences
//   - Backtick identifiers for reserved words and special characters
//   - Numeric literals (integers and decimals)
//   - Operators and punctuation
//   - ClickHouse keywords and reserved words
//
// Example usage for testing:
//
//	lexer := parser.GetLexer()
//	tokens, err := lexer.Tokenise("", "SELECT * FROM users WHERE active = 1")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	for _, token := range tokens {
//	    fmt.Printf("Token: %s = %q\n", token.Type, token.Value)
//	}
//
// This will output the tokenized representation of the SQL string, showing
// how the lexer breaks down the input into individual tokens for parsing.
func GetLexer() lexer.Definition {
	return clickhouseLexer
}

// ParseSQL parses ClickHouse DDL statements from a string and returns the parsed SQL structure.
// This is the primary parsing function that converts SQL text into structured DDL statements.
// It supports all implemented ClickHouse DDL operations including database creation,
// modification, attachment, detachment, and deletion.
//
// Example usage:
//
//	sql := `
//		CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics database';
//		CREATE TABLE analytics.events (
//			id UInt64,
//			user_id UInt64,
//			event_type LowCardinality(String),
//			timestamp DateTime DEFAULT now(),
//			data Map(String, String) DEFAULT map(),
//			metadata Nullable(String) CODEC(ZSTD)
//		) ENGINE = MergeTree()
//		ORDER BY (user_id, timestamp)
//		PARTITION BY toYYYYMM(timestamp)
//		SETTINGS index_granularity = 8192;
//		CREATE DICTIONARY analytics.users_dict (
//			id UInt64 IS_OBJECT_ID,
//			name String INJECTIVE
//		) PRIMARY KEY id
//		SOURCE(HTTP(url 'http://api.example.com/users'))
//		LAYOUT(HASHED())
//		LIFETIME(3600);
//		CREATE MATERIALIZED VIEW analytics.daily_stats
//		ENGINE = MergeTree() ORDER BY date
//		POPULATE
//		AS SELECT toDate(timestamp) as date, count() as cnt
//		FROM analytics.events
//		GROUP BY date;
//		ALTER DATABASE analytics MODIFY COMMENT 'Updated analytics database';
//		RENAME DATABASE analytics TO prod_analytics;
//		RENAME DICTIONARY prod_analytics.users_dict TO prod_analytics.user_data;
//		RENAME TABLE analytics.old_view TO analytics.new_view;
//	`
//
//	sqlResult, err := parser.ParseSQL(sql)
//	if err != nil {
//		log.Fatalf("Parse error: %v", err)
//	}
//
//	// Access parsed statements
//	for _, stmt := range sqlResult.Statements {
//		if stmt.CreateDatabase != nil {
//			fmt.Printf("CREATE DATABASE: %s\n", stmt.CreateDatabase.Name)
//		}
//		if stmt.CreateTable != nil {
//			name := stmt.CreateTable.Name
//			if stmt.CreateTable.Database != nil {
//				name = *stmt.CreateTable.Database + "." + name
//			}
//			fmt.Printf("CREATE TABLE: %s with %d columns\n", name, len(stmt.CreateTable.Columns))
//		}
//		if stmt.CreateDictionary != nil {
//			name := stmt.CreateDictionary.Name
//			if stmt.CreateDictionary.Database != nil {
//				name = *stmt.CreateDictionary.Database + "." + name
//			}
//			fmt.Printf("CREATE DICTIONARY: %s\n", name)
//		}
//		if stmt.RenameDatabase != nil {
//			for _, rename := range stmt.RenameDatabase.Renames {
//				fmt.Printf("RENAME DATABASE: %s TO %s\n", rename.From, rename.To)
//			}
//		}
//		if stmt.RenameDictionary != nil {
//			for _, rename := range stmt.RenameDictionary.Renames {
//				fromName := rename.FromName
//				if rename.FromDatabase != nil {
//					fromName = *rename.FromDatabase + "." + fromName
//				}
//				toName := rename.ToName
//				if rename.ToDatabase != nil {
//					toName = *rename.ToDatabase + "." + toName
//				}
//				fmt.Printf("RENAME DICTIONARY: %s TO %s\n", fromName, toName)
//			}
//		}
//		if stmt.CreateView != nil {
//			viewType := "VIEW"
//			if stmt.CreateView.Materialized {
//				viewType = "MATERIALIZED VIEW"
//			}
//			name := stmt.CreateView.Name
//			if stmt.CreateView.Database != nil {
//				name = *stmt.CreateView.Database + "." + name
//			}
//			fmt.Printf("CREATE %s: %s\n", viewType, name)
//		}
//	}
//
// Returns an error if the SQL contains syntax errors or unsupported constructs.
func ParseSQL(sql string) (*SQL, error) {
	sqlResult, err := parser.ParseString("", sql)
	if err != nil {
		return nil, fmt.Errorf("failed to parse SQL: %w", err)
	}

	return sqlResult, nil
}

// ParseSQLFromFile parses ClickHouse DDL statements from a file and returns the parsed SQL structure.
// This is a convenience function that reads a file and calls ParseSQL on its contents.
//
// Example usage:
//
//	sqlResult, err := parser.ParseSQLFromFile("schema.sql")
//	if err != nil {
//		log.Fatalf("Failed to parse schema file: %v", err)
//	}
//
//	// Process the parsed statements
//	for _, stmt := range sqlResult.Statements {
//		// Process each statement
//	}
//
// Returns an error if the file cannot be read or contains invalid SQL.
func ParseSQLFromFile(path string) (*SQL, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read SQL file: %w", err)
	}

	return ParseSQL(string(data))
}

// ParseSQLFromDirectory parses all .sql files in a directory and returns combined SQL structure.
// This function is useful for projects that split their schema definitions across multiple files.
// It automatically discovers all .sql files in the specified directory and combines their
// parsed results into a unified SQL representation.
//
// Example usage:
//
//	sqlResult, err := parser.ParseSQLFromDirectory("./schemas")
//	if err != nil {
//		log.Fatalf("Failed to parse schema directory: %v", err)
//	}
//
//	// The sqlResult now contains all statements from all .sql files in the directory
//	fmt.Printf("Parsed %d statements from directory\n", len(sqlResult.Statements))
//
//	for _, stmt := range sqlResult.Statements {
//		// Process each statement
//	}
//
// Returns an error if the directory cannot be read or any SQL file contains errors.
func ParseSQLFromDirectory(dir string) (*SQL, error) {
	var allStatements []*Statement

	files, err := filepath.Glob(filepath.Join(dir, "*.sql"))
	if err != nil {
		return nil, err
	}

	for _, file := range files {
		sqlResult, err := ParseSQLFromFile(file)
		if err != nil {
			return nil, fmt.Errorf("failed to parse %s: %w", file, err)
		}

		// Combine all statements
		allStatements = append(allStatements, sqlResult.Statements...)
	}

	return &SQL{Statements: allStatements}, nil
}

// parseBalancedParentheses parses content within balanced parentheses
// starting from the given position in the input string
