package parser

import (
	"io"
	"regexp"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/pkg/errors"
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
		participle.UseLookahead(4),
		participle.CaseInsensitive("Ident"), // Make identifier matching case-insensitive for keywords
	)
)

// normalizeCase is no longer needed with case-insensitive parser
func normalizeCase(sql string) string {
	// With case-insensitive parsing enabled, no normalization needed
	return sql
}

// normalizeImplicitAliases converts implicit table aliases to explicit AS syntax
func normalizeImplicitAliases(sql string) string {
	// Handle the most common cases using simple patterns
	// Be more careful to avoid transforming already correct SQL
	result := sql

	// Process patterns carefully, checking each match individually

	// Pattern 1: FROM tablename alias WHERE/GROUP/ORDER/etc
	keywords := []string{"WHERE", "LEFT", "RIGHT", "INNER", "JOIN", "GROUP", "ORDER", "LIMIT", "HAVING", "SETTINGS"}

	for _, keyword := range keywords {
		// Only process if the match doesn't already contain AS
		pattern := regexp.MustCompile(`\bFROM\s+(\w+(?:\.\w+)?)\s+(\w+)\s+` + keyword + `\b`)
		matches := pattern.FindAllStringSubmatch(result, -1)
		for _, match := range matches {
			if len(match) == 3 && !regexp.MustCompile(`\bAS\s+`).MatchString(match[0]) {
				result = strings.ReplaceAll(result, match[0], "FROM "+match[1]+" AS "+match[2]+" "+keyword)
			}
		}
	}

	// Pattern 2: FROM tablename alias ) (for subqueries)
	pattern2 := regexp.MustCompile(`\bFROM\s+(\w+(?:\.\w+)?)\s+(\w+)\s*\)`)
	matches2 := pattern2.FindAllStringSubmatch(result, -1)
	for _, match := range matches2 {
		if len(match) == 3 && !regexp.MustCompile(`\bAS\s+`).MatchString(match[0]) {
			result = strings.ReplaceAll(result, match[0], "FROM "+match[1]+" AS "+match[2]+" )")
		}
	}

	// Pattern 3: FROM tablename alias; (end of statement)
	semicolonPattern := regexp.MustCompile(`\bFROM\s+(\w+(?:\.\w+)?)\s+(\w+)\s*;`)
	matches3 := semicolonPattern.FindAllStringSubmatch(result, -1)
	for _, match := range matches3 {
		if len(match) == 3 && !regexp.MustCompile(`\bAS\s+`).MatchString(match[0]) {
			result = strings.ReplaceAll(result, match[0], "FROM "+match[1]+" AS "+match[2]+";")
		}
	}

	// Pattern 4: JOIN tablename alias ON (for JOIN clauses)
	joinPattern := regexp.MustCompile(`\bJOIN\s+(\w+(?:\.\w+)?)\s+(\w+)\s+ON\b`)
	matches4 := joinPattern.FindAllStringSubmatch(result, -1)
	for _, match := range matches4 {
		if len(match) == 3 && !regexp.MustCompile(`\bAS\s+`).MatchString(match[0]) {
			result = strings.ReplaceAll(result, match[0], "JOIN "+match[1]+" AS "+match[2]+" ON")
		}
	}

	// Pattern 5: ) alias ON (for JOIN conditions on subqueries)
	onPattern := regexp.MustCompile(`\)\s+(\w+)\s+ON\b`)
	matches5 := onPattern.FindAllStringSubmatch(result, -1)
	for _, match := range matches5 {
		if len(match) == 2 && !regexp.MustCompile(`\bAS\s+`).MatchString(match[0]) {
			result = strings.ReplaceAll(result, match[0], ") AS "+match[1]+" ON")
		}
	}

	return result
}

type (
	// SQL defines the complete ClickHouse DDL/DML SQL structure
	SQL struct {
		Statements []*Statement `parser:"@@*"`
	}

	// Statement represents any DDL or DML statement
	Statement struct {
		CreateDatabase        *CreateDatabaseStmt        `parser:"@@"`
		AlterDatabase         *AlterDatabaseStmt         `parser:"| @@"`
		AttachDatabase        *AttachDatabaseStmt        `parser:"| @@"`
		DetachDatabase        *DetachDatabaseStmt        `parser:"| @@"`
		DropDatabase          *DropDatabaseStmt          `parser:"| @@"`
		RenameDatabase        *RenameDatabaseStmt        `parser:"| @@"`
		CreateTable           *CreateTableStmt           `parser:"| @@"`
		AlterTable            *AlterTableStmt            `parser:"| @@"`
		CreateDictionary      *CreateDictionaryStmt      `parser:"| @@"`
		CreateView            *CreateViewStmt            `parser:"| @@"`
		CreateNamedCollection *CreateNamedCollectionStmt `parser:"| @@"`
		AlterNamedCollection  *AlterNamedCollectionStmt  `parser:"| @@"`
		CreateRole            *CreateRoleStmt            `parser:"| @@"`
		AlterRole             *AlterRoleStmt             `parser:"| @@"`
		DropRole              *DropRoleStmt              `parser:"| @@"`
		SetRole               *SetRoleStmt               `parser:"| @@"`
		SetDefaultRole        *SetDefaultRoleStmt        `parser:"| @@"`
		Grant                 *GrantStmt                 `parser:"| @@"`
		Revoke                *RevokeStmt                `parser:"| @@"`
		AttachView            *AttachViewStmt            `parser:"| @@"`
		AttachDictionary      *AttachDictionaryStmt      `parser:"| @@"`
		DetachView            *DetachViewStmt            `parser:"| @@"`
		DetachDictionary      *DetachDictionaryStmt      `parser:"| @@"`
		DropView              *DropViewStmt              `parser:"| @@"`
		DropDictionary        *DropDictionaryStmt        `parser:"| @@"`
		DropNamedCollection   *DropNamedCollectionStmt   `parser:"| @@"`
		AttachTable           *AttachTableStmt           `parser:"| @@"`
		DetachTable           *DetachTableStmt           `parser:"| @@"`
		DropTable             *DropTableStmt             `parser:"| @@"`
		RenameTable           *RenameTableStmt           `parser:"| @@"`
		RenameDictionary      *RenameDictionaryStmt      `parser:"| @@"`
		SelectStatement       *TopLevelSelectStatement   `parser:"| @@"`
	}
)

// Parse parses ClickHouse DDL statements from an io.Reader and returns the parsed SQL structure.
// This function allows parsing SQL from any source that implements io.Reader, including files,
// strings, network connections, or in-memory buffers.
//
// Example usage:
//
//	// Parse from a string
//	reader := strings.NewReader("CREATE DATABASE test ENGINE = Atomic;")
//	sqlResult, err := parser.Parse(reader)
//	if err != nil {
//		log.Fatalf("Parse error: %v", err)
//	}
//
//	// Parse from a file
//	file, err := os.Open("schema.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	sqlResult, err = parser.Parse(file)
//	if err != nil {
//		log.Fatalf("Parse error: %v", err)
//	}
//
//	// Parse from an HTTP response
//	resp, err := http.Get("https://example.com/schema.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer resp.Body.Close()
//
//	sqlResult, err = parser.Parse(resp.Body)
//	if err != nil {
//		log.Fatalf("Parse error: %v", err)
//	}
//
//	// Access parsed statements
//	for _, stmt := range sqlResult.Statements {
//		if stmt.CreateDatabase != nil {
//			fmt.Printf("CREATE DATABASE: %s\n", stmt.CreateDatabase.Name)
//		}
//	}
//
// Returns an error if the reader cannot be read or contains invalid SQL.
func Parse(reader io.Reader) (*SQL, error) {
	sqlResult, err := parser.Parse("", reader)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse SQL")
	}

	return sqlResult, nil
}

// ParseString parses ClickHouse DDL statements from a string and returns the parsed SQL structure.
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
//	sqlResult, err := parser.ParseString(sql)
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
func ParseString(sql string) (*SQL, error) {
	// Normalize case to uppercase for consistent parsing
	normalizedSQL := normalizeCase(sql)
	// Convert implicit table aliases to explicit AS syntax
	aliasNormalizedSQL := normalizeImplicitAliases(normalizedSQL)
	return Parse(strings.NewReader(aliasNormalizedSQL))
}
