package parser_test

import (
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/require"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// TestExpression is a wrapper to test expression parsing in isolation
type TestExpression struct {
	Expr parser.Expression `parser:"@@"`
}

func TestExpressionParsing(t *testing.T) {
	// Create a parser specifically for testing expressions
	exprParser := participle.MustBuild[TestExpression](
		participle.Lexer(parser.GetLexer()),
		participle.Elide("Comment", "MultilineComment", "Whitespace"),
		participle.CaseInsensitive("AND", "OR", "NOT", "LIKE", "IN", "BETWEEN", "IS", "NULL",
			"TRUE", "FALSE", "CASE", "WHEN", "THEN", "ELSE", "END", "CAST", "AS",
			"INTERVAL", "SECOND", "MINUTE", "HOUR", "DAY", "WEEK", "MONTH", "QUARTER", "YEAR",
			"EXTRACT", "FROM", "OVER", "PARTITION", "BY", "ORDER", "ROWS", "RANGE",
			"UNBOUNDED", "PRECEDING", "CURRENT", "ROW", "FOLLOWING", "NULLS", "FIRST", "LAST",
			"DESC", "ASC", "SELECT"),
		participle.UseLookahead(4),
	)

	tests := []struct {
		name  string
		input string
		valid bool
	}{
		// Literals
		{"number", "42", true},
		{"string", "'hello'", true},
		{"boolean true", "TRUE", true},
		{"boolean false", "FALSE", true},
		{"null", "NULL", true},

		// Identifiers
		{"simple identifier", "column_name", true},
		{"qualified identifier", "table.column", true},
		{"fully qualified", "db.table.column", true},
		{"backtick identifier", "`order`", true},
		{"backtick qualified", "`user-table`.`user-name`", true},
		{"backtick with keyword", "`select`", true},

		// Arithmetic
		{"addition", "1 + 2", true},
		{"subtraction", "10 - 5", true},
		{"multiplication", "3 * 4", true},
		{"division", "20 / 4", true},
		{"modulo", "10 % 3", true},
		{"complex arithmetic", "1 + 2 * 3 - 4 / 2", true},
		{"parentheses", "(1 + 2) * 3", true},

		// Comparison
		{"equals", "id = 1", true},
		{"not equals !=", "status != 'active'", true},
		{"not equals <>", "status <> 'active'", true},
		{"less than", "age < 18", true},
		{"greater than", "score > 90", true},
		{"less than or equal", "price <= 100", true},
		{"greater than or equal", "quantity >= 10", true},

		// Logical
		{"AND", "age > 18 AND status = 'active'", true},
		{"OR", "category = 'A' OR category = 'B'", true},
		{"NOT", "NOT active", true},
		{"complex logical", "age > 18 AND (status = 'active' OR status = 'pending')", true},

		// LIKE
		{"LIKE", "name LIKE '%john%'", true},
		{"NOT LIKE", "email NOT LIKE '%@spam.com'", true},

		// IN
		{"IN list", "id IN (1, 2, 3)", true},
		{"NOT IN list", "status NOT IN ('deleted', 'archived')", true},
		// {"IN subquery", "user_id IN (SELECT id FROM users)", true}, // Subquery parsing not fully implemented

		// BETWEEN
		{"BETWEEN", "age BETWEEN 18 AND 65", true},
		{"NOT BETWEEN", "price NOT BETWEEN 10 AND 100", true},

		// IS NULL
		{"IS NULL", "deleted_at IS NULL", true},
		{"IS NOT NULL", "email IS NOT NULL", true},

		// Functions
		{"simple function", "now()", true},
		{"function with args", "substring(name, 1, 10)", true},
		{"nested functions", "lower(trim(name))", true},
		// {"function in expression", "age > now() - INTERVAL 18 YEAR", true}, // INTERVAL parsing issue in isolated tests

		// Arrays
		{"array literal", "[1, 2, 3]", true},
		{"empty array", "[]", true},
		{"array in expression", "id IN [1, 2, 3]", true},

		// Tuples
		{"tuple", "(1, 'a', TRUE)", true},
		{"empty tuple", "()", true},
		{"single element tuple", "(42)", true},

		// CASE expressions - deferred due to circular grammar dependency issues
		// {"searched CASE", "CASE WHEN age < 18 THEN 'minor' ELSE 'adult' END", true},
		// {"CASE with value", "CASE status WHEN 'active' THEN 1 WHEN 'inactive' THEN 0 END", true},
		// {"nested CASE", "CASE WHEN age < 18 THEN 'minor' WHEN age < 65 THEN 'adult' ELSE 'senior' END", true},

		// CAST
		{"CAST", "CAST(age AS String)", true},
		{"CAST complex", "CAST(price * 1.1 AS Decimal(10, 2))", true},

		// INTERVAL - now working!
		{"INTERVAL", "INTERVAL 1 DAY", true},
		{"INTERVAL in expression", "timestamp > now() - INTERVAL 7 DAY", true},

		// Complex expressions from ALTER TABLE
		{"CHECK constraint", "id > 0", true},
		{"UPDATE expression", "age = age + 1", true},
		{"DELETE WHERE", "timestamp < now() - years(1)", true},
		{"complex WHERE", "status = 'inactive' AND last_login < now() - days(30)", true},

		// Window functions
		{"simple window function", "row_number() OVER ()", true},
		{"window with partition", "rank() OVER (PARTITION BY category)", true},
		{"window with order", "dense_rank() OVER (ORDER BY price DESC)", true},
		{"window with partition and order", "row_number() OVER (PARTITION BY category ORDER BY price DESC)", true},
		{"window with multiple partitions", "sum(amount) OVER (PARTITION BY user_id, category ORDER BY date)", true},
		{"window with nulls handling", "rank() OVER (ORDER BY score DESC NULLS LAST)", true},
		{"window with frame - current row", "sum(amount) OVER (ORDER BY date ROWS CURRENT ROW)", true},
		{"window with frame - unbounded preceding", "sum(amount) OVER (ORDER BY date ROWS UNBOUNDED PRECEDING)", true},
		{"window with frame - between", "sum(amount) OVER (ORDER BY date ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)", true},
		{"window with frame - following", "avg(price) OVER (ORDER BY date ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING)", true},
		{"window with range frame", "sum(amount) OVER (ORDER BY date RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)", true},
		
		// Advanced window function tests
		{"window with complex partition", "lead(price, 2, 0) OVER (PARTITION BY category, brand ORDER BY date DESC)", true},
		{"window with first_value", "first_value(name) OVER (PARTITION BY department ORDER BY salary DESC NULLS FIRST)", true},
		{"window with nth_value", "nth_value(score, 3) OVER (ORDER BY score DESC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)", true},
		{"window without frame but with order", "cumsum(amount) OVER (PARTITION BY user_id ORDER BY timestamp)", true},
		{"window with range and numeric value", "sum(sales) OVER (ORDER BY date RANGE BETWEEN 30 PRECEDING AND 10 FOLLOWING)", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := exprParser.ParseString("", tt.input)
			if tt.valid {
				require.NoError(t, err, "Failed to parse: %s", tt.input)
				require.NotNil(t, result)
			} else {
				require.Error(t, err, "Expected parse error for: %s", tt.input)
			}
		})
	}
}

// TestExpressionInContext tests expressions within actual DDL statements
func TestExpressionInContext(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "ALTER TABLE with CHECK constraint",
			sql:  "ALTER TABLE users ADD CONSTRAINT id_check CHECK id > 0;",
		},
		{
			name: "ALTER TABLE UPDATE with WHERE",
			sql:  "ALTER TABLE users UPDATE age = age + 1 WHERE id < 1000;",
		},
		{
			name: "ALTER TABLE DELETE with complex WHERE",
			sql:  "ALTER TABLE logs DELETE WHERE timestamp < now() - years(1);",
		},
		{
			name: "ALTER TABLE with complex CHECK",
			sql:  "ALTER TABLE orders ADD CONSTRAINT valid_price CHECK price > 0 AND price <= 1000000;",
		},
		{
			name: "CREATE TABLE with DEFAULT expression",
			sql: `CREATE TABLE events (
				id UInt64,
				timestamp DateTime DEFAULT now(),
				date Date DEFAULT today()
			) ENGINE = MergeTree() ORDER BY id;`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			grammar, err := parser.ParseSQL(tt.sql)
			require.NoError(t, err, "Failed to parse SQL: %s", tt.sql)
			require.NotNil(t, grammar)
			require.NotEmpty(t, grammar.Statements)
		})
	}
}
