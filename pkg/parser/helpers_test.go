package parser_test

import (
	"bytes"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/golden"
)

// statementTest defines a single test case for statement parsing
type statementTest struct {
	name string // Test name, also used as golden file name
	sql  string // Input SQL to parse
}

// runStatementTests runs golden file tests for a category of statements.
// Each test case parses the SQL, formats it, and compares against a golden file
// at testdata/<category>/<name>.sql
func runStatementTests(t *testing.T, category string, tests []statementTest) {
	t.Helper()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			parsed, err := ParseString(tt.sql)
			require.NoError(t, err)

			var buf bytes.Buffer
			require.NoError(t, format.Format(&buf, format.Defaults, parsed.Statements...))

			// Golden file at testdata/<category>/<name>.sql
			golden.Assert(t, buf.String()+"\n", category+"/"+tt.name+".sql")
		})
	}
}
