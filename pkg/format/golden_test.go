package format_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/golden"
)

func TestGoldenFiles(t *testing.T) {
	testdataDir := "testdata"

	// Find all *.in.sql files
	pattern := filepath.Join(testdataDir, "*.in.sql")
	matches, err := filepath.Glob(pattern)
	require.NoError(t, err)
	require.NotEmpty(t, matches)

	for _, inputFile := range matches {
		// Derive output filename: "example.in.sql" -> "example.sql"
		basename := filepath.Base(inputFile)
		outputName := strings.TrimSuffix(basename, ".in.sql") + ".sql"

		t.Run(outputName, func(t *testing.T) {
			// Read input SQL file
			inputSQL, err := os.ReadFile(inputFile)
			require.NoError(t, err)

			// Parse the SQL
			sqlResult, err := parser.ParseString(string(inputSQL))
			require.NoError(t, err)

			// Format all statements using the new API
			var buf bytes.Buffer
			require.NoError(t, Format(&buf, Defaults, sqlResult.Statements...))
			result := buf.String()

			// Add final newline for proper file ending
			if result != "" {
				result += "\n"
			}

			// Compare with golden file
			golden.Assert(t, result, outputName)
		})
	}
}
