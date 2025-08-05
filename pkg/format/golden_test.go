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
	require.NotEmpty(t, matches, "No *.in.sql files found in testdata directory")

	for _, inputFile := range matches {
		// Derive output filename: "example.in.sql" -> "example.sql"
		basename := filepath.Base(inputFile)
		outputName := strings.TrimSuffix(basename, ".in.sql") + ".sql"

		t.Run(outputName, func(t *testing.T) {
			// Read input SQL file
			inputSQL, err := os.ReadFile(inputFile)
			require.NoError(t, err, "Failed to read input file %s", inputFile)

			// Parse the SQL
			grammar, err := parser.ParseSQL(string(inputSQL))
			require.NoError(t, err, "Failed to parse SQL from %s", inputFile)

			// Format all statements using the new API
			var buf bytes.Buffer
			err = Format(&buf, Defaults, grammar.Statements...)
			require.NoError(t, err)
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
