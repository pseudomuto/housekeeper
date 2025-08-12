package schema_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/stretchr/testify/require"
)

func TestCompile(t *testing.T) {
	t.Run("compiles simple schema without imports", func(t *testing.T) {
		// Create temporary schema file
		tmpDir := t.TempDir()
		schemaFile := filepath.Join(tmpDir, "schema.sql")
		schemaContent := `CREATE DATABASE test_db ENGINE = Atomic;
CREATE TABLE test_db.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`

		err := os.WriteFile(schemaFile, []byte(schemaContent), consts.ModeFile)
		require.NoError(t, err)

		// Compile schema
		var buf bytes.Buffer
		err = schema.Compile(schemaFile, &buf)
		require.NoError(t, err)

		compiled := buf.String()
		require.Contains(t, compiled, "CREATE DATABASE test_db")
		require.Contains(t, compiled, "CREATE TABLE test_db.users")
	})

	t.Run("compiles schema with imports", func(t *testing.T) {
		// Create temporary directory structure
		tmpDir := t.TempDir()

		// Create main schema file with import
		mainFile := filepath.Join(tmpDir, "main.sql")
		mainContent := `CREATE DATABASE main_db ENGINE = Atomic;
-- housekeeper:import tables/users.sql
-- housekeeper:import tables/orders.sql`
		err := os.WriteFile(mainFile, []byte(mainContent), consts.ModeFile)
		require.NoError(t, err)

		// Create tables directory and files
		tablesDir := filepath.Join(tmpDir, "tables")
		err = os.MkdirAll(tablesDir, consts.ModeDir)
		require.NoError(t, err)

		usersFile := filepath.Join(tablesDir, "users.sql")
		usersContent := `CREATE TABLE main_db.users (
	id UInt64,
	name String
) ENGINE = MergeTree() ORDER BY id;`
		err = os.WriteFile(usersFile, []byte(usersContent), consts.ModeFile)
		require.NoError(t, err)

		ordersFile := filepath.Join(tablesDir, "orders.sql")
		ordersContent := `CREATE TABLE main_db.orders (
	id UInt64,
	user_id UInt64,
	amount Decimal64(2)
) ENGINE = MergeTree() ORDER BY id;`
		err = os.WriteFile(ordersFile, []byte(ordersContent), consts.ModeFile)
		require.NoError(t, err)

		// Compile schema
		var buf bytes.Buffer
		err = schema.Compile(mainFile, &buf)
		require.NoError(t, err)

		compiled := buf.String()

		// Should contain all content from main file and imported files
		require.Contains(t, compiled, "CREATE DATABASE main_db")
		require.Contains(t, compiled, "CREATE TABLE main_db.users")
		require.Contains(t, compiled, "CREATE TABLE main_db.orders")
		require.Contains(t, compiled, "name String")
		require.Contains(t, compiled, "amount Decimal64(2)")

		// Should not contain the import directives themselves
		require.NotContains(t, compiled, "-- housekeeper:import")
	})

	t.Run("handles nested imports", func(t *testing.T) {
		// Create temporary directory structure
		tmpDir := t.TempDir()

		// Create main schema file
		mainFile := filepath.Join(tmpDir, "main.sql")
		mainContent := `CREATE DATABASE app ENGINE = Atomic;
-- housekeeper:import shared/common.sql`
		err := os.WriteFile(mainFile, []byte(mainContent), consts.ModeFile)
		require.NoError(t, err)

		// Create shared directory and common file
		sharedDir := filepath.Join(tmpDir, "shared")
		err = os.MkdirAll(sharedDir, consts.ModeDir)
		require.NoError(t, err)

		commonFile := filepath.Join(sharedDir, "common.sql")
		commonContent := `CREATE TABLE app.base_table (id UInt64) ENGINE = MergeTree() ORDER BY id;
-- housekeeper:import ../tables/specific.sql`
		err = os.WriteFile(commonFile, []byte(commonContent), consts.ModeFile)
		require.NoError(t, err)

		// Create tables directory and specific file
		tablesDir := filepath.Join(tmpDir, "tables")
		err = os.MkdirAll(tablesDir, consts.ModeDir)
		require.NoError(t, err)

		specificFile := filepath.Join(tablesDir, "specific.sql")
		specificContent := `CREATE TABLE app.specific_table (
	id UInt64,
	data String
) ENGINE = MergeTree() ORDER BY id;`
		err = os.WriteFile(specificFile, []byte(specificContent), consts.ModeFile)
		require.NoError(t, err)

		// Compile schema
		var buf bytes.Buffer
		err = schema.Compile(mainFile, &buf)
		require.NoError(t, err)

		compiled := buf.String()

		// Should contain content from all files
		require.Contains(t, compiled, "CREATE DATABASE app")
		require.Contains(t, compiled, "CREATE TABLE app.base_table")
		require.Contains(t, compiled, "CREATE TABLE app.specific_table")
		require.Contains(t, compiled, "data String")
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		var buf bytes.Buffer
		err := schema.Compile("non-existent-file.sql", &buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read file")
	})

	t.Run("returns error for non-existent import", func(t *testing.T) {
		// Create temporary schema file with invalid import
		tmpDir := t.TempDir()
		schemaFile := filepath.Join(tmpDir, "schema.sql")
		schemaContent := `CREATE DATABASE test_db ENGINE = Atomic;
-- housekeeper:import non-existent-import.sql`

		err := os.WriteFile(schemaFile, []byte(schemaContent), consts.ModeFile)
		require.NoError(t, err)

		// Try to compile schema
		var buf bytes.Buffer
		err = schema.Compile(schemaFile, &buf)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read file")
	})

	t.Run("handles absolute import paths", func(t *testing.T) {
		// Create temporary files
		tmpDir := t.TempDir()

		importFile := filepath.Join(tmpDir, "import.sql")
		importContent := `CREATE TABLE test_db.imported_table (id UInt64) ENGINE = MergeTree() ORDER BY id;`
		err := os.WriteFile(importFile, []byte(importContent), consts.ModeFile)
		require.NoError(t, err)

		mainFile := filepath.Join(tmpDir, "main.sql")
		mainContent := `CREATE DATABASE test_db ENGINE = Atomic;
-- housekeeper:import ` + importFile
		err = os.WriteFile(mainFile, []byte(mainContent), consts.ModeFile)
		require.NoError(t, err)

		// Compile schema
		var buf bytes.Buffer
		err = schema.Compile(mainFile, &buf)
		require.NoError(t, err)

		compiled := buf.String()
		require.Contains(t, compiled, "CREATE DATABASE test_db")
		require.Contains(t, compiled, "CREATE TABLE test_db.imported_table")
	})

	t.Run("preserves line structure", func(t *testing.T) {
		// Create temporary schema file
		tmpDir := t.TempDir()
		schemaFile := filepath.Join(tmpDir, "schema.sql")
		schemaContent := `-- This is a comment
CREATE DATABASE test_db ENGINE = Atomic;

-- Another comment
CREATE TABLE test_db.users (
    id UInt64,
    name String
) ENGINE = MergeTree() ORDER BY id;`

		err := os.WriteFile(schemaFile, []byte(schemaContent), consts.ModeFile)
		require.NoError(t, err)

		// Compile schema
		var buf bytes.Buffer
		err = schema.Compile(schemaFile, &buf)
		require.NoError(t, err)

		compiled := buf.String()
		lines := strings.Split(strings.TrimSpace(compiled), "\n")

		// Should preserve comments and structure
		require.Contains(t, lines[0], "This is a comment")
		require.Contains(t, lines[2], "") // Empty line preserved
		require.Contains(t, lines[3], "Another comment")
	})
}
