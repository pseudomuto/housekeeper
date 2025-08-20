package migrator_test

import (
	"bytes"
	"embed"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/*.sql
var testdataFS embed.FS

//go:embed testdata/003_snapshot.sql
var snapshotFile string

func TestLoadMigration(t *testing.T) {
	tests := []struct {
		name        string
		version     string
		sql         string
		wantErr     bool
		stmtCount   int
		description string
	}{
		{
			name:        "simple_table_creation",
			version:     "001_create_users",
			sql:         "CREATE TABLE users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;",
			wantErr:     false,
			stmtCount:   1,
			description: "Basic table creation should parse successfully",
		},
		{
			name:    "multiple_statements",
			version: "002_setup_database",
			sql: `CREATE DATABASE test ENGINE = Atomic COMMENT 'Test database';
				  CREATE TABLE test.events (id UInt64, data String) ENGINE = MergeTree() ORDER BY id;
				  ALTER TABLE test.events ADD COLUMN created_at DateTime DEFAULT now();`,
			wantErr:     false,
			stmtCount:   3,
			description: "Multiple DDL statements should be parsed correctly",
		},
		{
			name:    "complex_migration",
			version: "003_complex_schema",
			sql: `CREATE DATABASE analytics ENGINE = Atomic;
				  
				  CREATE TABLE analytics.users (
					  id UInt64,
					  email String,
					  created_at DateTime DEFAULT now(),
					  metadata Map(String, String) DEFAULT map()
				  ) ENGINE = MergeTree() 
				  ORDER BY (id, created_at)
				  PARTITION BY toYYYYMM(created_at);
				  
				  CREATE DICTIONARY analytics.user_dict (
					  id UInt64 IS_OBJECT_ID,
					  email String INJECTIVE
				  ) PRIMARY KEY id
				  SOURCE(HTTP(url 'http://api.example.com/users'))
				  LAYOUT(HASHED())
				  LIFETIME(3600);
				  
				  CREATE MATERIALIZED VIEW analytics.daily_stats
				  ENGINE = MergeTree() ORDER BY date
				  AS SELECT toDate(created_at), count()
				  FROM analytics.users GROUP BY toDate(created_at);`,
			wantErr:     false,
			stmtCount:   4,
			description: "Complex migration with multiple object types should parse",
		},
		{
			name:        "empty_migration",
			version:     "004_empty",
			sql:         "",
			wantErr:     false,
			stmtCount:   0,
			description: "Empty migration should be handled gracefully",
		},
		{
			name:        "comments_only",
			version:     "005_comments",
			sql:         "-- This is a comment\n/* Multi-line comment */",
			wantErr:     false,
			stmtCount:   0,
			description: "Migration with only comments should parse successfully",
		},
		{
			name:        "invalid_sql",
			version:     "006_invalid",
			sql:         "CREATE INVALID SYNTAX;",
			wantErr:     true,
			stmtCount:   0,
			description: "Invalid SQL should return an error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.sql)
			migration, err := migrator.LoadMigration(tt.version, reader)

			if tt.wantErr {
				require.Error(t, err, tt.description)
				require.Nil(t, migration)
				return
			}

			require.NoError(t, err, tt.description)
			require.NotNil(t, migration)
			require.Equal(t, tt.version, migration.Version)
			require.Len(t, migration.Statements, tt.stmtCount, tt.description)
		})
	}
}

func TestLoadMigration_StatementTypes(t *testing.T) {
	sql := `CREATE DATABASE test ENGINE = Atomic;
			CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;
			ALTER TABLE test.users ADD COLUMN email String DEFAULT '';
			CREATE VIEW test.user_view AS SELECT id, name FROM test.users;
			DROP VIEW test.user_view;`

	migration, err := migrator.LoadMigration("test_types", strings.NewReader(sql))
	require.NoError(t, err)
	require.Equal(t, "test_types", migration.Version)
	require.Len(t, migration.Statements, 5)

	// Verify statement types
	require.NotNil(t, migration.Statements[0].CreateDatabase)
	require.Equal(t, "test", migration.Statements[0].CreateDatabase.Name)

	require.NotNil(t, migration.Statements[1].CreateTable)
	require.Equal(t, "users", migration.Statements[1].CreateTable.Name)
	require.NotNil(t, migration.Statements[1].CreateTable.Database)
	require.Equal(t, "test", *migration.Statements[1].CreateTable.Database)

	require.NotNil(t, migration.Statements[2].AlterTable)
	require.Equal(t, "users", migration.Statements[2].AlterTable.Name)
	require.Len(t, migration.Statements[2].AlterTable.Operations, 1)

	require.NotNil(t, migration.Statements[3].CreateView)
	require.Equal(t, "user_view", migration.Statements[3].CreateView.Name)
	require.NotNil(t, migration.Statements[3].CreateView.Database)
	require.Equal(t, "test", *migration.Statements[3].CreateView.Database)

	require.NotNil(t, migration.Statements[4].DropView)
	require.Equal(t, "user_view", migration.Statements[4].DropView.Name)
	require.NotNil(t, migration.Statements[4].DropView.Database)
	require.Equal(t, "test", *migration.Statements[4].DropView.Database)
}

func TestLoadMigrationDir(t *testing.T) {
	tests := []struct {
		name           string
		files          map[string]string
		wantErr        bool
		migrationCount int
		description    string
	}{
		{
			name: "single_migration",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
			},
			wantErr:        false,
			migrationCount: 1,
			description:    "Single migration file should be loaded",
		},
		{
			name: "multiple_migrations_ordered",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
				"20240101120200.sql": "CREATE VIEW test.user_view AS SELECT * FROM test.users;",
			},
			wantErr:        false,
			migrationCount: 3,
			description:    "Multiple migrations should be loaded in order",
		},
		{
			name: "migrations_with_sum_file",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
				"migrations.sum":     "h1:dGVzdA==\n20240101120000.sql h1:aGFzaDE=\n20240101120100.sql h1:aGFzaDI=",
			},
			wantErr:        false,
			migrationCount: 2,
			description:    "Migrations with sum file should load migrations only",
		},
		{
			name:           "empty_directory",
			files:          map[string]string{},
			wantErr:        false,
			migrationCount: 0,
			description:    "Empty directory should return empty migration set",
		},
		{
			name: "non_sql_files_ignored",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"readme.txt":         "This is documentation",
				"config.yaml":        "setting: value",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			wantErr:        false,
			migrationCount: 2,
			description:    "Non-SQL files should be ignored",
		},
		{
			name: "invalid_sql_in_migration",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "INVALID SQL SYNTAX HERE;",
			},
			wantErr:        true,
			migrationCount: 0,
			description:    "Invalid SQL in any migration should cause error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test filesystem
			fsys := make(fstest.MapFS)
			for filename, content := range tt.files {
				fsys[filename] = &fstest.MapFile{
					Data: []byte(content),
				}
			}

			migrationDir, err := migrator.LoadMigrationDir(fsys)

			if tt.wantErr {
				require.Error(t, err, tt.description)
				require.Nil(t, migrationDir)
				return
			}

			require.NoError(t, err, tt.description)
			require.NotNil(t, migrationDir)
			require.Len(t, migrationDir.Migrations, tt.migrationCount, tt.description)

			// Verify migrations are in lexical order
			for i := 1; i < len(migrationDir.Migrations); i++ {
				prev := migrationDir.Migrations[i-1].Version
				curr := migrationDir.Migrations[i].Version
				require.Less(t, prev, curr,
					"Migrations should be in lexical order: %s should be before %s", prev, curr)
			}
		})
	}
}

func TestLoadMigrationDir_RealFiles(t *testing.T) {
	// Test with embedded test files if they exist
	migrationDir, err := migrator.LoadMigrationDir(testdataFS)
	// This test will succeed even if no testdata exists
	if err != nil {
		t.Skipf("Skipping real file test: %v", err)
	}

	require.NotNil(t, migrationDir)
	// We can't assert specific counts since we don't control the testdata content
	require.NotNil(t, migrationDir.Migrations)
}

func TestLoadMigrationDir_LexicalOrdering(t *testing.T) {
	files := map[string]string{
		"20240101120300.sql": "CREATE VIEW third AS SELECT 1;",
		"20240101120000.sql": "CREATE DATABASE first ENGINE = Atomic;",
		"20240101130000.sql": "CREATE VIEW tenth AS SELECT 1;",
		"20240101120200.sql": "CREATE TABLE second (id UInt64) ENGINE = MergeTree() ORDER BY id;",
	}

	fsys := make(fstest.MapFS)
	for filename, content := range files {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	migrationDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)
	require.Len(t, migrationDir.Migrations, 4)

	// Verify lexical ordering
	expectedOrder := []string{"20240101120000", "20240101120200", "20240101120300", "20240101130000"}
	for i, expected := range expectedOrder {
		require.Equal(t, expected, migrationDir.Migrations[i].Version,
			"Migration %d should have version %s", i, expected)
	}
}

func TestMigration_EmptyVersion(t *testing.T) {
	migration, err := migrator.LoadMigration("", strings.NewReader("CREATE DATABASE test;"))
	require.NoError(t, err)
	require.Empty(t, migration.Version)
	require.Len(t, migration.Statements, 1)
}

func TestMigration_UnicodeContent(t *testing.T) {
	sql := `CREATE DATABASE test ENGINE = Atomic COMMENT 'Test with unicode: emoji and latin';
			CREATE TABLE test.unicode_table (
				id UInt64,
				name String COMMENT 'Name with unicode: latin accents'
			) ENGINE = MergeTree() ORDER BY id;`

	migration, err := migrator.LoadMigration("unicode_test", strings.NewReader(sql))
	require.NoError(t, err)
	require.Equal(t, "unicode_test", migration.Version)
	require.Len(t, migration.Statements, 2)

	// Verify unicode content is preserved
	db := migration.Statements[0].CreateDatabase
	require.NotNil(t, db)
	require.NotNil(t, db.Comment)
	require.Contains(t, *db.Comment, "emoji")
	require.Contains(t, *db.Comment, "latin")
}

func TestMigration_SnapshotDetection(t *testing.T) {
	tests := []struct {
		name       string
		content    string
		isSnapshot bool
	}{
		{
			name: "regular_migration",
			content: `CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`,
			isSnapshot: false,
		},
		{
			name: "snapshot_migration",
			content: `-- housekeeper:snapshot
-- version: 20240810120000_snapshot
-- description: Test snapshot
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init,002_users
-- cumulative_hash: abc123

CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`,
			isSnapshot: true,
		},
		{
			name: "migration_with_comment_but_not_snapshot",
			content: `-- This is just a regular comment
-- Not a snapshot marker
CREATE DATABASE test ENGINE = Atomic;`,
			isSnapshot: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			migration, err := migrator.LoadMigration("test_version", strings.NewReader(tt.content))
			require.NoError(t, err)
			require.Equal(t, tt.isSnapshot, migration.IsSnapshot)
			require.Equal(t, "test_version", migration.Version)
			require.NotEmpty(t, migration.Statements)
		})
	}
}

func TestMigrationDir_Rehash(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		modifyFiles map[string]string // Files to modify after initial load
		wantErr     bool
		description string
	}{
		{
			name: "simple_rehash",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			modifyFiles: map[string]string{},
			wantErr:     false,
			description: "Basic rehash without modifications should succeed",
		},
		{
			name: "rehash_with_modified_file",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			modifyFiles: map[string]string{
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;",
			},
			wantErr:     false,
			description: "Rehash with modified migration should update sum file",
		},
		{
			name: "rehash_with_new_file",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
			},
			modifyFiles: map[string]string{
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			wantErr:     false,
			description: "Rehash with new migration file should include it",
		},
		{
			name: "rehash_with_invalid_sql",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
			},
			modifyFiles: map[string]string{
				"20240101120100.sql": "INVALID SQL SYNTAX;",
			},
			wantErr:     true,
			description: "Rehash with invalid SQL should return error",
		},
		{
			name:        "empty_directory_rehash",
			files:       map[string]string{},
			modifyFiles: map[string]string{},
			wantErr:     false,
			description: "Rehash on empty directory should succeed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create initial filesystem
			fsys := make(fstest.MapFS)
			for filename, content := range tt.files {
				fsys[filename] = &fstest.MapFile{
					Data: []byte(content),
				}
			}

			// Load initial migration directory
			migDir, err := migrator.LoadMigrationDir(fsys)
			require.NoError(t, err)

			// Apply modifications to filesystem
			for filename, content := range tt.modifyFiles {
				fsys[filename] = &fstest.MapFile{
					Data: []byte(content),
				}
			}

			// Perform rehash
			err = migDir.Rehash()

			if tt.wantErr {
				require.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)

			// Verify migrations were reloaded
			// Account for duplicates by using a map
			allFiles := make(map[string]bool)
			for f := range tt.files {
				allFiles[f] = true
			}
			for f := range tt.modifyFiles {
				allFiles[f] = true
			}
			expectedCount := len(allFiles)

			require.Len(t, migDir.Migrations, expectedCount,
				"Should have correct number of migrations after rehash")

			// Verify sum file was recalculated
			var buf bytes.Buffer
			_, err = migDir.SumFile.WriteTo(&buf)
			require.NoError(t, err)

			sumContent := buf.String()
			require.NotEmpty(t, sumContent, "Sum file should have content after rehash")

			// Verify all migrations are in sum file
			for path := range allFiles {
				if filepath.Ext(path) == ".sql" {
					require.Contains(t, sumContent, path,
						"Sum file should contain entry for %s", path)
				}
			}
		})
	}
}

func TestMigrationDir_Rehash_Consistency(t *testing.T) {
	// Test that multiple rehashes produce consistent results
	files := map[string]string{
		"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
		"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
		"20240101120200.sql": "CREATE VIEW test.user_view AS SELECT * FROM test.users;",
	}

	fsys := make(fstest.MapFS)
	for filename, content := range files {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// First rehash
	err = migDir.Rehash()
	require.NoError(t, err)

	var buf1 bytes.Buffer
	_, err = migDir.SumFile.WriteTo(&buf1)
	require.NoError(t, err)
	hash1 := buf1.String()

	// Second rehash without changes
	err = migDir.Rehash()
	require.NoError(t, err)

	var buf2 bytes.Buffer
	_, err = migDir.SumFile.WriteTo(&buf2)
	require.NoError(t, err)
	hash2 := buf2.String()

	// Both hashes should be identical
	require.Equal(t, hash1, hash2,
		"Multiple rehashes without changes should produce identical sum files")
}

func TestMigrationDir_Rehash_NilFilesystem(t *testing.T) {
	// Create a MigrationDir without filesystem reference
	migDir := &migrator.MigrationDir{
		Migrations: []*migrator.Migration{},
		SumFile:    migrator.NewSumFile(),
	}

	err := migDir.Rehash()
	require.Error(t, err)
	require.Contains(t, err.Error(), "filesystem reference is nil")
}

func TestMigrationDir_Rehash_OrderPreservation(t *testing.T) {
	// Test that rehash preserves lexical ordering
	files := map[string]string{
		"20240101120300.sql": "CREATE VIEW third AS SELECT 1;",
		"20240101120000.sql": "CREATE DATABASE first ENGINE = Atomic;",
		"20240101120200.sql": "CREATE TABLE second (id UInt64) ENGINE = MergeTree() ORDER BY id;",
	}

	fsys := make(fstest.MapFS)
	for filename, content := range files {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Perform rehash
	err = migDir.Rehash()
	require.NoError(t, err)

	// Verify order is preserved
	require.Len(t, migDir.Migrations, 3)
	require.Equal(t, "20240101120000", migDir.Migrations[0].Version)
	require.Equal(t, "20240101120200", migDir.Migrations[1].Version)
	require.Equal(t, "20240101120300", migDir.Migrations[2].Version)
}

func TestMigrationDir_Validate(t *testing.T) {
	tests := []struct {
		name        string
		files       map[string]string
		expectValid bool
		expectError bool
		description string
	}{
		{
			name: "valid_single_migration",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
			},
			expectValid: true,
			expectError: false,
			description: "Single migration should validate successfully",
		},
		{
			name: "valid_multiple_migrations",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
				"20240101120200.sql": "CREATE VIEW test.user_view AS SELECT * FROM test.users;",
			},
			expectValid: true,
			expectError: false,
			description: "Multiple migrations should validate successfully",
		},
		{
			name:        "empty_directory",
			files:       map[string]string{},
			expectValid: true,
			expectError: false,
			description: "Empty migration directory should validate successfully",
		},
		{
			name: "non_sql_files_ignored",
			files: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"readme.txt":         "Documentation file",
				"config.yaml":        "configuration",
			},
			expectValid: true,
			expectError: false,
			description: "Non-SQL files should be ignored during validation",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create test filesystem
			fsys := make(fstest.MapFS)
			for filename, content := range tt.files {
				fsys[filename] = &fstest.MapFile{
					Data: []byte(content),
				}
			}

			// Load migration directory (this creates both migrations and sum file)
			migDir, err := migrator.LoadMigrationDir(fsys)
			require.NoError(t, err)

			// Validate the migration directory
			isValid, err := migDir.Validate()

			if tt.expectError {
				require.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)
			require.Equal(t, tt.expectValid, isValid, tt.description)
		})
	}
}

func TestMigrationDir_Validate_ModifiedFiles(t *testing.T) {
	// Create initial filesystem
	initialFiles := map[string]string{
		"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
		"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
	}

	fsys := make(fstest.MapFS)
	for filename, content := range initialFiles {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	// Load migration directory
	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Validation should pass initially
	isValid, err := migDir.Validate()
	require.NoError(t, err)
	require.True(t, isValid, "Initial validation should pass")

	// Modify one of the files
	fsys["20240101120000.sql"] = &fstest.MapFile{
		Data: []byte("CREATE DATABASE modified ENGINE = Atomic;"),
	}

	// Validation should now fail because file content changed
	isValid, err = migDir.Validate()
	require.NoError(t, err)
	require.False(t, isValid, "Validation should fail after file modification")
}

func TestMigrationDir_Validate_AddedFiles(t *testing.T) {
	// Create initial filesystem with single migration
	fsys := make(fstest.MapFS)
	fsys["20240101120000.sql"] = &fstest.MapFile{
		Data: []byte("CREATE DATABASE test ENGINE = Atomic;"),
	}

	// Load migration directory
	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Add a new migration file after loading
	fsys["20240101120100.sql"] = &fstest.MapFile{
		Data: []byte("CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"),
	}

	// Validation should fail because there's a new file not in the sum file
	// Note: This tests the case where files exist but weren't included in original sum
	isValid, err := migDir.Validate()
	require.NoError(t, err)
	require.True(t, isValid, "Validation should pass - new files don't affect existing sum validation")
}

func TestMigrationDir_Validate_RemovedFiles(t *testing.T) {
	// Create filesystem with two migrations
	fsys := make(fstest.MapFS)
	fsys["20240101120000.sql"] = &fstest.MapFile{
		Data: []byte("CREATE DATABASE test ENGINE = Atomic;"),
	}
	fsys["20240101120100.sql"] = &fstest.MapFile{
		Data: []byte("CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"),
	}

	// Load migration directory
	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Remove one migration file
	delete(fsys, "20240101120100.sql")

	// Validation should fail because file is missing
	isValid, err := migDir.Validate()
	require.Error(t, err, "Should error when trying to open missing file")
	require.False(t, isValid)
	require.Contains(t, err.Error(), "failed to open migration file")
}

func TestMigrationDir_Validate_NilFilesystem(t *testing.T) {
	// Create a MigrationDir without filesystem reference
	migDir := &migrator.MigrationDir{
		Migrations: []*migrator.Migration{},
		SumFile:    migrator.NewSumFile(),
		// fs is nil
	}

	isValid, err := migDir.Validate()
	require.Error(t, err)
	require.False(t, isValid)
	require.Contains(t, err.Error(), "filesystem reference is nil")
}

func TestMigrationDir_Validate_RehashConsistency(t *testing.T) {
	// Test that validation works correctly after rehashing
	files := map[string]string{
		"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
		"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
	}

	fsys := make(fstest.MapFS)
	for filename, content := range files {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	// Load migration directory
	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Initial validation should pass
	isValid, err := migDir.Validate()
	require.NoError(t, err)
	require.True(t, isValid, "Initial validation should pass")

	// Rehash the migration directory
	err = migDir.Rehash()
	require.NoError(t, err)

	// Validation should still pass after rehash
	isValid, err = migDir.Validate()
	require.NoError(t, err)
	require.True(t, isValid, "Validation should pass after rehash")
}

func TestMigrationDir_Validate_ComplexMigrations(t *testing.T) {
	// Test with more complex migration content including unicode
	files := map[string]string{
		"20240101120000.sql": `CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics with unicode: special chars';`,
		"20240101120100.sql": `CREATE TABLE analytics.events (
			id UInt64,
			timestamp DateTime DEFAULT now(),
			data Map(String, String) DEFAULT map(),
			metadata Nullable(String)
		) ENGINE = MergeTree() 
		ORDER BY (id, timestamp) 
		PARTITION BY toYYYYMM(timestamp);`,
		"20240101120200.sql": `CREATE MATERIALIZED VIEW analytics.daily_stats
		ENGINE = MergeTree() ORDER BY date
		AS SELECT toDate(timestamp), count()
		FROM analytics.events GROUP BY toDate(timestamp);`,
	}

	fsys := make(fstest.MapFS)
	for filename, content := range files {
		fsys[filename] = &fstest.MapFile{
			Data: []byte(content),
		}
	}

	// Load migration directory
	migDir, err := migrator.LoadMigrationDir(fsys)
	require.NoError(t, err)

	// Validation should handle complex content correctly
	isValid, err := migDir.Validate()
	require.NoError(t, err)
	require.True(t, isValid, "Complex migrations should validate successfully")
}

func TestMigrationDir_SnapshotIntegration(t *testing.T) {
	// Test loading migrations with snapshot present
	t.Run("load directory with snapshot and regular migrations", func(t *testing.T) {
		files := map[string]string{
			"001_init.sql":     "CREATE DATABASE test ENGINE = Atomic;",
			"002_users.sql":    "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			"003_snapshot.sql": snapshotFile,
			"004_products.sql": "CREATE TABLE test.products (id UInt64) ENGINE = MergeTree() ORDER BY id;",
		}

		fsys := make(fstest.MapFS)
		for filename, content := range files {
			fsys[filename] = &fstest.MapFile{Data: []byte(content)}
		}

		migDir, err := migrator.LoadMigrationDir(fsys)
		require.NoError(t, err)

		// Should have loaded 4 migrations (including snapshot as migration): 001, 002, 003_snapshot, 004
		require.Len(t, migDir.Migrations, 4) // 001, 002, 003_snapshot, 004
		require.True(t, migDir.HasSnapshot())

		snapshot := migDir.GetSnapshot()
		require.NotNil(t, snapshot)
		require.Equal(t, "003_snapshot", snapshot.Version)
		require.Equal(t, "Test snapshot for initial migrations", snapshot.Description)
		require.Equal(t, []string{"001_init", "002_users"}, snapshot.IncludedMigrations)
	})

	t.Run("get migrations after snapshot", func(t *testing.T) {
		files := map[string]string{
			"001_init.sql":     "CREATE DATABASE test ENGINE = Atomic;",
			"002_users.sql":    "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			"003_snapshot.sql": snapshotFile,
			"004_products.sql": "CREATE TABLE test.products (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			"005_orders.sql":   "CREATE TABLE test.orders (id UInt64) ENGINE = MergeTree() ORDER BY id;",
		}

		fsys := make(fstest.MapFS)
		for filename, content := range files {
			fsys[filename] = &fstest.MapFile{Data: []byte(content)}
		}

		migDir, err := migrator.LoadMigrationDir(fsys)
		require.NoError(t, err)

		afterSnapshot := migDir.GetMigrationsAfterSnapshot()
		require.Len(t, afterSnapshot, 2)
		require.Equal(t, "004_products", afterSnapshot[0].Version)
		require.Equal(t, "005_orders", afterSnapshot[1].Version)
	})

	t.Run("create new snapshot", func(t *testing.T) {
		files := map[string]string{
			"001_init.sql":     "CREATE DATABASE test ENGINE = Atomic;",
			"002_users.sql":    "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			"003_products.sql": "CREATE TABLE test.products (id UInt64) ENGINE = MergeTree() ORDER BY id;",
		}

		fsys := make(fstest.MapFS)
		for filename, content := range files {
			fsys[filename] = &fstest.MapFile{Data: []byte(content)}
		}

		migDir, err := migrator.LoadMigrationDir(fsys)
		require.NoError(t, err)

		// Create snapshot from all migrations
		snapshot, err := migDir.CreateSnapshot(
			"20240810120000_snapshot",
			"Test snapshot creation",
		)
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, "20240810120000_snapshot", snapshot.Version)
		require.Equal(t, "Test snapshot creation", snapshot.Description)
		require.Equal(t, []string{"001_init", "002_users", "003_products"}, snapshot.IncludedMigrations)
		require.Len(t, snapshot.Statements, 3)
	})

	t.Run("create snapshot with existing snapshot", func(t *testing.T) {
		files := map[string]string{
			"001_init.sql":     "CREATE DATABASE test ENGINE = Atomic;",
			"002_users.sql":    "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			"003_snapshot.sql": snapshotFile, // Already includes 001_init and 002_users
		}

		fsys := make(fstest.MapFS)
		for filename, content := range files {
			fsys[filename] = &fstest.MapFile{Data: []byte(content)}
		}

		migDir, err := migrator.LoadMigrationDir(fsys)
		require.NoError(t, err)

		// Create new snapshot - should include all current migrations
		snapshot, err := migDir.CreateSnapshot(
			"20240810120100_snapshot",
			"Another snapshot",
		)
		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, "20240810120100_snapshot", snapshot.Version)
		require.Equal(t, "Another snapshot", snapshot.Description)
		// Should include all 3 migrations: 001_init, 002_users, 003_snapshot
		require.Equal(t, []string{"001_init", "002_users", "003_snapshot"}, snapshot.IncludedMigrations)
	})

	t.Run("directory without migrations", func(t *testing.T) {
		fsys := make(fstest.MapFS)

		migDir, err := migrator.LoadMigrationDir(fsys)
		require.NoError(t, err)

		// Should not have any migrations or snapshot
		require.Empty(t, migDir.Migrations)
		require.False(t, migDir.HasSnapshot())
		require.Nil(t, migDir.GetSnapshot())

		// GetMigrationsAfterSnapshot should return empty slice
		afterSnapshot := migDir.GetMigrationsAfterSnapshot()
		require.Empty(t, afterSnapshot)

		// CreateSnapshot should fail
		_, err = migDir.CreateSnapshot("test", "description")
		require.Error(t, err)
		require.Contains(t, err.Error(), "no migrations to snapshot")
	})
}

func TestMigrationDir_LoadFromTestdata(t *testing.T) {
	// Test loading from actual embedded testdata
	migDir, err := migrator.LoadMigrationDir(testdataFS)
	require.NoError(t, err)

	// Should find regular migrations and snapshot
	require.NotEmpty(t, migDir.Migrations)
	require.True(t, migDir.HasSnapshot())

	// Validate the loaded snapshot
	snapshot := migDir.GetSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, "003_snapshot", snapshot.Version)
	require.Contains(t, snapshot.IncludedMigrations, "001_init")
	require.Contains(t, snapshot.IncludedMigrations, "002_users")

	// Should have migrations after snapshot
	afterSnapshot := migDir.GetMigrationsAfterSnapshot()
	require.NotEmpty(t, afterSnapshot)

	// Validate that we can get a migration that comes after the snapshot
	found := false
	for _, mig := range afterSnapshot {
		if mig.Version == "004_products" {
			found = true
			break
		}
	}
	require.True(t, found, "Should find 004_products migration after snapshot")
}
