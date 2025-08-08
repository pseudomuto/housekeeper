package project_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
)

const (
	dirPerm  = os.FileMode(0o755)
	filePerm = os.FileMode(0o644)
)

func TestLoadMigrationSet(t *testing.T) {
	t.Run("loads migration files in lexicographical order", func(t *testing.T) {
		// Use shared testdata - get absolute path
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)

		// Verify files are loaded and in lexicographical order
		files := ms.Files()
		require.Len(t, files, 3)

		// Check that files are in correct lexicographical order
		require.True(t, strings.HasSuffix(files[0], "001_init.sql"))
		require.True(t, strings.HasSuffix(files[1], "002_users_table.sql"))
		require.True(t, strings.HasSuffix(files[2], "003_products_table.sql"))

		// Verify all file paths are absolute
		for _, file := range files {
			require.True(t, filepath.IsAbs(file))
		}
	})

	t.Run("loads sum file when present", func(t *testing.T) {
		// Use shared testdata - staging environment has a sum file
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("staging")
		require.NoError(t, err)
		require.NotNil(t, ms)
		require.NotNil(t, ms.Sum())

		// Verify sum file was loaded with correct number of files
		require.Equal(t, 1, ms.Sum().Files())

		// Verify migration files were also loaded (excluding sum file)
		require.Len(t, ms.Files(), 1)
		require.True(t, strings.HasSuffix(ms.Files()[0], "001_staging_init.sql"))
	})

	t.Run("handles case insensitive sum file name", func(t *testing.T) {
		// Create temporary directory structure
		tempDir := t.TempDir()

		// Create housekeeper.yaml with test environment
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create sum file with different case
		sumFile := migrator.NewSumFile()
		sumFile.AddFile("test.sql", []byte("CREATE TABLE test (id UInt64);"))

		sumFilePath := filepath.Join(migrationsDir, "HOUSEKEEPER.SUM")
		file, err := os.Create(sumFilePath)
		require.NoError(t, err)
		defer file.Close()

		_, err = sumFile.WriteTo(file)
		require.NoError(t, err)

		// Initialize project and load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)
		require.NotNil(t, ms.Sum())
		require.Equal(t, 1, ms.Sum().Files())
	})

	t.Run("skips directories", func(t *testing.T) {
		// Create temporary directory structure
		tempDir := t.TempDir()

		// Create housekeeper.yaml with test environment
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create subdirectory that should be skipped
		err = os.MkdirAll(filepath.Join(migrationsDir, "subdir"), dirPerm)
		require.NoError(t, err)

		// Create migration file
		err = os.WriteFile(filepath.Join(migrationsDir, "001_init.sql"), []byte("CREATE DATABASE test;"), filePerm)
		require.NoError(t, err)

		// Initialize project and load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)

		// Should only find the one SQL file, not the directory
		require.Len(t, ms.Files(), 1)
		require.True(t, strings.HasSuffix(ms.Files()[0], "001_init.sql"))
	})

	t.Run("returns error for non-existent environment", func(t *testing.T) {
		// Use shared testdata
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Try to load migration set for non-existent environment
		_, err = proj.LoadMigrationSet("nonexistent")
		require.Error(t, err)
		require.Contains(t, err.Error(), "Env not found: nonexistent")
	})

	t.Run("returns error for non-existent migrations directory", func(t *testing.T) {
		// Create temporary directory structure
		tempDir := t.TempDir()

		// Create housekeeper.yaml with test environment pointing to non-existent directory
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: nonexistent/migrations`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Initialize project
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Try to load migration set for directory that doesn't exist
		_, err = proj.LoadMigrationSet("test")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read dir:")
	})

	t.Run("returns error for invalid sum file", func(t *testing.T) {
		// Create temporary directory structure
		tempDir := t.TempDir()

		// Create housekeeper.yaml with test environment
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create invalid sum file
		sumFilePath := filepath.Join(migrationsDir, "housekeeper.sum")
		err = os.WriteFile(sumFilePath, []byte("invalid sum file content"), filePerm)
		require.NoError(t, err)

		// Initialize project
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// Try to load migration set with invalid sum file
		_, err = proj.LoadMigrationSet("test")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to load sum file:")
	})

	t.Run("loads empty migration set for environment with no migrations", func(t *testing.T) {
		// Use shared testdata - imports environment has no migrations directory
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		// This should fail since the migrations directory doesn't exist
		_, err = proj.LoadMigrationSet("imports")
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read dir:")
	})
}

func TestMigrationSet_GenerateSumFile(t *testing.T) {
	t.Run("generates sum file from migration files", func(t *testing.T) {
		// Use shared testdata - test environment has 3 migration files
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)

		// Generate sum file from the loaded migration files
		generatedSum, err := ms.GenerateSumFile()
		require.NoError(t, err)
		require.NotNil(t, generatedSum)

		// Should have 3 files in the generated sum
		require.Equal(t, 3, generatedSum.Files())

		// TotalHash should be computed after triggering it via WriteTo
		var buf strings.Builder
		_, err = generatedSum.WriteTo(&buf)
		require.NoError(t, err)
		require.NotEmpty(t, generatedSum.TotalHash)
	})

	t.Run("handles empty migration set", func(t *testing.T) {
		// Create empty migration set
		ms := &project.MigrationSet{}

		generatedSum, err := ms.GenerateSumFile()
		require.NoError(t, err)
		require.NotNil(t, generatedSum)

		// Should have 0 files
		require.Equal(t, 0, generatedSum.Files())

		// TotalHash should be computed after triggering it via WriteTo
		var buf strings.Builder
		_, err = generatedSum.WriteTo(&buf)
		require.NoError(t, err)
		// For empty sum files, TotalHash is empty string by design
		require.Empty(t, generatedSum.TotalHash)
	})

	t.Run("returns error for non-existent file", func(t *testing.T) {
		// Create temporary project with invalid migration directory
		tempDir := t.TempDir()

		// Create housekeeper.yaml pointing to non-existent migration files
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory but with a file that will be deleted
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create a migration file then delete it after loading the migration set
		migrationPath := filepath.Join(migrationsDir, "001_test.sql")
		err = os.WriteFile(migrationPath, []byte("CREATE TABLE test;"), filePerm)
		require.NoError(t, err)

		// Load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)

		// Now delete the file to simulate error condition
		err = os.Remove(migrationPath)
		require.NoError(t, err)

		// GenerateSumFile should now fail
		_, err = ms.GenerateSumFile()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to read migration file:")
	})
}

func TestMigrationSet_IsValid(t *testing.T) {
	t.Run("returns true for valid migration set", func(t *testing.T) {
		// Create a fresh environment with matching sum file
		tempDir := t.TempDir()

		// Create housekeeper.yaml
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create a migration file
		migrationContent := "CREATE DATABASE test_db ENGINE = Atomic;"
		err = os.WriteFile(filepath.Join(migrationsDir, "001_init.sql"), []byte(migrationContent), filePerm)
		require.NoError(t, err)

		// Generate sum file that matches the migration
		sumFile := migrator.NewSumFile()
		sumFile.AddFile("001_init.sql", []byte(migrationContent))

		sumFilePath := filepath.Join(migrationsDir, "housekeeper.sum")
		file, err := os.Create(sumFilePath)
		require.NoError(t, err)
		defer file.Close()

		_, err = sumFile.WriteTo(file)
		require.NoError(t, err)

		// Load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)
		require.NotNil(t, ms.Sum())

		// Validation should pass (matching content)
		isValid, err := ms.IsValid()
		require.NoError(t, err)
		require.True(t, isValid)
	})

	t.Run("returns false when no sum file loaded", func(t *testing.T) {
		// Use test environment which has no sum file
		testdataPath, err := filepath.Abs("testdata")
		require.NoError(t, err)

		proj := project.New(testdataPath)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)
		require.Nil(t, ms.Sum()) // Should have no sum file

		// Validation should return false (no loaded sum file)
		isValid, err := ms.IsValid()
		require.NoError(t, err)
		require.False(t, isValid)
	})

	t.Run("returns false for mismatched hashes", func(t *testing.T) {
		// Create temporary migration set with mismatched sum file
		tempDir := t.TempDir()

		// Create housekeeper.yaml
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create a migration file
		migrationContent := "CREATE DATABASE test_db;"
		err = os.WriteFile(filepath.Join(migrationsDir, "001_init.sql"), []byte(migrationContent), filePerm)
		require.NoError(t, err)

		// Create a sum file with different content (mismatched hash)
		wrongSumFile := migrator.NewSumFile()
		wrongSumFile.AddFile("001_init.sql", []byte("CREATE DATABASE wrong_db;"))

		sumFilePath := filepath.Join(migrationsDir, "housekeeper.sum")
		file, err := os.Create(sumFilePath)
		require.NoError(t, err)
		defer file.Close()

		_, err = wrongSumFile.WriteTo(file)
		require.NoError(t, err)

		// Load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms)
		require.NotNil(t, ms.Sum())

		// Validation should return false (mismatched hashes)
		isValid, err := ms.IsValid()
		require.NoError(t, err)
		require.False(t, isValid)
	})

	t.Run("returns error when cannot generate sum file", func(t *testing.T) {
		// Create migration set with files that get deleted after loading
		tempDir := t.TempDir()

		// Create housekeeper.yaml
		configContent := `environments:
  - name: test
    entrypoint: db/main.sql
    dir: migrations/test`

		err := os.WriteFile(filepath.Join(tempDir, "housekeeper.yaml"), []byte(configContent), filePerm)
		require.NoError(t, err)

		// Create migrations directory
		migrationsDir := filepath.Join(tempDir, "migrations", "test")
		err = os.MkdirAll(migrationsDir, dirPerm)
		require.NoError(t, err)

		// Create migration file and sum file
		migrationPath := filepath.Join(migrationsDir, "001_init.sql")
		err = os.WriteFile(migrationPath, []byte("CREATE TABLE test;"), filePerm)
		require.NoError(t, err)

		// Create a sum file (content doesn't matter for this error test)
		sumFile := migrator.NewSumFile()
		sumFile.AddFile("001_init.sql", []byte("CREATE TABLE test;"))

		sumFilePath := filepath.Join(migrationsDir, "housekeeper.sum")
		file, err := os.Create(sumFilePath)
		require.NoError(t, err)
		defer file.Close()

		_, err = sumFile.WriteTo(file)
		require.NoError(t, err)

		// Load migration set
		proj := project.New(tempDir)
		err = proj.Initialize(project.InitOptions{})
		require.NoError(t, err)

		ms, err := proj.LoadMigrationSet("test")
		require.NoError(t, err)
		require.NotNil(t, ms.Sum()) // Should have loaded sum file

		// Delete the migration file to cause error in validation
		err = os.Remove(migrationPath)
		require.NoError(t, err)

		// Validation should return error
		_, err = ms.IsValid()
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to generate sum file for validation")
	})
}
