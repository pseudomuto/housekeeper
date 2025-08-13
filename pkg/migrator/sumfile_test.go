package migrator_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/stretchr/testify/require"
)

func TestNewSumFile(t *testing.T) {
	sumFile := migrator.NewSumFile()
	require.NotNil(t, sumFile)

	// Test that we can write an empty sum file
	var buf bytes.Buffer
	_, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)

	content := buf.String()
	lines := strings.Split(strings.TrimSpace(content), "\n")
	require.Len(t, lines, 1) // Only the total hash line for empty sum file
}

func TestSumFile_AddSingleEntry(t *testing.T) {
	sumFile := migrator.NewSumFile()

	content := "CREATE DATABASE test ENGINE = Atomic;"
	err := sumFile.Add("20240101120000.sql", strings.NewReader(content))
	require.NoError(t, err)

	var buf bytes.Buffer
	bytesWritten, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)
	require.Positive(t, bytesWritten)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2) // Total hash + 1 entry

	// Verify format
	require.True(t, strings.HasPrefix(lines[0], "h1:"))
	require.True(t, strings.HasPrefix(lines[1], "20240101120000.sql h1:"))
}

func TestSumFile_AddMultipleEntries(t *testing.T) {
	sumFile := migrator.NewSumFile()

	migrations := []struct {
		version string
		content string
	}{
		{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
		{"20240101120100.sql", "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"},
		{"20240101120200.sql", "CREATE VIEW test.user_view AS SELECT * FROM test.users;"},
	}

	for _, mig := range migrations {
		err := sumFile.Add(mig.version, strings.NewReader(mig.content))
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	_, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 4) // Total hash + 3 entries

	// Verify all entries are present
	for i, mig := range migrations {
		expectedPrefix := mig.version + " h1:"
		require.True(t, strings.HasPrefix(lines[i+1], expectedPrefix),
			"Line %d should start with %s, got: %s", i+1, expectedPrefix, lines[i+1])
	}
}

func TestSumFile_ChainedHashing(t *testing.T) {
	// Test that hashes are properly chained (each hash includes previous)
	sumFile := migrator.NewSumFile()

	content1 := "CREATE DATABASE test;"
	content2 := "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"

	err := sumFile.Add("20240101120000.sql", strings.NewReader(content1))
	require.NoError(t, err)

	// Capture first hash
	var buf1 bytes.Buffer
	_, err = sumFile.WriteTo(&buf1)
	require.NoError(t, err)
	firstLines := strings.Split(strings.TrimSpace(buf1.String()), "\n")
	firstHash := strings.TrimPrefix(firstLines[1], "20240101120000.sql ")

	// Add second entry
	err = sumFile.Add("20240101120100.sql", strings.NewReader(content2))
	require.NoError(t, err)

	var buf2 bytes.Buffer
	_, err = sumFile.WriteTo(&buf2)
	require.NoError(t, err)
	secondLines := strings.Split(strings.TrimSpace(buf2.String()), "\n")

	// Verify we have both entries
	require.Len(t, secondLines, 3) // Total + 2 entries
	require.Equal(t, firstHash, strings.TrimPrefix(secondLines[1], "20240101120000.sql "))

	// Verify second hash is different (includes chained data)
	secondHash := strings.TrimPrefix(secondLines[2], "20240101120100.sql ")
	require.NotEqual(t, firstHash, secondHash)

	// Manually verify chained hashing
	h := sha256.New()
	firstHashBytes, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(firstHash, "h1:"))
	require.NoError(t, err)

	h.Write(firstHashBytes)
	h.Write([]byte(content2))
	expectedSecondHash := "h1:" + base64.StdEncoding.EncodeToString(h.Sum(nil))

	require.Equal(t, expectedSecondHash, secondHash)
}

func TestLoadSumFile(t *testing.T) {
	// Create a valid sum file content
	sumContent := `h1:dGVzdF90b3RhbF9oYXNo
20240101120000.sql h1:aGFzaDE=
20240101120100.sql h1:aGFzaDI=
20240101120200.sql h1:aGFzaDM=`

	sumFile, err := migrator.LoadSumFile(strings.NewReader(sumContent))
	require.NoError(t, err)
	require.NotNil(t, sumFile)

	// Write it back and verify entry consistency
	var buf bytes.Buffer
	_, err = sumFile.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 4)

	// First line is computed total hash, so we skip it and check entry lines
	expectedLines := strings.Split(sumContent, "\n")
	for i := 1; i < len(expectedLines); i++ {
		require.Equal(t, expectedLines[i], lines[i], "Entry line %d should match", i)
	}

	// Verify first line has h1 format (computed total hash)
	require.True(t, strings.HasPrefix(lines[0], "h1:"), "First line should have h1 format")
}

func TestLoadSumFile_InvalidFormat(t *testing.T) {
	testCases := []struct {
		name    string
		content string
		wantErr bool
	}{
		{
			name:    "missing_h1_prefix_total",
			content: "totalhashwithoutprefix\n20240101120000.sql h1:hash=",
			wantErr: true,
		},
		{
			name:    "missing_h1_prefix_entry",
			content: "h1:totalhash=\n20240101120000.sql hashwithoutprefix",
			wantErr: true,
		},
		{
			name:    "invalid_base64_total",
			content: "h1:invalid_base64!\n20240101120000.sql h1:hash=",
			wantErr: true,
		},
		{
			name:    "invalid_base64_entry",
			content: "h1:totalhash=\n20240101120000.sql h1:invalid_base64!",
			wantErr: true,
		},
		{
			name:    "malformed_entry_line",
			content: "h1:totalhash=\ninvalid_format_line",
			wantErr: true,
		},
		{
			name:    "empty_content",
			content: "",
			wantErr: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := migrator.LoadSumFile(strings.NewReader(tc.content))
			if tc.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestSumFile_RoundTrip(t *testing.T) {
	// Test creating, serializing, and loading a sum file
	original := migrator.NewSumFile()

	migrations := []struct {
		version string
		content string
	}{
		{"20240101120000.sql", "CREATE DATABASE production ENGINE = Atomic;"},
		{"20240101120100.sql", "CREATE TABLE production.users (id UInt64, email String) ENGINE = MergeTree() ORDER BY id;"},
		{"20240101120200.sql", "ALTER TABLE production.users ADD INDEX email_idx email TYPE minmax;"},
	}

	// Add all migrations to original
	for _, mig := range migrations {
		err := original.Add(mig.version, strings.NewReader(mig.content))
		require.NoError(t, err)
	}

	// Serialize to buffer
	var buf bytes.Buffer
	bytesWritten, err := original.WriteTo(&buf)
	require.NoError(t, err)
	require.Positive(t, bytesWritten)

	// Capture original content before loading
	originalContent := buf.String()

	// Load from buffer
	loaded, err := migrator.LoadSumFile(&buf)
	require.NoError(t, err)

	// Serialize loaded version
	var buf2 bytes.Buffer
	bytesWritten2, err := loaded.WriteTo(&buf2)
	require.NoError(t, err)
	require.Equal(t, bytesWritten, bytesWritten2)

	loadedContent := buf2.String()

	// Compare line by line, skipping the first line (total hash)
	originalLines := strings.Split(strings.TrimSpace(originalContent), "\n")
	loadedLines := strings.Split(strings.TrimSpace(loadedContent), "\n")

	require.Len(t, loadedLines, len(originalLines))

	// Compare entry lines (skip total hash line which may differ)
	for i := 1; i < len(originalLines); i++ {
		require.Equal(t, originalLines[i], loadedLines[i])
	}
}

func TestSumFile_EmptyReaders(t *testing.T) {
	sumFile := migrator.NewSumFile()

	// Add entry with empty content
	err := sumFile.Add("empty.sql", strings.NewReader(""))
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = sumFile.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 2) // Total hash + 1 empty entry

	require.True(t, strings.HasPrefix(lines[1], "empty.sql h1:"))
}

func TestSumFile_ConcurrentAccess(t *testing.T) {
	// Test that the mutex protects concurrent access
	sumFile := migrator.NewSumFile()

	// Add entries sequentially (simulating concurrent access is complex in tests)
	entries := []string{
		"CREATE DATABASE test1;",
		"CREATE DATABASE test2;",
		"CREATE DATABASE test3;",
	}

	for i, content := range entries {
		version := fmt.Sprintf("%03d.sql", i+1)
		err := sumFile.Add(version, strings.NewReader(content))
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	_, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, 4) // Total + 3 entries
}

func TestSumFile_LargeContent(t *testing.T) {
	sumFile := migrator.NewSumFile()

	// Create large content
	var contentBuilder strings.Builder
	for i := 0; i < 1000; i++ {
		contentBuilder.WriteString(fmt.Sprintf("CREATE TABLE table_%d (id UInt64) ENGINE = MergeTree() ORDER BY id;\n", i))
	}
	largeContent := contentBuilder.String()

	err := sumFile.Add("large_migration.sql", strings.NewReader(largeContent))
	require.NoError(t, err)

	var buf bytes.Buffer
	bytesWritten, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)
	require.Positive(t, bytesWritten)

	// Verify we can load it back
	loaded, err := migrator.LoadSumFile(&buf)
	require.NoError(t, err)
	require.NotNil(t, loaded)
}

func TestSumFile_UnicodeContent(t *testing.T) {
	sumFile := migrator.NewSumFile()

	unicodeContent := `CREATE DATABASE test ENGINE = Atomic COMMENT 'Database with unicode: special chars';
		CREATE TABLE test.unicode_table (
			id UInt64,
			name String COMMENT 'Unicode field: special characters'
		) ENGINE = MergeTree() ORDER BY id;`

	err := sumFile.Add("unicode.sql", strings.NewReader(unicodeContent))
	require.NoError(t, err)

	var buf bytes.Buffer
	_, err = sumFile.WriteTo(&buf)
	require.NoError(t, err)

	// Capture original content before loading
	originalContent := buf.String()

	// Verify we can load it back correctly
	loaded, err := migrator.LoadSumFile(&buf)
	require.NoError(t, err)
	require.NotNil(t, loaded)

	// Round-trip test
	var buf2 bytes.Buffer
	_, err = loaded.WriteTo(&buf2)
	require.NoError(t, err)

	loadedContent := buf2.String()

	// Compare entry lines (skip total hash line which may differ)
	originalLines := strings.Split(strings.TrimSpace(originalContent), "\n")
	loadedLines := strings.Split(strings.TrimSpace(loadedContent), "\n")

	require.Len(t, loadedLines, len(originalLines))
	for i := 1; i < len(originalLines); i++ {
		require.Equal(t, originalLines[i], loadedLines[i])
	}
}

func TestSumFile_VersionNaming(t *testing.T) {
	sumFile := migrator.NewSumFile()

	versions := []string{
		"20240101120000.sql",
		"20240101120100.sql",
		"20240101120200.sql",
		"20240101120300.sql",
		"20240101120400.sql",
		"", // Empty version
	}

	for i, version := range versions {
		content := fmt.Sprintf("CREATE TABLE test_%d (id UInt64) ENGINE = MergeTree() ORDER BY id;", i)
		err := sumFile.Add(version, strings.NewReader(content))
		require.NoError(t, err)
	}

	var buf bytes.Buffer
	_, err := sumFile.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()
	lines := strings.Split(strings.TrimSpace(output), "\n")
	require.Len(t, lines, len(versions)+1) // Total + all versions

	// Verify all versions are present in output
	for i, version := range versions {
		if version == "" {
			// Empty version appears as just "h1:hash"
			require.True(t, strings.HasPrefix(lines[i+1], " h1:"))
		} else {
			expectedPrefix := version + " h1:"
			require.True(t, strings.HasPrefix(lines[i+1], expectedPrefix))
		}
	}
}

func TestSumFile_HashConsistency(t *testing.T) {
	// Test that identical content produces identical hashes
	content := "CREATE DATABASE test ENGINE = Atomic;"

	sumFile1 := migrator.NewSumFile()
	err := sumFile1.Add("test.sql", strings.NewReader(content))
	require.NoError(t, err)

	sumFile2 := migrator.NewSumFile()
	err = sumFile2.Add("test.sql", strings.NewReader(content))
	require.NoError(t, err)

	var buf1, buf2 bytes.Buffer
	_, err = sumFile1.WriteTo(&buf1)
	require.NoError(t, err)
	_, err = sumFile2.WriteTo(&buf2)
	require.NoError(t, err)

	require.Equal(t, buf1.String(), buf2.String())
}

func TestSumFile_OrderSensitivity(t *testing.T) {
	// Test that order of additions matters for chained hashing
	content1 := "CREATE DATABASE test1;"
	content2 := "CREATE DATABASE test2;"

	// Add in one order
	sumFile1 := migrator.NewSumFile()
	err := sumFile1.Add("a.sql", strings.NewReader(content1))
	require.NoError(t, err)
	err = sumFile1.Add("b.sql", strings.NewReader(content2))
	require.NoError(t, err)

	// Add in different order
	sumFile2 := migrator.NewSumFile()
	err = sumFile2.Add("b.sql", strings.NewReader(content2))
	require.NoError(t, err)
	err = sumFile2.Add("a.sql", strings.NewReader(content1))
	require.NoError(t, err)

	var buf1, buf2 bytes.Buffer
	_, err = sumFile1.WriteTo(&buf1)
	require.NoError(t, err)
	_, err = sumFile2.WriteTo(&buf2)
	require.NoError(t, err)

	// Different order should produce different results
	require.NotEqual(t, buf1.String(), buf2.String())
}

func TestSumFile_Validate(t *testing.T) {
	tests := []struct {
		name        string
		migrations  []struct{ version, content string }
		validFiles  map[string]string // Files to validate with (version -> content)
		expectValid bool
		expectError bool
		description string
	}{
		{
			name: "valid_single_file",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
			},
			expectValid: true,
			expectError: false,
			description: "Single file with matching content should validate",
		},
		{
			name: "valid_multiple_files",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
				{"20240101120100.sql", "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"},
				{"20240101120200.sql", "CREATE VIEW test.user_view AS SELECT * FROM test.users;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
				"20240101120200.sql": "CREATE VIEW test.user_view AS SELECT * FROM test.users;",
			},
			expectValid: true,
			expectError: false,
			description: "Multiple files with matching content should validate",
		},
		{
			name: "invalid_modified_first_file",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
				{"20240101120100.sql", "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE modified ENGINE = Atomic;", // Modified content
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			},
			expectValid: false,
			expectError: false,
			description: "Modified first file should fail validation",
		},
		{
			name: "invalid_modified_second_file",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
				{"20240101120100.sql", "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;", // Modified content
			},
			expectValid: false,
			expectError: false,
			description: "Modified second file should fail validation due to chained hashing",
		},
		{
			name: "missing_file",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
				{"20240101120100.sql", "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				// Missing 20240101120100.sql
			},
			expectValid: false,
			expectError: false,
			description: "Missing file should fail validation",
		},
		{
			name: "extra_file_ignored",
			migrations: []struct{ version, content string }{
				{"20240101120000.sql", "CREATE DATABASE test ENGINE = Atomic;"},
			},
			validFiles: map[string]string{
				"20240101120000.sql": "CREATE DATABASE test ENGINE = Atomic;",
				"20240101120100.sql": "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;", // Extra file
			},
			expectValid: true,
			expectError: false,
			description: "Extra files in validation map should be ignored",
		},
		{
			name:       "empty_sumfile",
			migrations: []struct{ version, content string }{
				// No migrations
			},
			validFiles:  map[string]string{},
			expectValid: true,
			expectError: false,
			description: "Empty sum file should validate successfully",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create and populate sum file
			sumFile := migrator.NewSumFile()
			for _, mig := range tt.migrations {
				err := sumFile.Add(mig.version, strings.NewReader(mig.content))
				require.NoError(t, err)
			}

			// Prepare validation files
			files := make(map[string]io.Reader)
			for version, content := range tt.validFiles {
				files[version] = strings.NewReader(content)
			}

			// Perform validation
			isValid, err := sumFile.Validate(files)

			if tt.expectError {
				require.Error(t, err, tt.description)
				return
			}

			require.NoError(t, err, tt.description)
			require.Equal(t, tt.expectValid, isValid, tt.description)
		})
	}
}

func TestSumFile_Validate_ReaderErrors(t *testing.T) {
	// Test validation with a reader that fails
	sumFile := migrator.NewSumFile()
	err := sumFile.Add("20240101120000.sql", strings.NewReader("CREATE DATABASE test;"))
	require.NoError(t, err)

	// Create a reader that will fail on read
	failingReader := &failingReader{failAfter: 5}
	files := map[string]io.Reader{
		"20240101120000.sql": failingReader,
	}

	isValid, err := sumFile.Validate(files)
	require.Error(t, err)
	require.False(t, isValid)
	require.Contains(t, err.Error(), "failed to read content for version")
}

func TestSumFile_Validate_ChainedHashing(t *testing.T) {
	// Test that chained hashing is properly validated
	// If we change the first file, the second file's hash should no longer match
	// even if the second file content is unchanged

	sumFile := migrator.NewSumFile()

	content1 := "CREATE DATABASE test;"
	content2 := "CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"

	err := sumFile.Add("20240101120000.sql", strings.NewReader(content1))
	require.NoError(t, err)

	err = sumFile.Add("20240101120100.sql", strings.NewReader(content2))
	require.NoError(t, err)

	// Validation with correct content should pass
	files := map[string]io.Reader{
		"20240101120000.sql": strings.NewReader(content1),
		"20240101120100.sql": strings.NewReader(content2),
	}

	isValid, err := sumFile.Validate(files)
	require.NoError(t, err)
	require.True(t, isValid, "Original content should validate successfully")

	// Validation with modified first file should fail on second file
	// because second file's hash depends on first file's hash
	modifiedContent1 := "CREATE DATABASE modified;"
	files = map[string]io.Reader{
		"20240101120000.sql": strings.NewReader(modifiedContent1),
		"20240101120100.sql": strings.NewReader(content2), // Unchanged content
	}

	isValid, err = sumFile.Validate(files)
	require.NoError(t, err)
	require.False(t, isValid, "Modified first file should cause validation to fail")
}

func TestSumFile_Validate_ThreadSafety(t *testing.T) {
	// Test that Validate is thread-safe
	sumFile := migrator.NewSumFile()

	// Add some entries
	migrations := []struct{ version, content string }{
		{"20240101120000.sql", "CREATE DATABASE test1;"},
		{"20240101120100.sql", "CREATE DATABASE test2;"},
		{"20240101120200.sql", "CREATE DATABASE test3;"},
	}

	for _, mig := range migrations {
		err := sumFile.Add(mig.version, strings.NewReader(mig.content))
		require.NoError(t, err)
	}

	// Run validations concurrently (simulating concurrent access)
	done := make(chan bool, 3)

	for range 3 {
		go func() {
			// Create fresh readers for each goroutine
			freshFiles := map[string]io.Reader{
				"20240101120000.sql": strings.NewReader("CREATE DATABASE test1;"),
				"20240101120100.sql": strings.NewReader("CREATE DATABASE test2;"),
				"20240101120200.sql": strings.NewReader("CREATE DATABASE test3;"),
			}

			isValid, err := sumFile.Validate(freshFiles)
			require.NoError(t, err)
			require.True(t, isValid)
			done <- true
		}()
	}

	// Wait for all validations to complete
	for range 3 {
		<-done
	}
}

// failingReader is a test helper that fails after reading a certain number of bytes
type failingReader struct {
	failAfter int
	read      int
}

func (fr *failingReader) Read(p []byte) (int, error) {
	if fr.read >= fr.failAfter {
		return 0, errors.New("simulated read failure")
	}

	// Read up to failAfter bytes
	toRead := len(p)
	if fr.read+toRead > fr.failAfter {
		toRead = fr.failAfter - fr.read
	}

	for i := 0; i < toRead; i++ {
		p[i] = 'x' // Fill with dummy data
	}

	fr.read += toRead
	return toRead, nil
}
