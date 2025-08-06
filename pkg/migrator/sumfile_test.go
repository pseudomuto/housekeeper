package migrator_test

import (
	"bytes"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/stretchr/testify/require"
)

func TestSumFile(t *testing.T) {
	t.Run("NewSumFile creates empty structure", func(t *testing.T) {
		sumFile := NewSumFile()
		require.NotNil(t, sumFile)
		require.Equal(t, 0, sumFile.Files())
		require.Empty(t, sumFile.TotalHash)
	})

	t.Run("AddFile updates structure with chained hashing", func(t *testing.T) {
		sumFile := NewSumFile()

		// Add first file
		content1 := []byte("CREATE TABLE users (id INT);")
		sumFile.AddFile("001_create_users.sql", content1)
		require.Equal(t, 1, sumFile.Files())
		// Total hash is not computed until WriteTo is called
		require.Empty(t, sumFile.TotalHash)

		// Add second file
		content2 := []byte("CREATE TABLE orders (id INT);")
		sumFile.AddFile("002_create_orders.sql", content2)
		require.Equal(t, 2, sumFile.Files())

		// Write to compute total hash
		var buf bytes.Buffer
		_, err := sumFile.WriteTo(&buf)
		require.NoError(t, err)
		require.NotEmpty(t, sumFile.TotalHash)
		require.True(t, strings.HasPrefix(sumFile.TotalHash, "h1:"))
	})

	t.Run("WriteTo outputs correct format", func(t *testing.T) {
		sumFile := NewSumFile()
		sumFile.AddFile("001_create_users.sql", []byte("CREATE TABLE users;"))
		sumFile.AddFile("002_create_orders.sql", []byte("CREATE TABLE orders;"))

		var buf bytes.Buffer
		n, err := sumFile.WriteTo(&buf)
		require.NoError(t, err)
		require.Positive(t, n)

		output := buf.String()
		lines := strings.Split(strings.TrimSpace(output), "\n")
		require.Len(t, lines, 3) // Total hash + 2 files

		// First line should be total hash
		require.True(t, strings.HasPrefix(lines[0], "h1:"))

		// Following lines should be file entries with H1 hashes
		require.True(t, strings.HasPrefix(lines[1], "001_create_users.sql h1:"))
		require.True(t, strings.HasPrefix(lines[2], "002_create_orders.sql h1:"))
	})

	t.Run("empty sum file produces empty total hash", func(t *testing.T) {
		sumFile := NewSumFile()
		require.Empty(t, sumFile.TotalHash)

		var buf bytes.Buffer
		n, err := sumFile.WriteTo(&buf)
		require.NoError(t, err)
		require.Equal(t, int64(1), n) // Just a newline

		output := buf.String()
		require.Equal(t, "\n", output)
	})

	t.Run("chained hashing produces deterministic results", func(t *testing.T) {
		content1 := []byte("CREATE TABLE users;")
		content2 := []byte("CREATE TABLE orders;")

		sumFile1 := NewSumFile()
		sumFile1.AddFile("001_create_users.sql", content1)
		sumFile1.AddFile("002_create_orders.sql", content2)

		sumFile2 := NewSumFile()
		sumFile2.AddFile("001_create_users.sql", content1)
		sumFile2.AddFile("002_create_orders.sql", content2)

		// Write both to compute total hashes
		var buf1, buf2 bytes.Buffer
		_, err := sumFile1.WriteTo(&buf1)
		require.NoError(t, err)
		_, err = sumFile2.WriteTo(&buf2)
		require.NoError(t, err)

		require.Equal(t, sumFile1.TotalHash, sumFile2.TotalHash)
		require.Equal(t, buf1.String(), buf2.String())
	})

	t.Run("different file order produces different hash due to chaining", func(t *testing.T) {
		content1 := []byte("CREATE TABLE users;")
		content2 := []byte("CREATE TABLE orders;")

		sumFile1 := NewSumFile()
		sumFile1.AddFile("001_create_users.sql", content1)
		sumFile1.AddFile("002_create_orders.sql", content2)

		sumFile2 := NewSumFile()
		sumFile2.AddFile("002_create_orders.sql", content2)
		sumFile2.AddFile("001_create_users.sql", content1)

		// Write both to compute total hashes
		var buf1, buf2 bytes.Buffer
		_, err := sumFile1.WriteTo(&buf1)
		require.NoError(t, err)
		_, err = sumFile2.WriteTo(&buf2)
		require.NoError(t, err)

		// Different order should produce different hashes due to chaining
		require.NotEqual(t, sumFile1.TotalHash, sumFile2.TotalHash)
	})

	t.Run("chained hash incorporates previous hash", func(t *testing.T) {
		// Test that the second file's hash is different when added alone
		// vs when added after another file (proving chaining)
		content := []byte("CREATE TABLE orders;")

		// Add file as first file
		sumFile1 := NewSumFile()
		sumFile1.AddFile("002_create_orders.sql", content)

		// Add same file as second file (after another)
		sumFile2 := NewSumFile()
		sumFile2.AddFile("001_create_users.sql", []byte("CREATE TABLE users;"))
		sumFile2.AddFile("002_create_orders.sql", content)

		// Write both to compute total hashes
		var buf1, buf2 bytes.Buffer
		_, err := sumFile1.WriteTo(&buf1)
		require.NoError(t, err)
		_, err = sumFile2.WriteTo(&buf2)
		require.NoError(t, err)

		// The total hash should be different because individual file hashes are different
		require.NotEqual(t, sumFile1.TotalHash, sumFile2.TotalHash)
	})

	t.Run("total hash is SHA256 of all file hashes", func(t *testing.T) {
		// Test that adding a new file changes the total hash
		// (it's not just the last file's hash)
		sumFile := NewSumFile()
		sumFile.AddFile("001_create_users.sql", []byte("CREATE TABLE users;"))

		var buf1 bytes.Buffer
		_, err := sumFile.WriteTo(&buf1)
		require.NoError(t, err)
		firstTotal := sumFile.TotalHash

		sumFile.AddFile("002_create_orders.sql", []byte("CREATE TABLE orders;"))

		var buf2 bytes.Buffer
		_, err = sumFile.WriteTo(&buf2)
		require.NoError(t, err)
		secondTotal := sumFile.TotalHash

		// Total hash should change when adding files
		require.NotEqual(t, firstTotal, secondTotal)

		// Create another sum file with just the second file
		sumFile2 := NewSumFile()
		sumFile2.AddFile("002_create_orders.sql", []byte("CREATE TABLE orders;"))

		var buf3 bytes.Buffer
		_, err = sumFile2.WriteTo(&buf3)
		require.NoError(t, err)

		// The total hash of two files should be different from just the second file
		// (proves it's not just using the last file's hash)
		require.NotEqual(t, secondTotal, sumFile2.TotalHash)
	})
}

func TestLoadSumFile(t *testing.T) {
	t.Run("loads empty sum file", func(t *testing.T) {
		input := "\n"
		reader := strings.NewReader(input)

		sumFile, err := LoadSumFile(reader)
		require.NoError(t, err)
		require.NotNil(t, sumFile)
		require.Equal(t, 0, sumFile.Files())
		require.Empty(t, sumFile.TotalHash)
	})

	t.Run("loads sum file with files", func(t *testing.T) {
		input := `h1:dG90YWxoYXNoZXhhbXBsZQ==
001_create_users.sql h1:dGVzdGRhdGE=
002_create_orders.sql h1:bW9yZXRlc3Q=`
		reader := strings.NewReader(input)

		sumFile, err := LoadSumFile(reader)
		require.NoError(t, err)
		require.NotNil(t, sumFile)
		require.Equal(t, 2, sumFile.Files())
		require.Equal(t, "h1:dG90YWxoYXNoZXhhbXBsZQ==", sumFile.TotalHash)
	})

	t.Run("roundtrip test - write and read back", func(t *testing.T) {
		// Create original sum file
		original := NewSumFile()
		original.AddFile("001_init.sql", []byte("CREATE DATABASE test;"))
		original.AddFile("002_users.sql", []byte("CREATE TABLE users (id INT);"))

		// Write to buffer
		var buf bytes.Buffer
		n, err := original.WriteTo(&buf)
		require.NoError(t, err)
		require.Positive(t, n)

		// Load back from buffer
		loaded, err := LoadSumFile(&buf)
		require.NoError(t, err)
		require.NotNil(t, loaded)

		// Compare
		require.Equal(t, original.Files(), loaded.Files())
		require.Equal(t, original.TotalHash, loaded.TotalHash)

		// Write both and compare output
		var buf1, buf2 bytes.Buffer
		_, err = original.WriteTo(&buf1)
		require.NoError(t, err)
		_, err = loaded.WriteTo(&buf2)
		require.NoError(t, err)

		require.Equal(t, buf1.String(), buf2.String())
	})

	t.Run("handles empty input", func(t *testing.T) {
		reader := strings.NewReader("")

		sumFile, err := LoadSumFile(reader)
		require.NoError(t, err)
		require.NotNil(t, sumFile)
		require.Equal(t, 0, sumFile.Files())
		require.Empty(t, sumFile.TotalHash)
	})

	t.Run("returns error for invalid total hash format", func(t *testing.T) {
		input := "invalid-hash-format\n"
		reader := strings.NewReader(input)

		_, err := LoadSumFile(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid total hash format")
	})

	t.Run("returns error for invalid file entry format", func(t *testing.T) {
		input := `h1:dG90YWxoYXNoZXhhbXBsZQ==
invalid-file-entry-without-hash`
		reader := strings.NewReader(input)

		_, err := LoadSumFile(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid file entry format")
	})

	t.Run("returns error for invalid hash in file entry", func(t *testing.T) {
		input := `h1:dG90YWxoYXNoZXhhbXBsZQ==
001_test.sql invalid-hash-format`
		reader := strings.NewReader(input)

		_, err := LoadSumFile(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "invalid hash format")
	})

	t.Run("returns error for invalid base64 in hash", func(t *testing.T) {
		input := `h1:dG90YWxoYXNoZXhhbXBsZQ==
001_test.sql h1:invalid-base64!@#`
		reader := strings.NewReader(input)

		_, err := LoadSumFile(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to decode hash")
	})

	t.Run("ignores empty lines", func(t *testing.T) {
		input := `h1:dG90YWxoYXNoZXhhbXBsZQ==

001_create_users.sql h1:dGVzdGRhdGE=

002_create_orders.sql h1:bW9yZXRlc3Q=

`
		reader := strings.NewReader(input)

		sumFile, err := LoadSumFile(reader)
		require.NoError(t, err)
		require.NotNil(t, sumFile)
		require.Equal(t, 2, sumFile.Files())
	})
}
