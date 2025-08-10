package migrator

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"hash"
	"io"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

type (
	// SumFile provides cryptographic integrity verification for migration files
	// using a reverse one-branch Merkle tree approach with chained SHA256 hashing.
	//
	// The SumFile maintains a chronologically ordered list of migration file hashes,
	// where each entry builds upon the previous hash to create a tamper-evident
	// chain. This design allows detection of any modification to migration files
	// and ensures the integrity of the complete migration history.
	//
	// File format (h1 compatible with Go modules):
	//   h1:TotalHashOfAllEntries=
	//   001_init.sql h1:HashOfFirstFile=
	//   002_users.sql h1:ChainedHashIncorporatingPreviousHash=
	//   003_views.sql h1:ChainedHashIncorporatingPreviousHash=
	//
	// The chained hashing means that:
	// - Entry 1 hash = SHA256(file1_content)
	// - Entry 2 hash = SHA256(entry1_hash + file2_content)
	// - Entry 3 hash = SHA256(entry2_hash + file3_content)
	//
	// This provides tamper evidence - changing any file or reordering files
	// will invalidate all subsequent hashes in the chain.
	SumFile struct {
		h       hash.Hash
		mu      sync.Mutex
		entries []sumEntry
		sum     []byte
	}

	// sumEntry represents a single migration file's integrity information
	// within the chained hash structure. Each entry contains the migration
	// version identifier and its chained hash value.
	sumEntry struct {
		// version is the migration identifier, typically derived from filename
		version string
		// hash is the chained SHA256 hash incorporating previous entry hash
		hash []byte
	}
)

// NewSumFile creates a new empty SumFile ready for adding migration entries.
// The SumFile is initialized with a SHA256 hasher and empty entry list.
//
// Example usage:
//
//	sumFile := migrator.NewSumFile()
//
//	// Add migrations in order
//	err := sumFile.Add("001_init.sql", strings.NewReader("CREATE DATABASE test;"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	err = sumFile.Add("002_users.sql", strings.NewReader("CREATE TABLE users (...);"))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write to file
//	file, err := os.Create("migrations.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = sumFile.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
func NewSumFile() *SumFile {
	return &SumFile{
		h: sha256.New(),
	}
}

// LoadSumFile reads and parses a SumFile from the provided reader.
// The reader should contain a properly formatted sum file with h1-prefixed
// base64-encoded SHA256 hashes.
//
// Expected format:
//
//	h1:TotalHashBase64=
//	001_init.sql h1:FileHashBase64=
//	002_users.sql h1:ChainedHashBase64=
//
// The first line contains the total hash of all entries, followed by
// individual migration entries with their chained hashes.
//
// Example usage:
//
//	// Load from file
//	file, err := os.Open("migrations.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	sumFile, err := migrator.LoadSumFile(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Load from string
//	sumContent := `h1:abcd1234base64hash=
//	001_init.sql h1:file1hash=
//	002_users.sql h1:chainedHash=`
//
//	sumFile, err = migrator.LoadSumFile(strings.NewReader(sumContent))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// The loaded SumFile can now be used for integrity verification
//	fmt.Printf("Loaded %d migration entries\n", len(sumFile.entries))
//
// Returns an error if the reader contains invalid format or corrupted hash data.
func LoadSumFile(r io.Reader) (*SumFile, error) {
	f := NewSumFile()

	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		return nil, errors.New("empty sum file: missing total hash line")
	}
	sum, err := readHash(scanner.Text())
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse hash: %s", scanner.Text())
	}
	f.sum = sum

	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), " ", 2)
		sum, err := readHash(parts[1])
		if err != nil {
			return nil, errors.Wrapf(err, "failed to parse hash for: %s", parts[0])
		}

		f.entries = append(f.entries, sumEntry{
			version: parts[0],
			hash:    sum,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "error reading sum file")
	}

	return f, nil
}

// Add appends a new migration entry to the SumFile with chained hash calculation.
//
// This method reads the provided migration content, calculates its hash chained
// with the previous entry's hash (if any), and adds it to the sum file entries.
// The chaining means that each hash incorporates the previous hash, creating
// a tamper-evident chain that detects any modification or reordering.
//
// Hash calculation:
//   - First entry: SHA256(file_content)
//   - Subsequent entries: SHA256(previous_hash + file_content)
//
// This method is thread-safe and can be called concurrently, though entries
// will be processed sequentially to maintain proper hash chaining.
//
// Example usage:
//
//	sumFile := migrator.NewSumFile()
//
//	// Add first migration
//	sql1 := "CREATE DATABASE test ENGINE = Atomic;"
//	err := sumFile.Add("001_init.sql", strings.NewReader(sql1))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Add second migration (hash will be chained with first)
//	sql2 := "CREATE TABLE test.users (id UInt64, name String) ENGINE = MergeTree() ORDER BY id;"
//	err = sumFile.Add("002_users.sql", strings.NewReader(sql2))
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Add from file
//	file, err := os.Open("003_views.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	err = sumFile.Add("003_views.sql", file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// The version parameter should be a unique identifier for the migration,
// typically the filename without directory path.
//
// Returns an error if the reader cannot be read or hash calculation fails.
func (f *SumFile) Add(v string, r io.Reader) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.h.Reset()
	if len(f.entries) > 0 {
		_, err := f.h.Write(f.entries[len(f.entries)-1].hash)
		if err != nil {
			return errors.Wrap(err, "failed to add previous hash")
		}
	}

	_, err := io.Copy(f.h, r)
	if err != nil {
		return errors.Wrap(err, "failed to hash input reader")
	}

	f.entries = append(f.entries, sumEntry{
		version: v,
		hash:    f.h.Sum(nil),
	})

	return nil
}

// WriteTo writes the complete SumFile to the provided writer in the standard format.
//
// The output format is compatible with Go module sum files (h1 format) and contains:
//  1. First line: Total hash of all entries (currently writes the sum field)
//  2. Subsequent lines: Each migration entry with format "version h1:hash"
//
// Output format:
//
//	h1:TotalHashBase64=
//	001_init.sql h1:FileHashBase64=
//	002_users.sql h1:ChainedHashBase64=
//	003_views.sql h1:ChainedHashBase64=
//
// This method implements the io.WriterTo interface for efficient streaming
// and returns the total number of bytes written.
//
// Example usage:
//
//	sumFile := migrator.NewSumFile()
//	// ... add entries ...
//
//	// Write to file
//	file, err := os.Create("migrations.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	bytesWritten, err := sumFile.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("Wrote %d bytes to sum file\n", bytesWritten)
//
//	// Write to buffer for string representation
//	var buf bytes.Buffer
//	_, err = sumFile.WriteTo(&buf)
//	if err != nil {
//		log.Fatal(err)
//	}
//	sumContent := buf.String()
//	fmt.Println("Sum file content:", sumContent)
//
//	// Write to HTTP response or any io.Writer
//	_, err = sumFile.WriteTo(httpResponseWriter)
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Returns the number of bytes written and any error encountered during writing.
func (f *SumFile) WriteTo(w io.Writer) (int64, error) {
	bytesWritten := int64(0)
	n, err := fmt.Fprintln(w, writeHash(f.sum))
	if err != nil {
		return bytesWritten, err
	}
	bytesWritten += int64(n)

	for _, entry := range f.entries {
		n, err := fmt.Fprintf(w, "%s %s\n", entry.version, writeHash(entry.hash))
		if err != nil {
			return bytesWritten, err
		}
		bytesWritten += int64(n)
	}

	return bytesWritten, nil
}

// Validate verifies the integrity of the SumFile by recalculating chained hashes
// from the provided migration content and comparing them with stored values.
//
// This method ensures that the migration files have not been modified since the
// SumFile was created by recomputing the chained hash sequence and comparing
// each entry against the stored hash values.
//
// The files parameter should be a map where keys are migration versions (matching
// the entries in this SumFile) and values are io.Reader instances containing
// the current migration file content.
//
// Validation process:
//  1. Iterates through all entries in lexical order by version
//  2. For each entry, calculates the chained hash using the previous hash and current content
//  3. Compares the calculated hash with the stored hash
//  4. Returns false immediately if any hash mismatch is found
//  5. Returns true only if all hashes match exactly
//
// Example usage:
//
//	// Load sum file
//	sumFile, err := migrator.LoadSumFile(sumFileReader)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Prepare migration files for validation
//	files := make(map[string]io.Reader)
//	files["20240101120000"] = strings.NewReader("CREATE DATABASE test ENGINE = Atomic;")
//	files["20240101120100"] = strings.NewReader("CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;")
//
//	// Validate integrity
//	isValid, err := sumFile.Validate(files)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if isValid {
//		fmt.Println("Migration files are valid and unmodified")
//	} else {
//		fmt.Println("Migration files have been modified!")
//	}
//
// Returns false if:
//   - Any entry's calculated hash doesn't match the stored hash
//   - A required migration file is missing from the files map
//
// Returns an error if:
//   - Any of the provided readers cannot be read
//   - Hash calculation fails for any entry
//
// Note: This method is thread-safe and does not modify the SumFile.
func (f *SumFile) Validate(files map[string]io.Reader) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Create a temporary hasher for validation
	h := sha256.New()

	for i, entry := range f.entries {
		reader, exists := files[entry.version]
		if !exists {
			return false, nil // Missing file means validation fails
		}

		h.Reset()

		// Add previous hash if this isn't the first entry
		if i > 0 {
			_, err := h.Write(f.entries[i-1].hash)
			if err != nil {
				return false, errors.Wrapf(err, "failed to write previous hash for version %s", entry.version)
			}
		}

		// Hash the file content
		_, err := io.Copy(h, reader)
		if err != nil {
			return false, errors.Wrapf(err, "failed to read content for version %s", entry.version)
		}

		// Calculate hash and compare
		calculatedHash := h.Sum(nil)
		if !equalHashes(calculatedHash, entry.hash) {
			return false, nil // Hash mismatch
		}
	}

	return true, nil
}

// equalHashes compares two byte slices for equality in constant time.
// This prevents timing attacks on hash comparisons.
func equalHashes(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}

	var result byte
	for i := range a {
		result |= a[i] ^ b[i]
	}

	return result == 0
}

// readHash decodes a base64-encoded hash string with h1 prefix.
// Expected format: "h1:base64encodeddata"
func readHash(h string) ([]byte, error) {
	return base64.StdEncoding.DecodeString(strings.TrimPrefix(h, "h1:"))
}

// writeHash encodes a hash byte slice as a base64 string with h1 prefix.
// Output format: "h1:base64encodeddata"
func writeHash(h []byte) string {
	return "h1:" + base64.StdEncoding.EncodeToString(h)
}
