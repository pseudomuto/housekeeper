package migrator

import (
	"bufio"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io"
	"strings"

	"github.com/pkg/errors"
)

type (
	// SumFile represents a collection of files with their individual hashes
	// and a total hash computed as SHA256 of all file hashes. Individual file
	// hashes use a chained approach where each file's hash incorporates the
	// previous file's hash.
	SumFile struct {
		files     []fileEntry // List of files with their hashes
		TotalHash string      // Total hash (H1 format) = SHA256(all file hashes)
	}

	// fileEntry represents a single file with its hash in the sum file
	fileEntry struct {
		Name string // File name
		Hash []byte // Raw SHA256 hash bytes
	}
)

// NewSumFile creates a new empty SumFile ready to accept files.
//
// Example:
//
//	sumFile := NewSumFile()
//	sumFile.AddFile("001_create_users.sql", fileContent1)
//	sumFile.AddFile("002_create_orders.sql", fileContent2)
//	fmt.Println(sumFile.TotalHash) // h1:base64-encoded-total-hash
func NewSumFile() *SumFile {
	return &SumFile{
		files: make([]fileEntry, 0),
	}
}

// LoadSumFile reads a SumFile from an io.Reader that contains data in the
// format produced by WriteTo. The expected format is:
//   - First line: total hash (h1:base64-encoded-hash)
//   - Following lines: <filename> <h1:base64-encoded-hash>
//
// Example:
//
//	file, err := os.Open("housekeeper.sum")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	sumFile, err := migrator.LoadSumFile(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("Loaded %d files\n", sumFile.Files())
func LoadSumFile(r io.Reader) (*SumFile, error) {
	scanner := bufio.NewScanner(r)
	sumFile := &SumFile{
		files: make([]fileEntry, 0),
	}

	// Read first line (total hash)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return nil, errors.Wrap(err, "failed to read total hash line")
		}
		// Empty file - return empty SumFile
		return sumFile, nil
	}

	totalHashLine := strings.TrimSpace(scanner.Text())
	if totalHashLine == "" {
		// Empty total hash means empty file
		sumFile.TotalHash = ""
		return sumFile, nil
	}

	if !strings.HasPrefix(totalHashLine, "h1:") {
		return nil, errors.Errorf("invalid total hash format: %s", totalHashLine)
	}
	sumFile.TotalHash = totalHashLine

	// Read file entries
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		parts := strings.SplitN(line, " ", 2)
		if len(parts) != 2 {
			return nil, errors.Errorf("invalid file entry format: %s", line)
		}

		filename := parts[0]
		h1Hash := parts[1]

		if !strings.HasPrefix(h1Hash, "h1:") {
			return nil, errors.Errorf("invalid hash format for file %s: %s", filename, h1Hash)
		}

		// Decode the base64 hash
		hashBase64 := strings.TrimPrefix(h1Hash, "h1:")
		hashBytes, err := base64.StdEncoding.DecodeString(hashBase64)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to decode hash for file %s", filename)
		}

		sumFile.files = append(sumFile.files, fileEntry{
			Name: filename,
			Hash: hashBytes,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, errors.Wrap(err, "error reading sum file")
	}

	return sumFile, nil
}

// AddFile adds a file with the given name and content, computing its hash
// based on the content and the previous file's hash (chained hashing).
//
// The chaining works as follows:
//   - First file: hash = SHA256(content)
//   - Subsequent files: hash = SHA256(content + previousHash)
//
// The total hash (SHA256 of all file hashes) is computed lazily when
// WriteTo is called, not on each AddFile call.
//
// Example:
//
//	sumFile := NewSumFile()
//	sumFile.AddFile("001_create_users.sql", []byte("CREATE TABLE users..."))
//	sumFile.AddFile("002_create_orders.sql", []byte("CREATE TABLE orders..."))
func (s *SumFile) AddFile(name string, content []byte) {
	hasher := sha256.New()
	hasher.Write(content)

	// If there's a previous file, include its hash in the computation
	if len(s.files) > 0 {
		previousHash := s.files[len(s.files)-1].Hash
		hasher.Write(previousHash)
	}

	hash := hasher.Sum(nil)
	s.files = append(s.files, fileEntry{Name: name, Hash: hash})
}

// Files returns the count of files in the sum file
func (s *SumFile) Files() int {
	return len(s.files)
}

// WriteTo writes the sum file to the provided writer in the specified format.
// It implements the io.WriterTo interface for efficient streaming.
// The total hash is computed at this point (lazy evaluation).
//
// Output format:
//   - First line: total hash (h1:base64-encoded-hash)
//   - Following lines: <file> <h1-hash-string>
//
// Example output:
//
//	h1:dG90YWxoYXNoZXhhbXBsZQ==
//	001_create_users.sql h1:dGVzdGRhdGE=
//	002_create_orders.sql h1:bW9yZXRlc3Q=
//
// Returns the number of bytes written and any error encountered.
func (s *SumFile) WriteTo(w io.Writer) (int64, error) {
	var total int64

	// Compute total hash before writing
	s.computeTotalHash()

	// Write total hash
	n, err := fmt.Fprintf(w, "%s\n", s.TotalHash)
	if err != nil {
		return total, err
	}
	total += int64(n)

	// Write each file entry with its H1 hash
	for _, file := range s.files {
		h1Hash := "h1:" + base64.StdEncoding.EncodeToString(file.Hash)
		n, err := fmt.Fprintf(w, "%s %s\n", file.Name, h1Hash)
		if err != nil {
			return total, err
		}
		total += int64(n)
	}

	return total, nil
}

// computeTotalHash calculates the total hash as SHA256 of all file hashes concatenated
func (s *SumFile) computeTotalHash() {
	if len(s.files) == 0 {
		s.TotalHash = ""
		return
	}

	hasher := sha256.New()
	for _, file := range s.files {
		hasher.Write(file.Hash)
	}

	totalHash := hasher.Sum(nil)
	s.TotalHash = "h1:" + base64.StdEncoding.EncodeToString(totalHash)
}
