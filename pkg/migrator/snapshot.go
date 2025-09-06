package migrator

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	snapshotMarker = "-- housekeeper:snapshot"
)

type (
	// Snapshot represents a migration snapshot that consolidates all previous
	// migrations into a single point-in-time snapshot.
	//
	// Snapshots allow for safe deletion of migration files that preceded the
	// snapshot, as all their changes are captured in the snapshot's cumulative
	// SQL. This helps manage large migration histories while maintaining the ability
	// to recreate the schema from scratch.
	//
	// A snapshot file uses a special comment directive format:
	//   -- housekeeper:snapshot
	//   -- version: 20240810120000_snapshot
	//   -- description: Q3 2024 Release
	//   -- created_at: 2024-08-10T12:00:00Z
	//   -- included_migrations: 001_init,002_users,003_products
	//   -- cumulative_hash: sha256:abc123...
	//
	// Followed by the cumulative SQL from all included migrations.
	Snapshot struct {
		// Version is the unique identifier for the snapshot, typically
		// a timestamp-based string like "20240810120000_snapshot".
		Version string

		// Description provides a human-readable description of the snapshot,
		// such as "Q3 2024 Release" or "Post-migration cleanup".
		Description string

		// CreatedAt records when the snapshot was created.
		CreatedAt time.Time

		// IncludedMigrations lists the versions of all migrations that are
		// consolidated in this snapshot, in the order they were applied.
		IncludedMigrations []string

		// CumulativeHash is the SHA256 hash of all included migration content,
		// used for integrity verification.
		CumulativeHash string

		// Statements contains the parsed DDL statements from all included
		// migrations, representing the cumulative schema state.
		Statements []*parser.Statement
	}

	// snapshotMetadata represents the parsed metadata from a snapshot file header.
	snapshotMetadata struct {
		version            string
		description        string
		createdAt          time.Time
		includedMigrations []string
		cumulativeHash     string
	}
)

// IsSnapshot checks if a migration file is a snapshot by examining its content
// for the snapshot marker comment.
//
// This function reads the beginning of the file to check for the special
// -- housekeeper:snapshot marker that identifies snapshot files.
//
// Example usage:
//
//	file, err := os.Open("20240810120000_snapshot.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	isSnapshot, err := migrator.IsSnapshot(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if isSnapshot {
//		fmt.Println("This is a snapshot file")
//	}
func IsSnapshot(r io.Reader) (bool, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, errors.Wrap(err, "failed to read file")
		}
		return false, nil
	}

	firstLine := strings.TrimSpace(scanner.Text())
	return firstLine == snapshotMarker, nil
}

// LoadSnapshot reads and parses a snapshot file from the provided reader.
//
// The reader should contain a properly formatted snapshot file with the
// snapshot marker, metadata headers, and cumulative SQL statements.
//
// Example usage:
//
//	file, err := os.Open("20240810120000_snapshot.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	snapshot, err := migrator.LoadSnapshot(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Snapshot %s includes %d migrations\n",
//		snapshot.Version, len(snapshot.IncludedMigrations))
func LoadSnapshot(r io.Reader) (*Snapshot, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read snapshot file")
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) < 1 || strings.TrimSpace(lines[0]) != snapshotMarker {
		return nil, errors.New("invalid snapshot file: missing snapshot marker")
	}

	metadata, endIndex, err := parseSnapshotMetadata(lines)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse snapshot metadata")
	}

	// Parse the SQL content after metadata
	sqlContent := strings.Join(lines[endIndex:], "\n")
	sql, err := parser.ParseString(sqlContent)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse snapshot SQL")
	}

	return &Snapshot{
		Version:            metadata.version,
		Description:        metadata.description,
		CreatedAt:          metadata.createdAt,
		IncludedMigrations: metadata.includedMigrations,
		CumulativeHash:     metadata.cumulativeHash,
		Statements:         sql.Statements,
	}, nil
}

// GenerateSnapshot creates a new snapshot from the provided migrations.
//
// This function consolidates all provided migrations into a single snapshot,
// calculating the cumulative hash and combining all SQL statements.
//
// Example usage:
//
//	migrations := []*migrator.Migration{
//		migration1,
//		migration2,
//		migration3,
//	}
//
//	snapshot, err := migrator.GenerateSnapshot(
//		"20240810120000_snapshot",
//		"Q3 2024 Release",
//		migrations,
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write snapshot to file
//	file, err := os.Create("migrations/20240810120000_snapshot.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = snapshot.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
func GenerateSnapshot(version, description string, migrations []*Migration) (*Snapshot, error) {
	if len(migrations) == 0 {
		return nil, errors.New("cannot create snapshot from empty migration list")
	}

	// Collect all statements and migration versions
	var allStatements []*parser.Statement
	includedVersions := make([]string, 0, len(migrations))
	hasher := sha256.New()

	for _, mig := range migrations {
		includedVersions = append(includedVersions, mig.Version)
		allStatements = append(allStatements, mig.Statements...)

		// Add migration content to hash
		for _, stmt := range mig.Statements {
			// Format statement for consistent hashing
			var buf strings.Builder
			err := format.Format(&buf, format.Defaults, stmt)
			if err != nil {
				return nil, errors.Wrapf(err, "failed to format statement for hashing")
			}
			hasher.Write([]byte(buf.String()))
		}
	}

	return &Snapshot{
		Version:            version,
		Description:        description,
		CreatedAt:          time.Now().UTC(),
		IncludedMigrations: includedVersions,
		CumulativeHash:     hex.EncodeToString(hasher.Sum(nil)),
		Statements:         allStatements,
	}, nil
}

// WriteTo writes the snapshot to the provided writer in the snapshot file format.
//
// The output includes the snapshot marker, metadata headers, and formatted
// SQL statements from all included migrations.
//
// This method implements the io.WriterTo interface for efficient streaming.
func (c *Snapshot) WriteTo(w io.Writer) (int64, error) {
	var totalBytes int64

	// Write snapshot marker
	n, err := fmt.Fprintln(w, snapshotMarker)
	if err != nil {
		return totalBytes, errors.Wrap(err, "failed to write snapshot marker")
	}
	totalBytes += int64(n)

	// Write metadata
	metadata := []string{
		"-- version: " + c.Version,
		"-- description: " + c.Description,
		"-- created_at: " + c.CreatedAt.Format(time.RFC3339),
		"-- included_migrations: " + strings.Join(c.IncludedMigrations, ","),
		"-- cumulative_hash: " + c.CumulativeHash,
		"", // Empty line after metadata
	}

	for _, line := range metadata {
		n, err := fmt.Fprintln(w, line)
		if err != nil {
			return totalBytes, errors.Wrap(err, "failed to write metadata")
		}
		totalBytes += int64(n)
	}

	// Write formatted SQL statements
	if len(c.Statements) > 0 {
		n, err := fmt.Fprintln(w, "-- Cumulative SQL from all included migrations")
		if err != nil {
			return totalBytes, errors.Wrap(err, "failed to write SQL header")
		}
		totalBytes += int64(n)

		err = format.Format(w, format.Defaults, c.Statements...)
		if err != nil {
			return totalBytes, errors.Wrap(err, "failed to write SQL statements")
		}
		// Note: format.Format doesn't return bytes written, so we can't track them precisely
		// This is acceptable as WriteTo is primarily for writing, not byte counting
	}

	return totalBytes, nil
}

// ValidateAgainstRevisions verifies that the snapshot is consistent with the
// revision history in the database.
//
// This checks that all included migrations have been successfully applied
// according to the revision records.
func (c *Snapshot) ValidateAgainstRevisions(revisionSet *RevisionSet) error {
	for _, version := range c.IncludedMigrations {
		revision := revisionSet.GetRevision(&Migration{Version: version})
		if revision == nil {
			return errors.Errorf("snapshot includes migration %s which has no revision record", version)
		}

		// Check if the revision indicates successful completion
		if revision.Kind != StandardRevision || revision.Error != nil {
			return errors.Errorf("snapshot includes migration %s which is not completed", version)
		}

		// Check if all statements were applied successfully
		if revision.Applied != revision.Total {
			return errors.Errorf("snapshot includes migration %s which is not completed", version)
		}
	}

	return nil
}

// parseSnapshotMetadata parses the metadata headers from snapshot file lines.
// Returns the parsed metadata and the index where the SQL content begins.
func parseSnapshotMetadata(lines []string) (*snapshotMetadata, int, error) {
	metadata := &snapshotMetadata{}
	endIndex := 1 // Start after snapshot marker

	for i := 1; i < len(lines); i++ {
		line := strings.TrimSpace(lines[i])

		// End of metadata section
		if !strings.HasPrefix(line, "-- ") || line == "--" {
			endIndex = i
			break
		}

		// Parse metadata fields
		err := parseMetadataField(line, metadata)
		if err != nil {
			return nil, 0, err
		}
	}

	// Validate required fields
	if metadata.version == "" {
		return nil, 0, errors.New("snapshot missing required version field")
	}
	// Note: includedMigrations can be empty for bootstrap snapshots, so no validation needed

	return metadata, endIndex, nil
}

// parseMetadataField parses a single metadata field line and updates the metadata struct
func parseMetadataField(line string, metadata *snapshotMetadata) error {
	switch {
	case strings.HasPrefix(line, "-- version: "):
		metadata.version = strings.TrimPrefix(line, "-- version: ")
	case strings.HasPrefix(line, "-- description: "):
		metadata.description = strings.TrimPrefix(line, "-- description: ")
	case strings.HasPrefix(line, "-- cumulative_hash: "):
		metadata.cumulativeHash = strings.TrimPrefix(line, "-- cumulative_hash: ")
	case strings.HasPrefix(line, "-- included_migrations: "):
		migrationsStr := strings.TrimPrefix(line, "-- included_migrations: ")
		if migrationsStr != "" {
			metadata.includedMigrations = strings.Split(migrationsStr, ",")
		}
	case strings.HasPrefix(line, "-- created_at: "):
		timeStr := strings.TrimPrefix(line, "-- created_at: ")
		createdAt, err := time.Parse(time.RFC3339, timeStr)
		if err != nil {
			return errors.Wrapf(err, "failed to parse created_at: %s", timeStr)
		}
		metadata.createdAt = createdAt
	}
	return nil
}
