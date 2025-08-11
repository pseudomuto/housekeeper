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
	checkpointMarker = "-- housekeeper:checkpoint"
)

type (
	// Checkpoint represents a migration checkpoint that consolidates all previous
	// migrations into a single point-in-time snapshot.
	//
	// Checkpoints allow for safe deletion of migration files that preceded the
	// checkpoint, as all their changes are captured in the checkpoint's cumulative
	// SQL. This helps manage large migration histories while maintaining the ability
	// to recreate the schema from scratch.
	//
	// A checkpoint file uses a special comment directive format:
	//   -- housekeeper:checkpoint
	//   -- version: 20240810120000_checkpoint
	//   -- description: Q3 2024 Release
	//   -- created_at: 2024-08-10T12:00:00Z
	//   -- included_migrations: 001_init,002_users,003_products
	//   -- cumulative_hash: sha256:abc123...
	//
	// Followed by the cumulative SQL from all included migrations.
	Checkpoint struct {
		// Version is the unique identifier for the checkpoint, typically
		// a timestamp-based string like "20240810120000_checkpoint".
		Version string

		// Description provides a human-readable description of the checkpoint,
		// such as "Q3 2024 Release" or "Post-migration cleanup".
		Description string

		// CreatedAt records when the checkpoint was created.
		CreatedAt time.Time

		// IncludedMigrations lists the versions of all migrations that are
		// consolidated in this checkpoint, in the order they were applied.
		IncludedMigrations []string

		// CumulativeHash is the SHA256 hash of all included migration content,
		// used for integrity verification.
		CumulativeHash string

		// Statements contains the parsed DDL statements from all included
		// migrations, representing the cumulative schema state.
		Statements []*parser.Statement
	}

	// checkpointMetadata represents the parsed metadata from a checkpoint file header.
	checkpointMetadata struct {
		version            string
		description        string
		createdAt          time.Time
		includedMigrations []string
		cumulativeHash     string
	}
)

// IsCheckpoint checks if a migration file is a checkpoint by examining its content
// for the checkpoint marker comment.
//
// This function reads the beginning of the file to check for the special
// -- housekeeper:checkpoint marker that identifies checkpoint files.
//
// Example usage:
//
//	file, err := os.Open("20240810120000_checkpoint.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	isCheckpoint, err := migrator.IsCheckpoint(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if isCheckpoint {
//		fmt.Println("This is a checkpoint file")
//	}
func IsCheckpoint(r io.Reader) (bool, error) {
	scanner := bufio.NewScanner(r)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return false, errors.Wrap(err, "failed to read file")
		}
		return false, nil
	}

	firstLine := strings.TrimSpace(scanner.Text())
	return firstLine == checkpointMarker, nil
}

// LoadCheckpoint reads and parses a checkpoint file from the provided reader.
//
// The reader should contain a properly formatted checkpoint file with the
// checkpoint marker, metadata headers, and cumulative SQL statements.
//
// Example usage:
//
//	file, err := os.Open("20240810120000_checkpoint.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	checkpoint, err := migrator.LoadCheckpoint(file)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Checkpoint %s includes %d migrations\n",
//		checkpoint.Version, len(checkpoint.IncludedMigrations))
func LoadCheckpoint(r io.Reader) (*Checkpoint, error) {
	content, err := io.ReadAll(r)
	if err != nil {
		return nil, errors.Wrap(err, "failed to read checkpoint file")
	}

	lines := strings.Split(string(content), "\n")
	if len(lines) < 1 || strings.TrimSpace(lines[0]) != checkpointMarker {
		return nil, errors.New("invalid checkpoint file: missing checkpoint marker")
	}

	metadata, endIndex, err := parseCheckpointMetadata(lines)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse checkpoint metadata")
	}

	// Parse the SQL content after metadata
	sqlContent := strings.Join(lines[endIndex:], "\n")
	sql, err := parser.ParseString(sqlContent)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse checkpoint SQL")
	}

	return &Checkpoint{
		Version:            metadata.version,
		Description:        metadata.description,
		CreatedAt:          metadata.createdAt,
		IncludedMigrations: metadata.includedMigrations,
		CumulativeHash:     metadata.cumulativeHash,
		Statements:         sql.Statements,
	}, nil
}

// GenerateCheckpoint creates a new checkpoint from the provided migrations.
//
// This function consolidates all provided migrations into a single checkpoint,
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
//	checkpoint, err := migrator.GenerateCheckpoint(
//		"20240810120000_checkpoint",
//		"Q3 2024 Release",
//		migrations,
//	)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Write checkpoint to file
//	file, err := os.Create("migrations/20240810120000_checkpoint.sql")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer file.Close()
//
//	_, err = checkpoint.WriteTo(file)
//	if err != nil {
//		log.Fatal(err)
//	}
func GenerateCheckpoint(version, description string, migrations []*Migration) (*Checkpoint, error) {
	if len(migrations) == 0 {
		return nil, errors.New("cannot create checkpoint from empty migration list")
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

	return &Checkpoint{
		Version:            version,
		Description:        description,
		CreatedAt:          time.Now().UTC(),
		IncludedMigrations: includedVersions,
		CumulativeHash:     hex.EncodeToString(hasher.Sum(nil)),
		Statements:         allStatements,
	}, nil
}

// WriteTo writes the checkpoint to the provided writer in the checkpoint file format.
//
// The output includes the checkpoint marker, metadata headers, and formatted
// SQL statements from all included migrations.
//
// This method implements the io.WriterTo interface for efficient streaming.
func (c *Checkpoint) WriteTo(w io.Writer) (int64, error) {
	var totalBytes int64

	// Write checkpoint marker
	n, err := fmt.Fprintln(w, checkpointMarker)
	if err != nil {
		return totalBytes, errors.Wrap(err, "failed to write checkpoint marker")
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

// ValidateAgainstRevisions verifies that the checkpoint is consistent with the
// revision history in the database.
//
// This checks that all included migrations have been successfully applied
// according to the revision records.
func (c *Checkpoint) ValidateAgainstRevisions(revisionSet *RevisionSet) error {
	for _, version := range c.IncludedMigrations {
		revision := revisionSet.GetRevision(&Migration{Version: version})
		if revision == nil {
			return errors.Errorf("checkpoint includes migration %s which has no revision record", version)
		}

		// Check if the revision indicates successful completion
		if revision.Kind != StandardRevision || revision.Error != nil {
			return errors.Errorf("checkpoint includes migration %s which is not completed", version)
		}

		// Check if all statements were applied successfully
		if revision.Applied != revision.Total {
			return errors.Errorf("checkpoint includes migration %s which is not completed", version)
		}
	}

	return nil
}

// parseCheckpointMetadata parses the metadata headers from checkpoint file lines.
// Returns the parsed metadata and the index where the SQL content begins.
func parseCheckpointMetadata(lines []string) (*checkpointMetadata, int, error) {
	metadata := &checkpointMetadata{}
	endIndex := 1 // Start after checkpoint marker

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
		return nil, 0, errors.New("checkpoint missing required version field")
	}
	if len(metadata.includedMigrations) == 0 {
		return nil, 0, errors.New("checkpoint missing included_migrations field")
	}

	return metadata, endIndex, nil
}

// parseMetadataField parses a single metadata field line and updates the metadata struct
func parseMetadataField(line string, metadata *checkpointMetadata) error {
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
