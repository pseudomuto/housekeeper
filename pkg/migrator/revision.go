package migrator

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RevisionKind constants define the types of migration revisions that can be recorded.
// These constants categorize different kinds of migration executions for tracking
// and rollback purposes.
const (
	// StandardRevision represents a normal migration execution containing
	// DDL statements that modify database schema. This is the most common
	// revision type for typical migration operations.
	StandardRevision RevisionKind = "migration"

	// CheckpointRevision represents a checkpoint marker in the migration
	// history, typically used for marking safe rollback points or
	// significant migration milestones. Checkpoints may not contain
	// actual DDL statements but serve as metadata markers.
	CheckpointRevision RevisionKind = "checkpoint"
)

type (
	ClickHouse interface {
		Query(context.Context, string, ...any) (driver.Rows, error)
	}

	// Revision represents a record of migration execution history, capturing
	// detailed information about when and how a migration was applied to
	// a ClickHouse database.
	//
	// Revisions provide complete audit trail information including execution
	// timing, success/failure status, and integrity verification data.
	// This information is essential for migration rollback, debugging,
	// and compliance tracking in production environments.
	//
	// Example usage:
	//   revision := &migrator.Revision{
	//       Version:            "20240101120000_create_users",
	//       ExecutedAt:         time.Now(),
	//       ExecutionTime:      2 * time.Second,
	//       Kind:               migrator.StandardRevision,
	//       Applied:            5,
	//       Total:              5,
	//       Hash:               "abc123...",
	//       PartialHashes:      []string{"hash1", "hash2", "hash3", "hash4", "hash5"},
	//       HousekeeperVersion: "1.0.0",
	//   }
	Revision struct {
		// Version is the unique identifier for the migration, typically
		// a timestamp-based string like "20240101120000_create_users".
		// Used for ordering and referencing specific migrations.
		Version string

		// ExecutedAt records the timestamp when the migration execution
		// began. Used for audit trails and determining migration order
		// in cases where version ordering is ambiguous.
		ExecutedAt time.Time

		// ExecutionTime records the total duration required to execute
		// all statements in the migration. Useful for performance
		// monitoring and identifying slow migrations.
		ExecutionTime time.Duration

		// Kind categorizes the type of revision (migration, checkpoint, etc.).
		// Determines how the revision should be processed during rollbacks
		// and migration analysis.
		Kind RevisionKind

		// Error contains the error message if the migration failed during
		// execution. Nil indicates successful execution. Used for debugging
		// failed migrations and determining rollback strategies.
		Error *string

		// Applied records the number of statements that were successfully
		// executed before completion or failure. Combined with Total,
		// this allows partial migration recovery and precise error reporting.
		Applied int

		// Total records the total number of statements in the migration.
		// Used to calculate completion percentage and validate that
		// all expected statements were processed.
		Total int

		// Hash contains the cryptographic hash of the complete migration
		// content, used for integrity verification and detecting
		// unauthorized migration modifications.
		Hash string

		// PartialHashes contains individual hashes for each statement
		// in the migration, enabling fine-grained integrity checking
		// and identification of specific statement modifications.
		PartialHashes []string

		// HousekeeperVersion records the version of the Housekeeper tool
		// that executed the migration. Used for compatibility tracking
		// and debugging version-specific migration behaviors.
		HousekeeperVersion string
	}

	// RevisionKind represents the category of a migration revision,
	// determining how it should be processed and what role it plays
	// in the overall migration lifecycle.
	//
	// Different revision kinds may have different rollback behaviors,
	// validation requirements, and execution priorities.
	RevisionKind string
)

func LoadRevisions(ctx context.Context, ch ClickHouse) ([]*Revision, error) {
	rows, err := ch.Query(ctx, `
		SELECT
			version,
			executed_at,
			execution_time_ms,
			kind,
			error,
			applied,
			total,
			hash,
			partial_hashes,
			housekeeper_version
		FROM housekeeper.revisions
		ORDER BY version ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var revisions []*Revision
	for rows.Next() {
		revision := &Revision{}
		var executionTimeMs int64
		var errorStr *string

		err := rows.Scan(
			&revision.Version,
			&revision.ExecutedAt,
			&executionTimeMs,
			&revision.Kind,
			&errorStr,
			&revision.Applied,
			&revision.Total,
			&revision.Hash,
			&revision.PartialHashes,
			&revision.HousekeeperVersion,
		)
		if err != nil {
			return nil, err
		}

		revision.ExecutionTime = time.Duration(executionTimeMs) * time.Millisecond
		if errorStr != nil {
			revision.Error = errorStr
		}

		revisions = append(revisions, revision)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return revisions, nil
}
