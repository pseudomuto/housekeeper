package migrator

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
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

	// RevisionSet represents a collection of migration revisions with convenient
	// query methods for determining migration execution status.
	//
	// This abstraction provides a clean interface for checking whether migrations
	// have been executed, failed, or are pending, encapsulating the logic for
	// handling different revision kinds and error states.
	RevisionSet struct {
		// revisions contains all revisions indexed by version for fast lookup
		revisions map[string]*Revision

		// orderedVersions maintains the order of revisions as they appear in the database
		orderedVersions []string
	}
)

// NewRevisionSet creates a new RevisionSet from a slice of revisions.
//
// The RevisionSet provides convenient methods for querying migration status
// without requiring callers to understand the internal revision structure
// or filtering logic.
//
// Example usage:
//
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Check if a specific migration is completed
//	if revisionSet.IsCompleted(migration) {
//		fmt.Printf("Migration %s is completed\n", migration.Version)
//	}
//
//	// Get all pending migrations
//	pending := revisionSet.GetPending(migrationDir)
//	fmt.Printf("Found %d pending migrations\n", len(pending))
func NewRevisionSet(revisions []*Revision) *RevisionSet {
	revisionMap := make(map[string]*Revision)
	orderedVersions := make([]string, 0, len(revisions))

	for _, revision := range revisions {
		revisionMap[revision.Version] = revision
		orderedVersions = append(orderedVersions, revision.Version)
	}

	return &RevisionSet{
		revisions:       revisionMap,
		orderedVersions: orderedVersions,
	}
}

// LoadRevisions loads revisions from ClickHouse and returns them as a RevisionSet.
//
// This provides a clean object-oriented API for querying migration status with
// intuitive method calls like IsCompleted() and IsPending().
//
// Example usage:
//
//	// Load revisions with the modern API
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Clean, readable status checks
//	for _, migration := range migrationDir.Migrations {
//		if revisionSet.IsCompleted(migration) {
//			fmt.Printf("✓ %s completed\n", migration.Version)
//		} else if revisionSet.IsPending(migration) {
//			fmt.Printf("⏳ %s pending\n", migration.Version)
//		}
//	}
//
//	// Bulk operations
//	pending := revisionSet.GetPending(migrationDir)
//
// Returns an error if the database query fails.
func LoadRevisions(ctx context.Context, ch ClickHouse) (*RevisionSet, error) {
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
		return nil, errors.Wrap(err, "failed to load revisions")
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
			return nil, errors.Wrap(err, "failed to scan revision row")
		}

		revision.ExecutionTime = time.Duration(executionTimeMs) * time.Millisecond
		if errorStr != nil {
			revision.Error = errorStr
		}

		revisions = append(revisions, revision)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "failed to iterate revision rows")
	}

	return NewRevisionSet(revisions), nil
}

// IsCompleted returns true if the migration has been successfully executed.
//
// A migration is considered completed if:
//   - There exists a revision with the same version
//   - The revision kind is StandardRevision
//   - The revision has no error (successful execution)
//   - All statements were applied (applied == total)
//   - The number of statements matches the revision total count
//
// Failed migrations, partially applied migrations, and checkpoint revisions are not considered completed.
//
// Example usage:
//
//	if revisionSet.IsCompleted(migration) {
//		fmt.Printf("✓ %s is completed\n", migration.Version)
//	} else {
//		fmt.Printf("⏳ %s needs to be run\n", migration.Version)
//	}
func (rs *RevisionSet) IsCompleted(migration *Migration) bool {
	revision, exists := rs.revisions[migration.Version]
	if !exists {
		return false
	}

	// Must be StandardRevision without errors
	if revision.Kind != StandardRevision || revision.Error != nil {
		return false
	}

	// All statements must have been applied
	if revision.Applied != revision.Total {
		return false
	}

	// Verify that the number of statements in the migration matches the total in the revision
	// This provides basic integrity verification without requiring exact hash matching
	if len(migration.Statements) != revision.Total {
		return false
	}

	return true
}

// IsFailed returns true if the migration has been attempted but failed.
//
// A migration is considered failed if:
//   - There exists a revision with the same version
//   - The revision kind is StandardRevision
//   - The revision has an error (failed execution)
//
// Example usage:
//
//	if revisionSet.IsFailed(migration) {
//		revision := revisionSet.GetRevision(migration)
//		fmt.Printf("✗ %s failed: %s\n", migration.Version, *revision.Error)
//	}
func (rs *RevisionSet) IsFailed(migration *Migration) bool {
	revision, exists := rs.revisions[migration.Version]
	if !exists {
		return false
	}

	// StandardRevision entries with errors are considered failed
	return revision.Kind == StandardRevision && revision.Error != nil
}

// IsPending returns true if the migration has not been successfully executed.
//
// A migration is considered pending if it is not completed, regardless of
// whether it has failed before or has no revision record.
//
// This is equivalent to !IsCompleted(migration).
//
// Example usage:
//
//	if revisionSet.IsPending(migration) {
//		fmt.Printf("⏳ %s is pending\n", migration.Version)
//	}
func (rs *RevisionSet) IsPending(migration *Migration) bool {
	return !rs.IsCompleted(migration)
}

// GetRevision returns the revision record for a migration, if it exists.
//
// Returns nil if no revision exists for the migration version.
//
// Example usage:
//
//	if revision := revisionSet.GetRevision(migration); revision != nil {
//		fmt.Printf("Executed at: %s\n", revision.ExecutedAt.Format("2006-01-02 15:04:05"))
//		fmt.Printf("Execution time: %v\n", revision.ExecutionTime)
//	}
func (rs *RevisionSet) GetRevision(migration *Migration) *Revision {
	return rs.revisions[migration.Version]
}

// GetPending returns all migrations that have not been successfully executed.
//
// This method filters the migrations in a MigrationDir to return only those
// that are pending execution. The order of migrations is preserved from the
// original MigrationDir.
//
// Example usage:
//
//	pending := revisionSet.GetPending(migrationDir)
//	fmt.Printf("Found %d pending migrations:\n", len(pending))
//	for _, migration := range pending {
//		fmt.Printf("  - %s\n", migration.Version)
//	}
func (rs *RevisionSet) GetPending(migrationDir *MigrationDir) []*Migration {
	if migrationDir == nil {
		return make([]*Migration, 0)
	}

	pending := make([]*Migration, 0)
	for _, migration := range migrationDir.Migrations {
		if rs.IsPending(migration) {
			pending = append(pending, migration)
		}
	}

	return pending
}

// GetCompleted returns all migrations that have been successfully executed.
//
// This method filters the migrations in a MigrationDir to return only those
// that have been completed. The order of migrations is preserved from the
// original MigrationDir.
//
// Example usage:
//
//	completed := revisionSet.GetCompleted(migrationDir)
//	fmt.Printf("Found %d completed migrations:\n", len(completed))
//	for _, migration := range completed {
//		fmt.Printf("  ✓ %s\n", migration.Version)
//	}
func (rs *RevisionSet) GetCompleted(migrationDir *MigrationDir) []*Migration {
	if migrationDir == nil {
		return make([]*Migration, 0)
	}

	completed := make([]*Migration, 0)
	for _, migration := range migrationDir.Migrations {
		if rs.IsCompleted(migration) {
			completed = append(completed, migration)
		}
	}

	return completed
}

// GetFailed returns all migrations that have been attempted but failed.
//
// This method filters the migrations in a MigrationDir to return only those
// that have failed during execution. The order of migrations is preserved
// from the original MigrationDir.
//
// Example usage:
//
//	failed := revisionSet.GetFailed(migrationDir)
//	if len(failed) > 0 {
//		fmt.Printf("Found %d failed migrations:\n", len(failed))
//		for _, migration := range failed {
//			revision := revisionSet.GetRevision(migration)
//			fmt.Printf("  ✗ %s: %s\n", migration.Version, *revision.Error)
//		}
//	}
func (rs *RevisionSet) GetFailed(migrationDir *MigrationDir) []*Migration {
	if migrationDir == nil {
		return make([]*Migration, 0)
	}

	failed := make([]*Migration, 0)
	for _, migration := range migrationDir.Migrations {
		if rs.IsFailed(migration) {
			failed = append(failed, migration)
		}
	}

	return failed
}

// GetExecutedVersions returns a slice of migration versions that have been
// successfully executed.
//
// Only successful StandardRevision entries are included. Failed migrations
// and checkpoint revisions are excluded from the results.
//
// The versions are returned in the order they appear in the original revisions
// slice, which typically corresponds to execution order.
//
// Example usage:
//
//	executed := revisionSet.GetExecutedVersions()
//	fmt.Printf("Executed migrations:\n")
//	for _, version := range executed {
//		fmt.Printf("  ✓ %s\n", version)
//	}
func (rs *RevisionSet) GetExecutedVersions() []string {
	executed := make([]string, 0)
	for _, version := range rs.orderedVersions {
		revision := rs.revisions[version]
		if revision.Kind == StandardRevision && revision.Error == nil {
			executed = append(executed, version)
		}
	}

	return executed
}

// Count returns the total number of revisions in the set.
func (rs *RevisionSet) Count() int {
	return len(rs.revisions)
}

// HasRevision returns true if a revision exists for the given version.
func (rs *RevisionSet) HasRevision(version string) bool {
	_, exists := rs.revisions[version]
	return exists
}

// GetLastCheckpoint returns the most recent checkpoint revision.
//
// Returns nil if no checkpoint revision exists in the set.
//
// Example usage:
//
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if checkpoint := revisionSet.GetLastCheckpoint(); checkpoint != nil {
//		fmt.Printf("Last checkpoint: %s at %s\n",
//			checkpoint.Version, checkpoint.ExecutedAt.Format("2006-01-02"))
//	}
func (rs *RevisionSet) GetLastCheckpoint() *Revision {
	var lastCheckpoint *Revision

	// Iterate through ordered versions to find the last checkpoint
	for i := len(rs.orderedVersions) - 1; i >= 0; i-- {
		revision := rs.revisions[rs.orderedVersions[i]]
		if revision.Kind == CheckpointRevision && revision.Error == nil {
			lastCheckpoint = revision
			break
		}
	}

	return lastCheckpoint
}

// GetMigrationsAfterCheckpoint returns all successfully executed migrations after the last checkpoint.
//
// If no checkpoint exists, returns all successfully executed migrations.
//
// Example usage:
//
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	migrationsAfterCheckpoint := revisionSet.GetMigrationsAfterCheckpoint()
//	fmt.Printf("Found %d migrations after checkpoint\n", len(migrationsAfterCheckpoint))
func (rs *RevisionSet) GetMigrationsAfterCheckpoint() []string {
	lastCheckpoint := rs.GetLastCheckpoint()
	if lastCheckpoint == nil {
		return rs.GetExecutedVersions()
	}

	// Find the index of the checkpoint
	checkpointIndex := -1
	for i, version := range rs.orderedVersions {
		if version == lastCheckpoint.Version {
			checkpointIndex = i
			break
		}
	}

	if checkpointIndex == -1 {
		// Shouldn't happen, but return all executed versions as fallback
		return rs.GetExecutedVersions()
	}

	// Collect successful migrations after the checkpoint
	var migrationsAfter []string
	for i := checkpointIndex + 1; i < len(rs.orderedVersions); i++ {
		revision := rs.revisions[rs.orderedVersions[i]]
		if revision.Kind == StandardRevision && revision.Error == nil {
			migrationsAfter = append(migrationsAfter, revision.Version)
		}
	}

	return migrationsAfter
}

// HasCheckpoint returns true if there is at least one checkpoint revision.
//
// Example usage:
//
//	if revisionSet.HasCheckpoint() {
//		fmt.Println("Database has checkpoint revisions")
//	}
func (rs *RevisionSet) HasCheckpoint() bool {
	for _, revision := range rs.revisions {
		if revision.Kind == CheckpointRevision && revision.Error == nil {
			return true
		}
	}
	return false
}
