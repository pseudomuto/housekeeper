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

	// SnapshotRevision represents a snapshot marker in the migration
	// history, typically used for marking safe rollback points or
	// significant migration milestones. Snapshots may not contain
	// actual DDL statements but serve as metadata markers.
	SnapshotRevision RevisionKind = "snapshot"
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

		// Kind categorizes the type of revision (migration, snapshot, etc.).
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
		var executionTimeMs uint64
		var errorStr *string
		var kindStr string
		var applied uint32
		var total uint32

		err := rows.Scan(
			&revision.Version,
			&revision.ExecutedAt,
			&executionTimeMs,
			&kindStr,
			&errorStr,
			&applied,
			&total,
			&revision.Hash,
			&revision.PartialHashes,
			&revision.HousekeeperVersion,
		)
		if err != nil {
			return nil, errors.Wrap(err, "failed to scan revision row")
		}

		const maxDurationMs = 9223372036854775 // max safe milliseconds before overflow
		if executionTimeMs <= maxDurationMs {
			revision.ExecutionTime = time.Duration(int64(executionTimeMs)) * time.Millisecond
		} else {
			revision.ExecutionTime = time.Duration(1<<63 - 1) // max time.Duration
		}
		revision.Kind = RevisionKind(kindStr)
		revision.Applied = int(applied)
		revision.Total = int(total)
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
// Failed migrations, partially applied migrations, and snapshot revisions are not considered completed.
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

	// Check for errors
	if revision.Error != nil {
		return false
	}

	// Handle snapshot migrations differently
	if migration.IsSnapshot {
		// Snapshots are completed if they have a SnapshotRevision record without errors
		return revision.Kind == SnapshotRevision
	}

	// For standard migrations, must be StandardRevision without errors
	if revision.Kind != StandardRevision {
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
// and snapshot revisions are excluded from the results.
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

// GetLastSnapshot returns the most recent snapshot revision.
//
// Returns nil if no snapshot revision exists in the set.
//
// Example usage:
//
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if snapshot := revisionSet.GetLastSnapshot(); snapshot != nil {
//		fmt.Printf("Last snapshot: %s at %s\n",
//			snapshot.Version, snapshot.ExecutedAt.Format("2006-01-02"))
//	}
func (rs *RevisionSet) GetLastSnapshot() *Revision {
	var lastSnapshot *Revision

	// Iterate through ordered versions to find the last snapshot
	for i := len(rs.orderedVersions) - 1; i >= 0; i-- {
		revision := rs.revisions[rs.orderedVersions[i]]
		if revision.Kind == SnapshotRevision && revision.Error == nil {
			lastSnapshot = revision
			break
		}
	}

	return lastSnapshot
}

// GetMigrationsAfterSnapshot returns all successfully executed migrations after the last snapshot.
//
// If no snapshot exists, returns all successfully executed migrations.
//
// Example usage:
//
//	revisionSet, err := migrator.LoadRevisions(ctx, client)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	migrationsAfterSnapshot := revisionSet.GetMigrationsAfterSnapshot()
//	fmt.Printf("Found %d migrations after snapshot\n", len(migrationsAfterSnapshot))
func (rs *RevisionSet) GetMigrationsAfterSnapshot() []string {
	lastSnapshot := rs.GetLastSnapshot()
	if lastSnapshot == nil {
		return rs.GetExecutedVersions()
	}

	// Find the index of the snapshot
	snapshotIndex := -1
	for i, version := range rs.orderedVersions {
		if version == lastSnapshot.Version {
			snapshotIndex = i
			break
		}
	}

	if snapshotIndex == -1 {
		// Shouldn't happen, but return all executed versions as fallback
		return rs.GetExecutedVersions()
	}

	// Collect successful migrations after the snapshot
	var migrationsAfter []string
	for i := snapshotIndex + 1; i < len(rs.orderedVersions); i++ {
		revision := rs.revisions[rs.orderedVersions[i]]
		if revision.Kind == StandardRevision && revision.Error == nil {
			migrationsAfter = append(migrationsAfter, revision.Version)
		}
	}

	return migrationsAfter
}

// HasSnapshot returns true if there is at least one snapshot revision.
//
// Example usage:
//
//	if revisionSet.HasSnapshot() {
//		fmt.Println("Database has snapshot revisions")
//	}
func (rs *RevisionSet) HasSnapshot() bool {
	for _, revision := range rs.revisions {
		if revision.Kind == SnapshotRevision && revision.Error == nil {
			return true
		}
	}
	return false
}
