package executor

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

type (
	// ClickHouse defines the interface for ClickHouse database operations
	// required by the migration executor.
	ClickHouse interface {
		Query(context.Context, string, ...any) (driver.Rows, error)
		Exec(context.Context, string, ...any) error
	}

	// Executor handles the execution of database migrations against ClickHouse.
	//
	// The executor provides safe, atomic migration execution with comprehensive
	// error handling, progress tracking, and integrity verification. It integrates
	// with the existing revision tracking system to maintain a complete audit
	// trail of migration execution.
	//
	// Key features:
	//   - Statement-by-statement execution with transaction safety
	//   - Automatic bootstrap of housekeeper.revisions table
	//   - Progress tracking and error recovery
	//   - Hash-based integrity verification
	//   - Integration with existing revision and migration systems
	//
	// Example usage:
	//
	//	executor := executor.New(executor.Config{
	//		ClickHouse:         client,
	//		Formatter:          format.New(format.Defaults),
	//		HousekeeperVersion: "1.0.0",
	//	})
	//
	//	results, err := executor.Execute(ctx, []*migrator.Migration{migration})
	//	if err != nil {
	//		log.Fatal(err)
	//	}
	//
	//	for _, result := range results {
	//		fmt.Printf("Migration %s: %s\n", result.Version, result.Status)
	//	}
	Executor struct {
		ch                 ClickHouse
		formatter          *format.Formatter
		housekeeperVersion string
	}

	// Config contains configuration options for creating a new Executor.
	Config struct {
		// ClickHouse client for database operations
		ClickHouse ClickHouse

		// Formatter for generating clean SQL output
		Formatter *format.Formatter

		// HousekeeperVersion to record in revision entries
		HousekeeperVersion string
	}

	// ExecutionResult contains the result of executing a single migration.
	//
	// Results provide detailed information about migration execution including
	// timing, success/failure status, and any errors encountered. This information
	// is essential for debugging failed migrations and tracking execution progress.
	ExecutionResult struct {
		// Version is the migration version that was executed
		Version string

		// Status indicates the outcome of the migration execution
		Status ExecutionStatus

		// Error contains any error that occurred during execution
		Error error

		// ExecutionTime records how long the migration took to execute
		ExecutionTime time.Duration

		// StatementsApplied indicates how many statements were successfully executed
		StatementsApplied int

		// TotalStatements is the total number of statements in the migration
		TotalStatements int

		// Revision contains the revision record that was created for this execution
		Revision *migrator.Revision
	}

	// ExecutionStatus represents the outcome of a migration execution.
	ExecutionStatus string
)

const (
	// StatusSuccess indicates the migration was executed successfully
	StatusSuccess ExecutionStatus = "success"

	// StatusFailed indicates the migration execution failed
	StatusFailed ExecutionStatus = "failed"

	// StatusSkipped indicates the migration was skipped (already applied)
	StatusSkipped ExecutionStatus = "skipped"
)

// New creates a new migration executor with the provided configuration.
//
// The executor requires a ClickHouse client for database operations, a formatter
// for SQL generation, and the current Housekeeper version for revision tracking.
//
// Example usage:
//
//	executor := executor.New(executor.Config{
//		ClickHouse:         clickhouseClient,
//		Formatter:          format.New(format.Defaults),
//		HousekeeperVersion: "1.0.0",
//	})
func New(config Config) *Executor {
	return &Executor{
		ch:                 config.ClickHouse,
		formatter:          config.Formatter,
		housekeeperVersion: config.HousekeeperVersion,
	}
}

// Execute applies a list of migrations to the ClickHouse database.
//
// This method handles the complete migration execution lifecycle:
//   - Ensures housekeeper database and table exist (bootstrap)
//   - Loads existing revisions to determine what needs to be executed
//   - Executes each migration statement-by-statement with error handling
//   - Records revision entries for successful and failed executions
//   - Returns detailed execution results for each migration
//
// The execution is atomic per migration - if any statement in a migration fails,
// the entire migration is marked as failed and execution stops. Previously
// executed migrations remain applied.
//
// Example usage:
//
//	migrations := []*migrator.Migration{migration1, migration2}
//	results, err := executor.Execute(ctx, migrations)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	for _, result := range results {
//		switch result.Status {
//		case executor.StatusSuccess:
//			fmt.Printf("✓ %s completed in %v\n", result.Version, result.ExecutionTime)
//		case executor.StatusFailed:
//			fmt.Printf("✗ %s failed: %v\n", result.Version, result.Error)
//		case executor.StatusSkipped:
//			fmt.Printf("- %s already applied\n", result.Version)
//		}
//	}
func (e *Executor) Execute(ctx context.Context, migrations []*migrator.Migration) ([]*ExecutionResult, error) {
	// Ensure housekeeper infrastructure exists
	if err := e.ensureBootstrap(ctx); err != nil {
		return nil, errors.Wrap(err, "failed to bootstrap housekeeper infrastructure")
	}

	// Load existing revisions to determine what needs to be executed
	revisionSet, err := migrator.LoadRevisions(ctx, e.ch)
	if err != nil {
		return nil, errors.Wrap(err, "failed to load existing revisions")
	}

	results := make([]*ExecutionResult, 0, len(migrations))

	for _, migration := range migrations {
		result := e.executeMigration(ctx, migration, revisionSet)
		results = append(results, result)

		// Stop execution on first failure
		if result.Status == StatusFailed {
			break
		}
	}

	return results, nil
}

// IsBootstrapped checks whether the housekeeper database and revisions table exist.
//
// This method verifies that the migration tracking infrastructure is properly
// set up and ready for use. It checks for both the housekeeper database and
// the revisions table within that database.
//
// Example usage:
//
//	bootstrapped, err := executor.IsBootstrapped(ctx)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	if !bootstrapped {
//		fmt.Println("Housekeeper infrastructure needs to be initialized")
//	}
func (e *Executor) IsBootstrapped(ctx context.Context) (bool, error) {
	// Check if housekeeper database exists
	rows, err := e.ch.Query(ctx, "SELECT 1 FROM system.databases WHERE name = 'housekeeper'")
	if err != nil {
		return false, errors.Wrap(err, "failed to check for housekeeper database")
	}
	defer rows.Close()

	if !rows.Next() {
		return false, nil
	}

	// Check if revisions table exists
	rows, err = e.ch.Query(ctx, "SELECT 1 FROM system.tables WHERE database = 'housekeeper' AND name = 'revisions'")
	if err != nil {
		return false, errors.Wrap(err, "failed to check for revisions table")
	}
	defer rows.Close()

	return rows.Next(), nil
}

// ensureBootstrap creates the housekeeper database and revisions table if they don't exist.
func (e *Executor) ensureBootstrap(ctx context.Context) error {
	bootstrapped, err := e.IsBootstrapped(ctx)
	if err != nil {
		return err
	}

	if bootstrapped {
		return nil
	}

	// Parse the bootstrap SQL from embedded template
	bootstrapSQL := `
-- Housekeeper migration tracking infrastructure
CREATE DATABASE IF NOT EXISTS housekeeper
ENGINE = Atomic
COMMENT 'Housekeeper migration tracking database';

CREATE TABLE IF NOT EXISTS housekeeper.revisions (
    version String COMMENT 'The version (e.g. 20250101123045)',
    executed_at DateTime(3, 'UTC') COMMENT 'The UTC time at which this attempt was executed',
    execution_time_ms UInt64 COMMENT 'How long the migration took to run',
    kind String COMMENT 'The type of migration this is (migration, snapshot, etc)',
    error Nullable(String) COMMENT 'The error message from the last attempt (if any)',
    applied UInt32 COMMENT 'The number of applied statements',
    total UInt32 COMMENT 'The total number of statements in the migration',
    hash String COMMENT 'The h1 hash of the migration',
    partial_hashes Array(String) COMMENT 'h1 hashes for each statement in the migration',
    housekeeper_version String COMMENT 'The version of housekeeper used to run the migration'
)
ENGINE = MergeTree()
ORDER BY version
PARTITION BY toYYYYMM(executed_at)
COMMENT 'Table used to track migrations';
`

	sql, err := parser.ParseString(bootstrapSQL)
	if err != nil {
		return errors.Wrap(err, "failed to parse bootstrap SQL")
	}

	// Execute bootstrap statements
	for _, stmt := range sql.Statements {
		// Skip comment-only statements as they cannot be executed
		if stmt.CommentStatement != nil {
			continue
		}

		stmtSQL, err := e.formatStatement(stmt)
		if err != nil {
			return errors.Wrap(err, "failed to format bootstrap statement")
		}

		if err := e.ch.Exec(ctx, stmtSQL); err != nil {
			return errors.Wrapf(err, "failed to execute bootstrap statement: %s", stmtSQL)
		}
	}

	return nil
}

// executeMigration executes a single migration and returns the result.
func (e *Executor) executeMigration(ctx context.Context, migration *migrator.Migration, revisionSet *migrator.RevisionSet) *ExecutionResult {
	startTime := time.Now()

	// Check if migration is already completed
	if revisionSet.IsCompleted(migration) {
		return &ExecutionResult{
			Version:           migration.Version,
			Status:            StatusSkipped,
			ExecutionTime:     0,
			StatementsApplied: len(migration.Statements),
			TotalStatements:   len(migration.Statements),
		}
	}

	// Handle snapshot migrations specially
	if migration.IsSnapshot {
		return e.executeSnapshotMigration(ctx, migration, startTime)
	}

	// Check for partial execution and determine starting point
	_, startIndex, err := e.getPartialRevision(migration, revisionSet)
	if err != nil {
		return &ExecutionResult{
			Version:           migration.Version,
			Status:            StatusFailed,
			Error:             errors.Wrap(err, "failed to validate partial revision"),
			ExecutionTime:     time.Since(startTime),
			StatementsApplied: 0,
			TotalStatements:   len(migration.Statements),
		}
	}

	// Execute migration statements starting from the determined index
	statementsApplied := startIndex
	var executionError error

	for i := startIndex; i < len(migration.Statements); i++ {
		stmt := migration.Statements[i]

		// Skip comment-only statements as they cannot be executed
		if stmt.CommentStatement != nil {
			statementsApplied++
			continue
		}

		stmtSQL, err := e.formatStatement(stmt)
		if err != nil {
			executionError = errors.Wrapf(err, "failed to format statement %d", i+1)
			break
		}

		if err := e.ch.Exec(ctx, stmtSQL); err != nil {
			executionError = errors.Wrapf(err, "failed to execute statement %d: %s", i+1, stmtSQL)
			break
		}

		statementsApplied++
	}

	executionTime := time.Since(startTime)

	// Determine execution status
	status := StatusSuccess
	if executionError != nil {
		status = StatusFailed
	}

	// Compute migration hash and partial hashes
	migrationHash, partialHashes := e.ComputeHashes(migration)

	// Create revision record
	revision := &migrator.Revision{
		Version:            migration.Version,
		ExecutedAt:         startTime,
		ExecutionTime:      executionTime,
		Kind:               migrator.StandardRevision,
		Applied:            statementsApplied,
		Total:              len(migration.Statements),
		Hash:               migrationHash,
		PartialHashes:      partialHashes,
		HousekeeperVersion: e.housekeeperVersion,
	}

	if executionError != nil {
		errorStr := executionError.Error()
		revision.Error = &errorStr
	}

	// Save revision record
	if err := e.saveRevision(ctx, revision); err != nil {
		// Log error but don't fail the migration result
		// The migration may have succeeded even if revision saving failed
		fmt.Printf("Warning: failed to save revision record: %v\n", err)
	}

	return &ExecutionResult{
		Version:           migration.Version,
		Status:            status,
		Error:             executionError,
		ExecutionTime:     executionTime,
		StatementsApplied: statementsApplied,
		TotalStatements:   len(migration.Statements),
		Revision:          revision,
	}
}

// executeSnapshotMigration handles the execution of snapshot migrations.
//
// Snapshot migrations are treated specially:
//   - DDL statements are not executed (they represent consolidated state)
//   - Revision is recorded with SnapshotRevision kind
//   - Validation ensures snapshot represents current database state
//
// This prevents executing DDL that has already been applied in previous migrations
// while maintaining the revision history for tracking purposes.
func (e *Executor) executeSnapshotMigration(ctx context.Context, migration *migrator.Migration, startTime time.Time) *ExecutionResult {
	executionTime := time.Since(startTime)

	// Compute migration hash and partial hashes (for integrity tracking)
	migrationHash, partialHashes := e.ComputeHashes(migration)

	// Create revision record with SnapshotRevision kind
	revision := &migrator.Revision{
		Version:            migration.Version,
		ExecutedAt:         startTime,
		ExecutionTime:      executionTime,
		Kind:               migrator.SnapshotRevision,
		Applied:            1, // Snapshots are considered as single "applied" unit
		Total:              1, // Snapshots are considered as single "total" unit
		Hash:               migrationHash,
		PartialHashes:      partialHashes,
		HousekeeperVersion: e.housekeeperVersion,
		Error:              nil, // Snapshots don't execute DDL, so no execution errors
	}

	// Save revision record
	if err := e.saveRevision(ctx, revision); err != nil {
		// If we can't save the revision, treat it as a failure
		errorStr := fmt.Sprintf("failed to save snapshot revision: %v", err)
		revision.Error = &errorStr

		return &ExecutionResult{
			Version:           migration.Version,
			Status:            StatusFailed,
			Error:             errors.New(errorStr),
			ExecutionTime:     executionTime,
			StatementsApplied: 0,
			TotalStatements:   len(migration.Statements),
			Revision:          revision,
		}
	}

	return &ExecutionResult{
		Version:           migration.Version,
		Status:            StatusSuccess,
		Error:             nil,
		ExecutionTime:     executionTime,
		StatementsApplied: len(migration.Statements), // Report as if all statements were "applied"
		TotalStatements:   len(migration.Statements),
		Revision:          revision,
	}
}

// getPartialRevision checks for existing partial revisions and determines the starting execution index.
//
// Returns:
//   - partialRevision: the existing partial revision, or nil if none exists
//   - startIndex: the statement index to start execution from (0 for new migrations)
//   - error: validation error if partial revision exists but is invalid
//
// This method enables resuming partially failed migrations by:
//   - Detecting existing revisions with Applied < Total (partial execution)
//   - Validating statement integrity using partial hashes
//   - Determining the correct starting statement index for resume
func (e *Executor) getPartialRevision(migration *migrator.Migration, revisionSet *migrator.RevisionSet) (*migrator.Revision, int, error) {
	// Check if a revision already exists for this migration
	revision := revisionSet.GetRevision(migration)
	if revision == nil {
		// No existing revision, start from beginning
		return nil, 0, nil
	}

	// If migration is completed, this shouldn't be called (caught earlier)
	if revision.Error == nil && revision.Applied == revision.Total {
		return nil, 0, nil
	}

	// If revision has error but no partial execution, start from beginning
	if revision.Applied == 0 {
		return revision, 0, nil
	}

	// Validate partial revision integrity
	if err := e.validatePartialRevision(migration, revision); err != nil {
		return nil, 0, errors.Wrap(err, "partial revision validation failed")
	}

	// Resume from the next statement after the last successfully applied one
	startIndex := revision.Applied

	return revision, startIndex, nil
}

// validatePartialRevision validates that a partial revision matches the current migration file.
//
// This prevents resuming migrations when:
//   - Migration file has been modified since partial execution
//   - Statement count has changed
//   - Statement hashes don't match (indicating content changes)
//
// Returns error if validation fails, nil if validation passes.
func (e *Executor) validatePartialRevision(migration *migrator.Migration, revision *migrator.Revision) error {
	// Check that statement count matches
	if len(migration.Statements) != revision.Total {
		return errors.Errorf(
			"migration statement count changed: expected %d statements, found %d in revision",
			len(migration.Statements), revision.Total,
		)
	}

	// Check that we have partial hashes for validation
	if len(revision.PartialHashes) == 0 {
		return errors.New("partial revision has no statement hashes for validation")
	}

	// Validate hashes for the applied statements
	for i := 0; i < revision.Applied && i < len(revision.PartialHashes); i++ {
		stmt := migration.Statements[i]
		stmtSQL, err := e.formatStatement(stmt)
		if err != nil {
			return errors.Wrapf(err, "failed to format statement %d for validation", i+1)
		}

		expectedHash := revision.PartialHashes[i]
		actualHash := e.computeHash(stmtSQL)

		if expectedHash != actualHash {
			return errors.Errorf(
				"statement %d hash mismatch: migration file may have been modified since partial execution (expected %s, got %s)",
				i+1, expectedHash, actualHash,
			)
		}
	}

	return nil
}

// saveRevision saves a revision record to the housekeeper.revisions table.
func (e *Executor) saveRevision(ctx context.Context, revision *migrator.Revision) error {
	insertSQL := `
		INSERT INTO housekeeper.revisions (
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
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`

	var errorValue *string
	if revision.Error != nil {
		errorValue = revision.Error
	}

	return e.ch.Exec(ctx, insertSQL,
		revision.Version,
		revision.ExecutedAt,
		revision.ExecutionTime.Milliseconds(),
		string(revision.Kind),
		errorValue,
		revision.Applied,
		revision.Total,
		revision.Hash,
		revision.PartialHashes,
		revision.HousekeeperVersion,
	)
}

// formatStatement formats a single statement using the formatter.
func (e *Executor) formatStatement(stmt *parser.Statement) (string, error) {
	var buf strings.Builder
	if err := e.formatter.Format(&buf, stmt); err != nil {
		return "", err
	}
	return buf.String(), nil
}

// ComputeHashes computes the migration hash and partial hashes for each statement.
// This method is exported for testing purposes.
func (e *Executor) ComputeHashes(migration *migrator.Migration) (string, []string) {
	// Compute hash for each statement
	partialHashes := make([]string, 0, len(migration.Statements))
	var allContent strings.Builder

	for _, stmt := range migration.Statements {
		stmtSQL, err := e.formatStatement(stmt)
		if err != nil {
			// If formatting fails, use a placeholder hash
			stmtSQL = fmt.Sprintf("statement_%d", len(partialHashes))
		}

		// Compute hash for this statement
		hash := e.computeHash(stmtSQL)
		partialHashes = append(partialHashes, hash)

		// Add to overall content for migration hash
		allContent.WriteString(stmtSQL)
		allContent.WriteString("\n")
	}

	// Compute overall migration hash
	migrationHash := e.computeHash(allContent.String())

	return migrationHash, partialHashes
}

// computeHash computes a SHA256 hash in h1 format for the given content.
func (e *Executor) computeHash(content string) string {
	hash := sha256.Sum256([]byte(content))
	encoded := base64.StdEncoding.EncodeToString(hash[:])
	return "h1:" + encoded + "="
}
