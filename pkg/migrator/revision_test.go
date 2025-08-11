package migrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

type (
	// mockClickHouse implements the ClickHouse interface for testing
	mockClickHouse struct {
		queryFunc func(ctx context.Context, query string, args ...any) (driver.Rows, error)
	}

	// mockRows implements driver.Rows for testing
	mockRows struct {
		data      [][]any
		current   int
		closed    bool
		scanErr   error
		rowsErr   error
		nextCount int
	}
)

func TestLoadRevisions(t *testing.T) {
	t.Run("successful_load", func(t *testing.T) {
		executedAt := time.Date(2024, 8, 10, 14, 30, 0, 0, time.UTC)
		errorMsg := "some error"

		mockRows := &mockRows{
			data: [][]any{
				{
					"20240101120000_init", // version
					executedAt,            // executed_at
					int64(2500),           // execution_time_ms
					"migration",           // kind
					nil,                   // error (null)
					5,                     // applied
					5,                     // total
					"abc123hash",          // hash
					[]string{"h1", "h2"},  // partial_hashes
					"1.0.0",               // housekeeper_version
				},
				{
					"20240102120000_users",
					executedAt.Add(time.Hour),
					int64(1200),
					"snapshot",
					errorMsg, // error (not null)
					3,
					5,
					"def456hash",
					[]string{"h3", "h4", "h5"},
					"1.0.1",
				},
			},
		}

		mockCH := &mockClickHouse{
			queryFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
				require.Contains(t, query, "FROM housekeeper.revisions")
				require.Contains(t, query, "ORDER BY version ASC")
				return mockRows, nil
			},
		}

		ctx := context.Background()
		revisionSet, err := migrator.LoadRevisions(ctx, mockCH)

		require.NoError(t, err)
		require.Equal(t, 2, revisionSet.Count())

		// Check first revision via RevisionSet
		require.True(t, revisionSet.HasRevision("20240101120000_init"))
		migration1 := &migrator.Migration{
			Version:    "20240101120000_init",
			Statements: make([]*parser.Statement, 5), // 5 statements to match revision
		}
		rev1 := revisionSet.GetRevision(migration1)
		require.NotNil(t, rev1)
		require.Equal(t, "20240101120000_init", rev1.Version)
		require.Equal(t, executedAt, rev1.ExecutedAt)
		require.Equal(t, 2500*time.Millisecond, rev1.ExecutionTime)
		require.Equal(t, migrator.StandardRevision, rev1.Kind)
		require.Nil(t, rev1.Error)
		require.True(t, revisionSet.IsCompleted(migration1))
		require.Equal(t, 5, rev1.Applied)
		require.Equal(t, 5, rev1.Total)
		require.Equal(t, "abc123hash", rev1.Hash)
		require.Equal(t, []string{"h1", "h2"}, rev1.PartialHashes)
		require.Equal(t, "1.0.0", rev1.HousekeeperVersion)

		// Check second revision via RevisionSet
		require.True(t, revisionSet.HasRevision("20240102120000_users"))
		migration2 := &migrator.Migration{
			Version:    "20240102120000_users",
			Statements: make([]*parser.Statement, 5), // 5 statements to match revision
		}
		rev2 := revisionSet.GetRevision(migration2)
		require.NotNil(t, rev2)
		require.Equal(t, "20240102120000_users", rev2.Version)
		require.Equal(t, executedAt.Add(time.Hour), rev2.ExecutedAt)
		require.Equal(t, 1200*time.Millisecond, rev2.ExecutionTime)
		require.Equal(t, migrator.SnapshotRevision, rev2.Kind)
		require.NotNil(t, rev2.Error)
		require.Equal(t, "some error", *rev2.Error)
		require.False(t, revisionSet.IsCompleted(migration2)) // Snapshot not considered completed
		require.Equal(t, 3, rev2.Applied)
		require.Equal(t, 5, rev2.Total)
		require.Equal(t, "def456hash", rev2.Hash)
		require.Equal(t, []string{"h3", "h4", "h5"}, rev2.PartialHashes)
		require.Equal(t, "1.0.1", rev2.HousekeeperVersion)

		require.True(t, mockRows.closed)
	})

	t.Run("query_error", func(t *testing.T) {
		mockCH := &mockClickHouse{
			queryFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
				return nil, errors.New("database connection failed")
			},
		}

		ctx := context.Background()
		revisionSet, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "database connection failed")
		require.Nil(t, revisionSet)
	})

	t.Run("scan_error", func(t *testing.T) {
		mockRows := &mockRows{
			data: [][]any{
				{"20240101120000_init", time.Now(), int64(1000), "migration", nil, 1, 1, "hash", []string{}, "1.0.0"},
			},
			scanErr: errors.New("scan failed"),
		}

		mockCH := &mockClickHouse{
			queryFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
				return mockRows, nil
			},
		}

		ctx := context.Background()
		revisionSet, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "scan failed")
		require.Nil(t, revisionSet)
	})

	t.Run("rows_error", func(t *testing.T) {
		mockRows := &mockRows{
			data:    [][]any{},
			rowsErr: errors.New("rows iteration failed"),
		}

		mockCH := &mockClickHouse{
			queryFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
				return mockRows, nil
			},
		}

		ctx := context.Background()
		revisionSet, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "rows iteration failed")
		require.Nil(t, revisionSet)
	})

	t.Run("empty_result", func(t *testing.T) {
		mockRows := &mockRows{
			data: [][]any{},
		}

		mockCH := &mockClickHouse{
			queryFunc: func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
				return mockRows, nil
			},
		}

		ctx := context.Background()
		revisionSet, err := migrator.LoadRevisions(ctx, mockCH)

		require.NoError(t, err)
		require.Equal(t, 0, revisionSet.Count())
		require.True(t, mockRows.closed)
	})
}

func (m *mockClickHouse) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query, args...)
	}
	return nil, errors.New("no query function set")
}

func (m *mockRows) Next() bool {
	if m.closed {
		return false
	}
	m.nextCount++
	if m.current < len(m.data) {
		m.current++
		return true
	}
	return false
}

func (m *mockRows) Scan(dest ...any) error {
	if m.scanErr != nil {
		return m.scanErr
	}
	if m.current <= 0 || m.current > len(m.data) {
		return errors.New("no current row")
	}

	row := m.data[m.current-1]
	if len(dest) != len(row) {
		return errors.New("column count mismatch")
	}

	for i, val := range row {
		switch d := dest[i].(type) {
		case *string:
			if s, ok := val.(string); ok {
				*d = s
			}
		case **string:
			if val == nil {
				*d = nil
			} else if s, ok := val.(string); ok {
				str := s
				*d = &str
			}
		case *time.Time:
			if t, ok := val.(time.Time); ok {
				*d = t
			}
		case *int64:
			if i, ok := val.(int64); ok {
				*d = i
			}
		case *int:
			if i, ok := val.(int); ok {
				*d = i
			}
		case *migrator.RevisionKind:
			if s, ok := val.(string); ok {
				*d = migrator.RevisionKind(s)
			}
		case *[]string:
			if arr, ok := val.([]string); ok {
				*d = arr
			} else if val == nil {
				*d = nil
			}
		}
	}

	return nil
}

func (m *mockRows) Close() error {
	m.closed = true
	return nil
}

func (m *mockRows) Err() error {
	return m.rowsErr
}

func (m *mockRows) ScanStruct(dest any) error {
	return errors.New("ScanStruct not implemented in mock")
}

func (m *mockRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (m *mockRows) Totals(dest ...any) error {
	return errors.New("Totals not implemented in mock")
}

func (m *mockRows) Columns() []string {
	return []string{
		"version", "executed_at", "execution_time_ms", "kind", "error",
		"applied", "total", "hash", "partial_hashes", "housekeeper_version",
	}
}

// RevisionSet Tests

func TestNewRevisionSet(t *testing.T) {
	revisions := []*migrator.Revision{
		{Version: "001_create_users", Kind: migrator.StandardRevision, Error: nil},
		{Version: "002_add_email", Kind: migrator.StandardRevision, Error: stringPtr("failed")},
		{Version: "snapshot_001", Kind: migrator.SnapshotRevision, Error: nil},
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	require.Equal(t, 3, revisionSet.Count())
	require.True(t, revisionSet.HasRevision("001_create_users"))
	require.True(t, revisionSet.HasRevision("002_add_email"))
	require.True(t, revisionSet.HasRevision("snapshot_001"))
	require.False(t, revisionSet.HasRevision("nonexistent"))
}

func TestRevisionSet_IsCompleted(t *testing.T) {
	now := time.Now()
	revisions := []*migrator.Revision{
		{
			Version:       "001_create_users",
			ExecutedAt:    now,
			Kind:          migrator.StandardRevision,
			Error:         nil, // successful
			Applied:       3,
			Total:         3,
			PartialHashes: []string{"hash1", "hash2", "hash3"},
		},
		{
			Version:       "002_add_email",
			ExecutedAt:    now,
			Kind:          migrator.StandardRevision,
			Error:         stringPtr("syntax error"), // failed
			Applied:       2,
			Total:         3,
			PartialHashes: []string{"hash1", "hash2"},
		},
		{
			Version:       "003_partial_apply",
			ExecutedAt:    now,
			Kind:          migrator.StandardRevision,
			Error:         nil, // no error but partially applied
			Applied:       2,
			Total:         3,
			PartialHashes: []string{"hash1", "hash2"},
		},
		{
			Version:       "004_statement_mismatch",
			ExecutedAt:    now,
			Kind:          migrator.StandardRevision,
			Error:         nil,
			Applied:       3,
			Total:         3, // 3 statements in revision, but migration only has 2
			PartialHashes: []string{"hash1", "hash2", "hash3"},
		},
		{
			Version:       "snapshot_001",
			ExecutedAt:    now,
			Kind:          migrator.SnapshotRevision,
			Error:         nil, // snapshot
			Applied:       1,
			Total:         1,
			PartialHashes: []string{"snapshot_hash"},
		},
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	tests := []struct {
		name      string
		migration *migrator.Migration
		expected  bool
		reason    string
	}{
		{
			name: "fully_completed_migration",
			migration: &migrator.Migration{
				Version:    "001_create_users",
				Statements: make([]*parser.Statement, 3), // 3 statements to match revision.Total
			},
			expected: true,
			reason:   "Migration with matching applied==total and partial hashes should be completed",
		},
		{
			name: "failed_standard_revision",
			migration: &migrator.Migration{
				Version:    "002_add_email",
				Statements: make([]*parser.Statement, 3), // 3 statements
			},
			expected: false,
			reason:   "Standard revision with error should not be completed",
		},
		{
			name: "partially_applied_migration",
			migration: &migrator.Migration{
				Version:    "003_partial_apply",
				Statements: make([]*parser.Statement, 3), // 3 statements (but only 2 applied)
			},
			expected: false,
			reason:   "Migration with applied < total should not be completed",
		},
		{
			name: "statement_count_mismatch",
			migration: &migrator.Migration{
				Version:    "004_statement_mismatch",
				Statements: make([]*parser.Statement, 2), // 2 statements, but revision has 3 total
			},
			expected: false,
			reason:   "Migration with mismatched statement count should not be completed",
		},
		{
			name: "snapshot_revision",
			migration: &migrator.Migration{
				Version:    "snapshot_001",
				Statements: make([]*parser.Statement, 1), // 1 statement
			},
			expected: false,
			reason:   "Snapshot revisions should not be considered completed migrations",
		},
		{
			name: "no_revision_exists",
			migration: &migrator.Migration{
				Version:    "005_create_orders",
				Statements: make([]*parser.Statement, 1), // 1 statement
			},
			expected: false,
			reason:   "Migration with no revision should not be completed",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := revisionSet.IsCompleted(tt.migration)
			require.Equal(t, tt.expected, result, tt.reason)
		})
	}
}

func TestRevisionSet_IsPending(t *testing.T) {
	now := time.Now()
	revisions := []*migrator.Revision{
		{
			Version:    "001_create_users",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil, // successful
		},
		{
			Version:    "002_add_email",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      stringPtr("syntax error"), // failed
		},
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	tests := []struct {
		name      string
		migration *migrator.Migration
		expected  bool
		reason    string
	}{
		{
			name: "completed_migration",
			migration: &migrator.Migration{
				Version:    "001_create_users",
				Statements: make([]*parser.Statement, 0), // Empty statements
			},
			expected: false,
			reason:   "Completed migration should not be pending",
		},
		{
			name: "failed_migration",
			migration: &migrator.Migration{
				Version:    "002_add_email",
				Statements: make([]*parser.Statement, 0), // Empty statements
			},
			expected: true,
			reason:   "Failed migration should be pending for retry",
		},
		{
			name: "no_revision_exists",
			migration: &migrator.Migration{
				Version:    "003_create_orders",
				Statements: make([]*parser.Statement, 0), // Empty statements
			},
			expected: true,
			reason:   "Migration with no revision should be pending",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := revisionSet.IsPending(tt.migration)
			require.Equal(t, tt.expected, result, tt.reason)
		})
	}
}

func TestRevisionSet_GetPending_NilMigrationDir(t *testing.T) {
	revisions := []*migrator.Revision{}
	revisionSet := migrator.NewRevisionSet(revisions)

	pending := revisionSet.GetPending(nil)

	require.NotNil(t, pending)
	require.Empty(t, pending)
}

func TestRevisionSet_EmptySet(t *testing.T) {
	revisionSet := migrator.NewRevisionSet([]*migrator.Revision{})

	migration := &migrator.Migration{
		Version:    "001_test",
		Statements: make([]*parser.Statement, 0), // Empty statements
	}

	require.Equal(t, 0, revisionSet.Count())
	require.False(t, revisionSet.HasRevision("001_test"))
	require.False(t, revisionSet.IsCompleted(migration))
	require.False(t, revisionSet.IsFailed(migration))
	require.True(t, revisionSet.IsPending(migration))
	require.Nil(t, revisionSet.GetRevision(migration))
}

// Helper function for creating string pointers
func TestRevisionSet_GetCompleted(t *testing.T) {
	now := time.Now()
	revisions := []*migrator.Revision{
		{
			Version:    "001_create_users",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil, // completed
		},
		{
			Version:    "002_add_email",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      stringPtr("failed"), // failed, not completed
		},
	}

	migrations := []*migrator.Migration{
		{Version: "001_create_users", Statements: nil},
		{Version: "002_add_email", Statements: nil},
		{Version: "003_create_orders", Statements: nil}, // no revision
	}

	migrationDir := &migrator.MigrationDir{
		Migrations: migrations,
		SumFile:    migrator.NewSumFile(),
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	completed := revisionSet.GetCompleted(migrationDir)

	require.Len(t, completed, 1)
	require.Equal(t, "001_create_users", completed[0].Version)
}

func TestRevisionSet_GetFailed(t *testing.T) {
	now := time.Now()
	revisions := []*migrator.Revision{
		{
			Version:    "001_create_users",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil, // completed
		},
		{
			Version:    "002_add_email",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      stringPtr("syntax error"), // failed
		},
		{
			Version:    "003_create_orders",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      stringPtr("connection timeout"), // failed
		},
	}

	migrations := []*migrator.Migration{
		{Version: "001_create_users", Statements: nil},
		{Version: "002_add_email", Statements: nil},
		{Version: "003_create_orders", Statements: nil},
		{Version: "004_add_indexes", Statements: nil}, // no revision
	}

	migrationDir := &migrator.MigrationDir{
		Migrations: migrations,
		SumFile:    migrator.NewSumFile(),
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	failed := revisionSet.GetFailed(migrationDir)

	require.Len(t, failed, 2)
	require.Equal(t, "002_add_email", failed[0].Version)
	require.Equal(t, "003_create_orders", failed[1].Version)
}

func TestRevisionSet_GetExecutedVersions(t *testing.T) {
	now := time.Now()
	revisions := []*migrator.Revision{
		{
			Version:    "001_create_users",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil, // executed
		},
		{
			Version:    "002_add_email",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      stringPtr("failed"), // failed, not executed
		},
		{
			Version:    "003_create_orders",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil, // executed
		},
		{
			Version:    "snapshot_001",
			ExecutedAt: now,
			Kind:       migrator.SnapshotRevision,
			Error:      nil, // snapshot, not counted
		},
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	executed := revisionSet.GetExecutedVersions()

	expectedVersions := []string{"001_create_users", "003_create_orders"}
	require.Equal(t, expectedVersions, executed)
}

func TestRevisionSet_SnapshotMethods(t *testing.T) {
	now := time.Now()

	revisions := []*migrator.Revision{
		{
			Version:    "001_init",
			ExecutedAt: now.Add(-time.Hour * 3),
			Kind:       migrator.StandardRevision,
			Error:      nil,
		},
		{
			Version:    "002_users",
			ExecutedAt: now.Add(-time.Hour * 2),
			Kind:       migrator.StandardRevision,
			Error:      nil,
		},
		{
			Version:    "003_snapshot",
			ExecutedAt: now.Add(-time.Hour),
			Kind:       migrator.SnapshotRevision,
			Error:      nil,
		},
		{
			Version:    "004_products",
			ExecutedAt: now.Add(-time.Minute * 30),
			Kind:       migrator.StandardRevision,
			Error:      nil,
		},
		{
			Version:    "005_orders",
			ExecutedAt: now,
			Kind:       migrator.StandardRevision,
			Error:      nil,
		},
	}

	revisionSet := migrator.NewRevisionSet(revisions)

	t.Run("HasSnapshot", func(t *testing.T) {
		require.True(t, revisionSet.HasSnapshot())

		// Test with no snapshots
		noSnapshotRevisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Error:   nil,
			},
		}
		noSnapshotSet := migrator.NewRevisionSet(noSnapshotRevisions)
		require.False(t, noSnapshotSet.HasSnapshot())
	})

	t.Run("GetLastSnapshot", func(t *testing.T) {
		snapshot := revisionSet.GetLastSnapshot()
		require.NotNil(t, snapshot)
		require.Equal(t, "003_snapshot", snapshot.Version)
		require.Equal(t, migrator.SnapshotRevision, snapshot.Kind)

		// Test with multiple snapshots - should return the last one
		multiSnapshotRevisions := []*migrator.Revision{
			{
				Version:    "001_snapshot_old",
				ExecutedAt: now.Add(-time.Hour * 2),
				Kind:       migrator.SnapshotRevision,
				Error:      nil,
			},
			{
				Version:    "002_migration",
				ExecutedAt: now.Add(-time.Hour),
				Kind:       migrator.StandardRevision,
				Error:      nil,
			},
			{
				Version:    "003_snapshot_new",
				ExecutedAt: now,
				Kind:       migrator.SnapshotRevision,
				Error:      nil,
			},
		}
		multiSnapshotSet := migrator.NewRevisionSet(multiSnapshotRevisions)
		lastSnapshot := multiSnapshotSet.GetLastSnapshot()
		require.NotNil(t, lastSnapshot)
		require.Equal(t, "003_snapshot_new", lastSnapshot.Version)

		// Test with no snapshots
		noSnapshotRevisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Error:   nil,
			},
		}
		noSnapshotSet := migrator.NewRevisionSet(noSnapshotRevisions)
		require.Nil(t, noSnapshotSet.GetLastSnapshot())
	})

	t.Run("GetMigrationsAfterSnapshot", func(t *testing.T) {
		migrationsAfter := revisionSet.GetMigrationsAfterSnapshot()

		// Should return migrations after the last snapshot
		expected := []string{"004_products", "005_orders"}
		require.Equal(t, expected, migrationsAfter)

		// Test with no snapshots - should return all executed versions
		noSnapshotRevisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Error:   nil,
			},
			{
				Version: "002_users",
				Kind:    migrator.StandardRevision,
				Error:   nil,
			},
		}
		noSnapshotSet := migrator.NewRevisionSet(noSnapshotRevisions)
		allMigrations := noSnapshotSet.GetMigrationsAfterSnapshot()
		require.Equal(t, []string{"001_init", "002_users"}, allMigrations)

		// Test with failed migrations after snapshot - should be excluded
		withFailedRevisions := []*migrator.Revision{
			{
				Version: "001_snapshot",
				Kind:    migrator.SnapshotRevision,
				Error:   nil,
			},
			{
				Version: "002_success",
				Kind:    migrator.StandardRevision,
				Error:   nil,
			},
			{
				Version: "003_failed",
				Kind:    migrator.StandardRevision,
				Error:   stringPtr("migration failed"),
			},
		}
		withFailedSet := migrator.NewRevisionSet(withFailedRevisions)
		afterFailed := withFailedSet.GetMigrationsAfterSnapshot()
		require.Equal(t, []string{"002_success"}, afterFailed)
	})
}

func stringPtr(s string) *string {
	return &s
}
