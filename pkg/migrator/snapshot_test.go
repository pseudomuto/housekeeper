package migrator_test

import (
	"strings"
	"testing"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/stretchr/testify/require"
)

func TestIsSnapshot(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "valid snapshot file",
			content:  "-- housekeeper:snapshot\n-- version: test\n",
			expected: true,
			wantErr:  false,
		},
		{
			name:     "regular migration file",
			content:  "CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;",
			expected: false,
			wantErr:  false,
		},
		{
			name:     "snapshot marker with leading spaces",
			content:  "  -- housekeeper:snapshot\n",
			expected: true,
			wantErr:  false,
		},
		{
			name:     "empty file",
			content:  "",
			expected: false,
			wantErr:  false,
		},
		{
			name:     "snapshot marker not on first line",
			content:  "-- Some comment\n-- housekeeper:snapshot\n",
			expected: false,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.content)
			isSnapshot, err := migrator.IsSnapshot(reader)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, isSnapshot)
			}
		})
	}
}

func TestLoadSnapshot(t *testing.T) {
	validSnapshot := `-- housekeeper:snapshot
-- version: 20240810120000_snapshot
-- description: Test snapshot
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init,002_users,003_products
-- cumulative_hash: abc123def456789

CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`

	t.Run("valid snapshot", func(t *testing.T) {
		reader := strings.NewReader(validSnapshot)
		snapshot, err := migrator.LoadSnapshot(reader)

		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, "20240810120000_snapshot", snapshot.Version)
		require.Equal(t, "Test snapshot", snapshot.Description)
		require.Equal(t, "abc123def456789", snapshot.CumulativeHash)
		require.Equal(t, []string{"001_init", "002_users", "003_products"}, snapshot.IncludedMigrations)
		require.Len(t, snapshot.Statements, 2)

		// Check parsed time
		expectedTime, _ := time.Parse(time.RFC3339, "2024-08-10T12:00:00Z")
		require.Equal(t, expectedTime, snapshot.CreatedAt)
	})

	t.Run("missing snapshot marker", func(t *testing.T) {
		content := `-- version: test
CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadSnapshot(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing snapshot marker")
	})

	t.Run("missing required version field", func(t *testing.T) {
		content := `-- housekeeper:snapshot
-- description: Test
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init
-- cumulative_hash: abc123

CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadSnapshot(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing required version field")
	})

	t.Run("invalid SQL content", func(t *testing.T) {
		content := `-- housekeeper:snapshot
-- version: test
-- included_migrations: 001_init

INVALID SQL HERE;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadSnapshot(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse snapshot SQL")
	})
}

func TestGenerateSnapshot(t *testing.T) {
	// Create test migrations by parsing actual SQL
	migration1, err := migrator.LoadMigration("001_init", strings.NewReader("CREATE DATABASE test ENGINE = Atomic COMMENT 'Test database';"))
	require.NoError(t, err)

	migration2, err := migrator.LoadMigration("002_users", strings.NewReader("CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"))
	require.NoError(t, err)

	t.Run("generate snapshot from migrations", func(t *testing.T) {
		migrations := []*migrator.Migration{migration1, migration2}
		snapshot, err := migrator.GenerateSnapshot(
			"20240810120000_snapshot",
			"Test snapshot",
			migrations,
		)

		require.NoError(t, err)
		require.NotNil(t, snapshot)
		require.Equal(t, "20240810120000_snapshot", snapshot.Version)
		require.Equal(t, "Test snapshot", snapshot.Description)
		require.Equal(t, []string{"001_init", "002_users"}, snapshot.IncludedMigrations)
		require.Len(t, snapshot.Statements, 2)
		require.NotEmpty(t, snapshot.CumulativeHash)
		require.False(t, snapshot.CreatedAt.IsZero())
	})

	t.Run("empty migration list", func(t *testing.T) {
		_, err := migrator.GenerateSnapshot(
			"test",
			"description",
			[]*migrator.Migration{},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty migration list")
	})
}

func TestSnapshotWriteTo(t *testing.T) {
	// Create a snapshot with parsed SQL statements
	migration, err := migrator.LoadMigration("test", strings.NewReader("CREATE DATABASE test ENGINE = Atomic;"))
	require.NoError(t, err)

	snapshot := &migrator.Snapshot{
		Version:            "20240810120000_snapshot",
		Description:        "Test snapshot",
		CreatedAt:          time.Date(2024, 8, 10, 12, 0, 0, 0, time.UTC),
		IncludedMigrations: []string{"001_init", "002_users"},
		CumulativeHash:     "abc123def456",
		Statements:         migration.Statements,
	}

	var buf strings.Builder
	_, err = snapshot.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()

	// Check for required components
	require.Contains(t, output, "-- housekeeper:snapshot")
	require.Contains(t, output, "-- version: 20240810120000_snapshot")
	require.Contains(t, output, "-- description: Test snapshot")
	require.Contains(t, output, "-- created_at: 2024-08-10T12:00:00Z")
	require.Contains(t, output, "-- included_migrations: 001_init,002_users")
	require.Contains(t, output, "-- cumulative_hash: abc123def456")
	require.Contains(t, output, "CREATE DATABASE")
}

func TestSnapshotValidateAgainstRevisions(t *testing.T) {
	snapshot := &migrator.Snapshot{
		Version:            "snapshot_001",
		IncludedMigrations: []string{"001_init", "002_users", "003_products"},
	}

	t.Run("all migrations completed", func(t *testing.T) {
		revisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
				Error:   nil,
			},
			{
				Version: "002_users",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
				Error:   nil,
			},
			{
				Version: "003_products",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
				Error:   nil,
			},
		}
		revisionSet := migrator.NewRevisionSet(revisions)

		// The validation only checks if revisions exist and are completed at the revision level
		// It doesn't need access to actual migration objects
		err := snapshot.ValidateAgainstRevisions(revisionSet)
		require.NoError(t, err)
	})

	t.Run("missing migration revision", func(t *testing.T) {
		revisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
			},
			{
				Version: "002_users",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
			},
			// 003_products is missing
		}
		revisionSet := migrator.NewRevisionSet(revisions)

		err := snapshot.ValidateAgainstRevisions(revisionSet)
		require.Error(t, err)
		require.Contains(t, err.Error(), "003_products which has no revision record")
	})

	t.Run("migration with error", func(t *testing.T) {
		errorMsg := "failed to execute"
		revisions := []*migrator.Revision{
			{
				Version: "001_init",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
			},
			{
				Version: "002_users",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
				Error:   &errorMsg,
			},
			{
				Version: "003_products",
				Kind:    migrator.StandardRevision,
				Applied: 1,
				Total:   1,
			},
		}
		revisionSet := migrator.NewRevisionSet(revisions)

		err := snapshot.ValidateAgainstRevisions(revisionSet)
		require.Error(t, err)
		require.Contains(t, err.Error(), "002_users which is not completed")
	})
}
