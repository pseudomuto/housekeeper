package migrator_test

import (
	"strings"
	"testing"
	"time"

	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/stretchr/testify/require"
)

func TestIsCheckpoint(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected bool
		wantErr  bool
	}{
		{
			name:     "valid checkpoint file",
			content:  "-- housekeeper:checkpoint\n-- version: test\n",
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
			name:     "checkpoint marker with leading spaces",
			content:  "  -- housekeeper:checkpoint\n",
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
			name:     "checkpoint marker not on first line",
			content:  "-- Some comment\n-- housekeeper:checkpoint\n",
			expected: false,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := strings.NewReader(tt.content)
			isCheckpoint, err := migrator.IsCheckpoint(reader)

			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expected, isCheckpoint)
			}
		})
	}
}

func TestLoadCheckpoint(t *testing.T) {
	validCheckpoint := `-- housekeeper:checkpoint
-- version: 20240810120000_checkpoint
-- description: Test checkpoint
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init,002_users,003_products
-- cumulative_hash: abc123def456789

CREATE DATABASE test ENGINE = Atomic;
CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;`

	t.Run("valid checkpoint", func(t *testing.T) {
		reader := strings.NewReader(validCheckpoint)
		checkpoint, err := migrator.LoadCheckpoint(reader)

		require.NoError(t, err)
		require.NotNil(t, checkpoint)
		require.Equal(t, "20240810120000_checkpoint", checkpoint.Version)
		require.Equal(t, "Test checkpoint", checkpoint.Description)
		require.Equal(t, "abc123def456789", checkpoint.CumulativeHash)
		require.Equal(t, []string{"001_init", "002_users", "003_products"}, checkpoint.IncludedMigrations)
		require.Len(t, checkpoint.Statements, 2)

		// Check parsed time
		expectedTime, _ := time.Parse(time.RFC3339, "2024-08-10T12:00:00Z")
		require.Equal(t, expectedTime, checkpoint.CreatedAt)
	})

	t.Run("missing checkpoint marker", func(t *testing.T) {
		content := `-- version: test
CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadCheckpoint(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing checkpoint marker")
	})

	t.Run("missing required version field", func(t *testing.T) {
		content := `-- housekeeper:checkpoint
-- description: Test
-- created_at: 2024-08-10T12:00:00Z
-- included_migrations: 001_init
-- cumulative_hash: abc123

CREATE TABLE test (id UInt64) ENGINE = MergeTree() ORDER BY id;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadCheckpoint(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "missing required version field")
	})

	t.Run("invalid SQL content", func(t *testing.T) {
		content := `-- housekeeper:checkpoint
-- version: test
-- included_migrations: 001_init

INVALID SQL HERE;`
		reader := strings.NewReader(content)
		_, err := migrator.LoadCheckpoint(reader)
		require.Error(t, err)
		require.Contains(t, err.Error(), "failed to parse checkpoint SQL")
	})
}

func TestGenerateCheckpoint(t *testing.T) {
	// Create test migrations by parsing actual SQL
	migration1, err := migrator.LoadMigration("001_init", strings.NewReader("CREATE DATABASE test ENGINE = Atomic COMMENT 'Test database';"))
	require.NoError(t, err)

	migration2, err := migrator.LoadMigration("002_users", strings.NewReader("CREATE TABLE test.users (id UInt64) ENGINE = MergeTree() ORDER BY id;"))
	require.NoError(t, err)

	t.Run("generate checkpoint from migrations", func(t *testing.T) {
		migrations := []*migrator.Migration{migration1, migration2}
		checkpoint, err := migrator.GenerateCheckpoint(
			"20240810120000_checkpoint",
			"Test checkpoint",
			migrations,
		)

		require.NoError(t, err)
		require.NotNil(t, checkpoint)
		require.Equal(t, "20240810120000_checkpoint", checkpoint.Version)
		require.Equal(t, "Test checkpoint", checkpoint.Description)
		require.Equal(t, []string{"001_init", "002_users"}, checkpoint.IncludedMigrations)
		require.Len(t, checkpoint.Statements, 2)
		require.NotEmpty(t, checkpoint.CumulativeHash)
		require.False(t, checkpoint.CreatedAt.IsZero())
	})

	t.Run("empty migration list", func(t *testing.T) {
		_, err := migrator.GenerateCheckpoint(
			"test",
			"description",
			[]*migrator.Migration{},
		)
		require.Error(t, err)
		require.Contains(t, err.Error(), "empty migration list")
	})
}

func TestCheckpointWriteTo(t *testing.T) {
	// Create a checkpoint with parsed SQL statements
	migration, err := migrator.LoadMigration("test", strings.NewReader("CREATE DATABASE test ENGINE = Atomic;"))
	require.NoError(t, err)

	checkpoint := &migrator.Checkpoint{
		Version:            "20240810120000_checkpoint",
		Description:        "Test checkpoint",
		CreatedAt:          time.Date(2024, 8, 10, 12, 0, 0, 0, time.UTC),
		IncludedMigrations: []string{"001_init", "002_users"},
		CumulativeHash:     "abc123def456",
		Statements:         migration.Statements,
	}

	var buf strings.Builder
	_, err = checkpoint.WriteTo(&buf)
	require.NoError(t, err)

	output := buf.String()

	// Check for required components
	require.Contains(t, output, "-- housekeeper:checkpoint")
	require.Contains(t, output, "-- version: 20240810120000_checkpoint")
	require.Contains(t, output, "-- description: Test checkpoint")
	require.Contains(t, output, "-- created_at: 2024-08-10T12:00:00Z")
	require.Contains(t, output, "-- included_migrations: 001_init,002_users")
	require.Contains(t, output, "-- cumulative_hash: abc123def456")
	require.Contains(t, output, "CREATE DATABASE")
}

func TestCheckpointValidateAgainstRevisions(t *testing.T) {
	checkpoint := &migrator.Checkpoint{
		Version:            "checkpoint_001",
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
		err := checkpoint.ValidateAgainstRevisions(revisionSet)
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

		err := checkpoint.ValidateAgainstRevisions(revisionSet)
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

		err := checkpoint.ValidateAgainstRevisions(revisionSet)
		require.Error(t, err)
		require.Contains(t, err.Error(), "002_users which is not completed")
	})
}
