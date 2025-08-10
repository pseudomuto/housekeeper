package migrator_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
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
					"checkpoint",
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
		revisions, err := migrator.LoadRevisions(ctx, mockCH)

		require.NoError(t, err)
		require.Len(t, revisions, 2)

		// Check first revision
		rev1 := revisions[0]
		require.Equal(t, "20240101120000_init", rev1.Version)
		require.Equal(t, executedAt, rev1.ExecutedAt)
		require.Equal(t, 2500*time.Millisecond, rev1.ExecutionTime)
		require.Equal(t, migrator.StandardRevision, rev1.Kind)
		require.Nil(t, rev1.Error)
		require.Equal(t, 5, rev1.Applied)
		require.Equal(t, 5, rev1.Total)
		require.Equal(t, "abc123hash", rev1.Hash)
		require.Equal(t, []string{"h1", "h2"}, rev1.PartialHashes)
		require.Equal(t, "1.0.0", rev1.HousekeeperVersion)

		// Check second revision
		rev2 := revisions[1]
		require.Equal(t, "20240102120000_users", rev2.Version)
		require.Equal(t, executedAt.Add(time.Hour), rev2.ExecutedAt)
		require.Equal(t, 1200*time.Millisecond, rev2.ExecutionTime)
		require.Equal(t, migrator.CheckpointRevision, rev2.Kind)
		require.NotNil(t, rev2.Error)
		require.Equal(t, "some error", *rev2.Error)
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
		revisions, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "database connection failed")
		require.Nil(t, revisions)
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
		revisions, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "scan failed")
		require.Nil(t, revisions)
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
		revisions, err := migrator.LoadRevisions(ctx, mockCH)

		require.Error(t, err)
		require.Contains(t, err.Error(), "rows iteration failed")
		require.Nil(t, revisions)
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
		revisions, err := migrator.LoadRevisions(ctx, mockCH)

		require.NoError(t, err)
		require.Empty(t, revisions)
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
