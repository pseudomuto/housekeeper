package executor_test

import (
	"context"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/executor"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockClickHouse struct {
	queryFunc func(context.Context, string, ...any) (driver.Rows, error)
	execFunc  func(context.Context, string, ...any) error
	queries   []string
	execs     []string
}

func (m *mockClickHouse) Query(ctx context.Context, query string, args ...any) (driver.Rows, error) {
	m.queries = append(m.queries, query)
	if m.queryFunc != nil {
		return m.queryFunc(ctx, query, args...)
	}
	return &mockRows{}, nil
}

func (m *mockClickHouse) Exec(ctx context.Context, query string, args ...any) error {
	m.execs = append(m.execs, query)
	if m.execFunc != nil {
		return m.execFunc(ctx, query, args...)
	}
	return nil
}

type mockRows struct {
	nextCalled bool
	scanCalled bool
}

func (m *mockRows) Next() bool {
	if !m.nextCalled {
		m.nextCalled = true
		return true
	}
	return false
}

func (m *mockRows) Scan(dest ...any) error {
	m.scanCalled = true
	return nil
}

func (m *mockRows) Close() error {
	return nil
}

func (m *mockRows) Err() error {
	return nil
}

func (m *mockRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (m *mockRows) Columns() []string {
	return nil
}

func (m *mockRows) ScanStruct(dest any) error {
	return nil
}

func (m *mockRows) Totals(dest ...any) error {
	return nil
}

func TestNew(t *testing.T) {
	mockCH := &mockClickHouse{}
	formatter := format.New(format.Defaults)

	executor := executor.New(executor.Config{
		ClickHouse:         mockCH,
		Formatter:          formatter,
		HousekeeperVersion: "1.0.0",
	})

	assert.NotNil(t, executor)
}

func TestExecutor_IsBootstrapped(t *testing.T) {
	tests := []struct {
		name           string
		setupMock      func(*mockClickHouse)
		expectedResult bool
		expectError    bool
	}{
		{
			name: "fully bootstrapped",
			setupMock: func(m *mockClickHouse) {
				callCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					callCount++
					return &mockRows{}, nil
				}
			},
			expectedResult: true,
		},
		{
			name: "database missing",
			setupMock: func(m *mockClickHouse) {
				callCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					callCount++
					if callCount == 1 {
						// Database check - return empty result
						return &mockRows{nextCalled: true}, nil
					}
					return &mockRows{}, nil
				}
			},
			expectedResult: false,
		},
		{
			name: "table missing",
			setupMock: func(m *mockClickHouse) {
				callCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					callCount++
					if callCount == 1 {
						// Database exists
						return &mockRows{}, nil
					}
					// Table missing - return empty result
					return &mockRows{nextCalled: true}, nil
				}
			},
			expectedResult: false,
		},
		{
			name: "query error",
			setupMock: func(m *mockClickHouse) {
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					return nil, errors.New("connection failed")
				}
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCH := &mockClickHouse{}
			tt.setupMock(mockCH)

			executor := executor.New(executor.Config{
				ClickHouse:         mockCH,
				Formatter:          format.New(format.Defaults),
				HousekeeperVersion: "1.0.0",
			})

			result, err := executor.IsBootstrapped(context.Background())

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expectedResult, result)
			}
		})
	}
}

func TestExecutor_Execute(t *testing.T) {
	tests := []struct {
		name           string
		migrations     []*migrator.Migration
		setupMock      func(*mockClickHouse)
		expectedStatus []executor.ExecutionStatus
		expectError    bool
	}{
		{
			name: "successful execution",
			migrations: []*migrator.Migration{
				{
					Version: "20240101120000_test",
					Statements: []*parser.Statement{
						{
							CreateDatabase: &parser.CreateDatabaseStmt{
								Name: "test_db",
							},
						},
					},
				},
			},
			setupMock: func(m *mockClickHouse) {
				// Mock bootstrap checks
				queryCallCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					queryCallCount++
					if queryCallCount <= 2 {
						// Bootstrap checks - return that infrastructure exists
						return &mockRows{}, nil
					}
					// LoadRevisions query - return empty revisions
					return &mockRows{nextCalled: true}, nil
				}
			},
			expectedStatus: []executor.ExecutionStatus{executor.StatusSuccess},
		},
		{
			name: "migration already completed",
			migrations: []*migrator.Migration{
				{
					Version: "20240101120000_test",
					Statements: []*parser.Statement{
						{
							CreateDatabase: &parser.CreateDatabaseStmt{
								Name: "test_db",
							},
						},
					},
				},
			},
			setupMock: func(m *mockClickHouse) {
				// Mock bootstrap checks and existing revision
				queryCallCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					queryCallCount++
					if queryCallCount <= 2 {
						// Bootstrap checks - return that infrastructure exists
						return &mockRows{}, nil
					}
					// LoadRevisions query - return existing successful revision
					return &mockCompletedRevisionRows{}, nil
				}
			},
			expectedStatus: []executor.ExecutionStatus{executor.StatusSkipped},
		},
		{
			name: "execution failure",
			migrations: []*migrator.Migration{
				{
					Version: "20240101120000_test",
					Statements: []*parser.Statement{
						{
							CreateDatabase: &parser.CreateDatabaseStmt{
								Name: "test_db",
							},
						},
					},
				},
			},
			setupMock: func(m *mockClickHouse) {
				// Mock bootstrap checks
				queryCallCount := 0
				m.queryFunc = func(ctx context.Context, query string, args ...any) (driver.Rows, error) {
					queryCallCount++
					if queryCallCount <= 2 {
						// Bootstrap checks - return that infrastructure exists
						return &mockRows{}, nil
					}
					// LoadRevisions query - return empty revisions
					return &mockRows{nextCalled: true}, nil
				}

				// Mock execution failure
				m.execFunc = func(ctx context.Context, query string, args ...any) error {
					if len(args) > 0 {
						// This is the INSERT statement for revision - let it succeed
						return nil
					}
					// This is the migration statement - make it fail
					return errors.New("execution failed")
				}
			},
			expectedStatus: []executor.ExecutionStatus{executor.StatusFailed},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockCH := &mockClickHouse{}
			tt.setupMock(mockCH)

			executor := executor.New(executor.Config{
				ClickHouse:         mockCH,
				Formatter:          format.New(format.Defaults),
				HousekeeperVersion: "1.0.0",
			})

			results, err := executor.Execute(context.Background(), tt.migrations)

			if tt.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Len(t, results, len(tt.expectedStatus))

				for i, expectedStatus := range tt.expectedStatus {
					assert.Equal(t, expectedStatus, results[i].Status, "result %d status mismatch", i)
					assert.Equal(t, tt.migrations[i].Version, results[i].Version, "result %d version mismatch", i)
				}
			}
		})
	}
}

// mockCompletedRevisionRows simulates a successful revision in the database
type mockCompletedRevisionRows struct {
	nextCalled bool
}

func (m *mockCompletedRevisionRows) Next() bool {
	if !m.nextCalled {
		m.nextCalled = true
		return true
	}
	return false
}

func (m *mockCompletedRevisionRows) Scan(dest ...any) error {
	// Simulate scanning a completed revision
	if len(dest) < 10 {
		return nil
	}

	scanField := func(idx int, handler func(any)) {
		if idx < len(dest) {
			handler(dest[idx])
		}
	}

	scanField(0, func(d any) {
		if version, ok := d.(*string); ok {
			*version = "20240101120000_test"
		}
	})
	scanField(1, func(d any) {
		if executedAt, ok := d.(*time.Time); ok {
			*executedAt = time.Now()
		}
	})
	scanField(2, func(d any) {
		if executionTimeMs, ok := d.(*uint64); ok {
			*executionTimeMs = 1000
		}
	})
	scanField(3, func(d any) {
		if kind, ok := d.(*string); ok {
			*kind = string(migrator.StandardRevision)
		}
	})
	scanField(4, func(d any) {
		if errorStr, ok := d.(**string); ok {
			*errorStr = nil // No error
		}
	})
	scanField(5, func(d any) {
		if applied, ok := d.(*uint32); ok {
			*applied = 1
		}
	})
	scanField(6, func(d any) {
		if total, ok := d.(*uint32); ok {
			*total = 1
		}
	})
	scanField(7, func(d any) {
		if hash, ok := d.(*string); ok {
			*hash = "test-hash"
		}
	})
	scanField(8, func(d any) {
		if partialHashes, ok := d.(*[]string); ok {
			*partialHashes = []string{"test-hash"}
		}
	})
	scanField(9, func(d any) {
		if housekeeperVersion, ok := d.(*string); ok {
			*housekeeperVersion = "1.0.0"
		}
	})

	return nil
}

func (m *mockCompletedRevisionRows) Close() error {
	return nil
}

func (m *mockCompletedRevisionRows) Err() error {
	return nil
}

func (m *mockCompletedRevisionRows) ColumnTypes() []driver.ColumnType {
	return nil
}

func (m *mockCompletedRevisionRows) Columns() []string {
	return nil
}

func (m *mockCompletedRevisionRows) ScanStruct(dest any) error {
	return nil
}

func (m *mockCompletedRevisionRows) Totals(dest ...any) error {
	return nil
}
