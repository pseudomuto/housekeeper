package schema

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

// Helper function to create a TableEngine from a name
func makeEngine(name string) *parser.TableEngine {
	if name == "" {
		return nil
	}
	return &parser.TableEngine{Name: name}
}

// Helper function to create a simple DataType from a name
func makeDataType(name string) *parser.DataType {
	if name == "" {
		return nil
	}
	return &parser.DataType{
		Simple: &parser.SimpleType{Name: name},
	}
}

// Helper function to create a simple expression for tests
func makeExpression(value string) *parser.Expression {
	return &parser.Expression{
		Or: &parser.OrExpression{
			And: &parser.AndExpression{
				Not: &parser.NotExpression{
					Comparison: &parser.ComparisonExpression{
						Addition: &parser.AdditionExpression{
							Multiplication: &parser.MultiplicationExpression{
								Unary: &parser.UnaryExpression{
									Primary: &parser.PrimaryExpression{
										Identifier: &parser.IdentifierExpr{
											Name: value,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func TestIsIntegrationEngine(t *testing.T) {
	tests := []struct {
		name     string
		engine   *parser.TableEngine
		expected bool
	}{
		{"Kafka engine", makeEngine("Kafka"), true},
		{"Kafka with params", makeEngine("Kafka"), true},
		{"MySQL engine", makeEngine("MySQL"), true},
		{"MySQL with params", makeEngine("MySQL"), true},
		{"PostgreSQL engine", makeEngine("PostgreSQL"), true},
		{"RabbitMQ engine", makeEngine("RabbitMQ"), true},
		{"MongoDB engine", makeEngine("MongoDB"), true},
		{"S3 engine", makeEngine("S3"), true},
		{"HDFS engine", makeEngine("HDFS"), true},
		{"URL engine", makeEngine("URL"), true},
		{"File engine", makeEngine("File"), true},
		{"MergeTree engine", makeEngine("MergeTree"), false},
		{"MergeTree with params", makeEngine("MergeTree"), false},
		{"ReplicatedMergeTree", makeEngine("ReplicatedMergeTree"), false},
		{"Memory engine", makeEngine("Memory"), false},
		{"Distributed engine", makeEngine("Distributed"), false},
		{"Empty engine", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isIntegrationEngine(tt.engine)
			require.Equal(t, tt.expected, result, "Expected %v for engine %v", tt.expected, tt.engine)
		})
	}
}

func TestIsSystemDatabase(t *testing.T) {
	tests := []struct {
		name     string
		database string
		expected bool
	}{
		{"system database", "system", true},
		{"INFORMATION_SCHEMA", "INFORMATION_SCHEMA", true},
		{"information_schema", "information_schema", true},
		{"regular database", "analytics", false},
		{"empty database", "", false},
		{"custom database", "my_db", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isSystemDatabase(tt.database)
			require.Equal(t, tt.expected, result, "Expected %v for database %s", tt.expected, tt.database)
		})
	}
}

func TestValidateTableOperation(t *testing.T) {
	tests := []struct {
		name        string
		current     *TableInfo
		target      *TableInfo
		expectError bool
		errorType   error
	}{
		{
			name:    "valid operation - normal table creation",
			current: nil,
			target: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   makeEngine("MergeTree"),
			},
			expectError: false,
		},
		{
			name: "valid - integration engine modification (uses DROP+CREATE)",
			current: &TableInfo{
				Name:     "kafka_events",
				Database: "analytics",
				Engine:   makeEngine("Kafka"),
			},
			target: &TableInfo{
				Name:     "kafka_events",
				Database: "analytics",
				Engine:   makeEngine("Kafka"),
				Columns: []ColumnInfo{
					{Name: "new_col", DataType: makeDataType("String")},
				},
			},
			expectError: false,
		},
		{
			name:    "valid - create integration engine table",
			current: nil,
			target: &TableInfo{
				Name:     "mysql_users",
				Database: "analytics",
				Engine:   makeEngine("MySQL"),
			},
			expectError: false,
		},
		{
			name: "invalid - cluster change",
			current: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   makeEngine("MergeTree"),
				Cluster:  "staging",
			},
			target: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   makeEngine("MergeTree"),
				Cluster:  "production",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - engine change",
			current: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   makeEngine("MergeTree"),
			},
			target: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   makeEngine("ReplacingMergeTree"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - system table modification",
			current: &TableInfo{
				Name:     "databases",
				Database: "system",
				Engine:   makeEngine("SystemDatabases"),
			},
			target: &TableInfo{
				Name:     "databases",
				Database: "system",
				Engine:   makeEngine("SystemDatabases"),
				Comment:  "Modified comment",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "invalid - create system table",
			current: nil,
			target: &TableInfo{
				Name:     "custom_table",
				Database: "system",
				Engine:   makeEngine("MergeTree"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "invalid - Distributed table with PRIMARY KEY clause",
			current: nil,
			target: &TableInfo{
				Name:       "events_distributed",
				Database:   "analytics",
				Engine:     makeEngine("Distributed"),
				PrimaryKey: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "invalid - Buffer table with multiple invalid clauses",
			current: nil,
			target: &TableInfo{
				Name:        "buffer_table",
				Database:    "analytics",
				Engine:      makeEngine("Buffer"),
				PrimaryKey:  makeExpression("id"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "valid - Distributed table with no restricted clauses",
			current: nil,
			target: &TableInfo{
				Name:     "events_distributed",
				Database: "analytics",
				Engine:   makeEngine("Distributed"),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableOperation(tt.current, tt.target)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				require.True(t, errors.Is(err, tt.errorType), "Expected error type %v, got %v", tt.errorType, err)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

func TestValidateDatabaseOperation(t *testing.T) {
	tests := []struct {
		name        string
		current     *DatabaseInfo
		target      *DatabaseInfo
		expectError bool
		errorType   error
	}{
		{
			name:    "valid operation - database creation",
			current: nil,
			target: &DatabaseInfo{
				Name:    "analytics",
				Engine:  "Atomic",
				Comment: "Analytics database",
			},
			expectError: false,
		},
		{
			name: "invalid - cluster change",
			current: &DatabaseInfo{
				Name:    "analytics",
				Engine:  "Atomic",
				Cluster: "staging",
			},
			target: &DatabaseInfo{
				Name:    "analytics",
				Engine:  "Atomic",
				Cluster: "production",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - engine change",
			current: &DatabaseInfo{
				Name:   "analytics",
				Engine: "Atomic",
			},
			target: &DatabaseInfo{
				Name:   "analytics",
				Engine: "Memory",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - system database modification",
			current: &DatabaseInfo{
				Name:   "system",
				Engine: "Atomic",
			},
			target: &DatabaseInfo{
				Name:    "system",
				Engine:  "Atomic",
				Comment: "Modified system database",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "invalid - create system database",
			current: nil,
			target: &DatabaseInfo{
				Name:   "system",
				Engine: "Atomic",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDatabaseOperation(tt.current, tt.target)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				require.True(t, errors.Is(err, tt.errorType), "Expected error type %v, got %v", tt.errorType, err)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

func TestValidateDictionaryOperation(t *testing.T) {
	tests := []struct {
		name        string
		current     *DictionaryInfo
		target      *DictionaryInfo
		expectError bool
		errorType   error
	}{
		{
			name:    "valid operation - dictionary creation",
			current: nil,
			target: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
			},
			expectError: false,
		},
		{
			name: "valid operation - dictionary drop",
			current: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
			},
			target:      nil,
			expectError: false,
		},
		{
			name: "valid operation - dictionary replacement",
			current: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
			},
			target: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
				Comment:  "Modified comment",
			},
			expectError: false, // Dictionary replacements are allowed
		},
		{
			name: "invalid - cluster change",
			current: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
				Cluster:  "staging",
			},
			target: &DictionaryInfo{
				Name:     "users_dict",
				Database: "analytics",
				Cluster:  "production",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - system dictionary modification",
			current: &DictionaryInfo{
				Name:     "dict",
				Database: "system",
			},
			target: &DictionaryInfo{
				Name:     "dict",
				Database: "system",
				Comment:  "Modified",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name:    "invalid - create system dictionary",
			current: nil,
			target: &DictionaryInfo{
				Name:     "new_dict",
				Database: "system",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateDictionaryOperation(tt.current, tt.target)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				require.True(t, errors.Is(err, tt.errorType), "Expected error type %v, got %v", tt.errorType, err)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

func TestValidateViewOperation(t *testing.T) {
	tests := []struct {
		name        string
		current     *ViewInfo
		target      *ViewInfo
		expectError bool
		errorType   error
	}{
		{
			name:    "valid operation - view creation",
			current: nil,
			target: &ViewInfo{
				Name:     "daily_stats",
				Database: "analytics",
				Query:    "SELECT date, count(*) FROM events GROUP BY date",
			},
			expectError: false,
		},
		{
			name: "invalid - cluster change",
			current: &ViewInfo{
				Name:     "daily_stats",
				Database: "analytics",
				Cluster:  "staging",
			},
			target: &ViewInfo{
				Name:     "daily_stats",
				Database: "analytics",
				Cluster:  "production",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "valid - materialized view query change (uses DROP+CREATE)",
			current: &ViewInfo{
				Name:           "mv_stats",
				Database:       "analytics",
				IsMaterialized: true,
				Query:          "SELECT date, count(*) FROM events GROUP BY date",
			},
			target: &ViewInfo{
				Name:           "mv_stats",
				Database:       "analytics",
				IsMaterialized: true,
				Query:          "SELECT date, count(*), sum(amount) FROM events GROUP BY date",
			},
			expectError: false, // Now handled by DROP+CREATE instead of error
		},
		{
			name: "valid - regular view query change",
			current: &ViewInfo{
				Name:           "daily_stats",
				Database:       "analytics",
				IsMaterialized: false,
				Query:          "SELECT date, count(*) FROM events GROUP BY date",
			},
			target: &ViewInfo{
				Name:           "daily_stats",
				Database:       "analytics",
				IsMaterialized: false,
				Query:          "SELECT date, count(*), sum(amount) FROM events GROUP BY date",
			},
			expectError: false,
		},
		{
			name: "invalid - system view modification",
			current: &ViewInfo{
				Name:     "tables",
				Database: "system",
			},
			target: &ViewInfo{
				Name:     "tables",
				Database: "system",
				Query:    "SELECT * FROM system.tables WHERE database != 'system'",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateViewOperation(tt.current, tt.target)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				require.True(t, errors.Is(err, tt.errorType), "Expected error type %v, got %v", tt.errorType, err)
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

func TestValidateTableClauses(t *testing.T) {
	tests := []struct {
		name        string
		table       *TableInfo
		expectError bool
		errorType   error
		errorMsg    string
	}{
		{
			name: "valid - MergeTree with all clauses",
			table: &TableInfo{
				Name:        "events",
				Database:    "analytics",
				Engine:      makeEngine("MergeTree"),
				PrimaryKey:  makeExpression("id"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
				OrderBy:     makeExpression("id"),
				SampleBy:    makeExpression("id"),
			},
			expectError: false,
		},
		{
			name: "valid - ReplicatedMergeTree with all clauses",
			table: &TableInfo{
				Name:        "events",
				Database:    "analytics",
				Engine:      makeEngine("ReplicatedMergeTree"),
				PrimaryKey:  makeExpression("id"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
				OrderBy:     makeExpression("id"),
				SampleBy:    makeExpression("id"),
			},
			expectError: false,
		},
		{
			name: "valid - Memory with ORDER BY and PRIMARY KEY",
			table: &TableInfo{
				Name:       "temp_data",
				Database:   "analytics",
				Engine:     makeEngine("Memory"),
				PrimaryKey: makeExpression("id"),
				OrderBy:    makeExpression("id"),
			},
			expectError: false,
		},
		{
			name: "valid - Distributed with only ENGINE",
			table: &TableInfo{
				Name:     "events_distributed",
				Database: "analytics",
				Engine:   makeEngine("Distributed"),
			},
			expectError: false,
		},
		{
			name: "invalid - Distributed with PRIMARY KEY",
			table: &TableInfo{
				Name:       "events_distributed",
				Database:   "analytics",
				Engine:     makeEngine("Distributed"),
				PrimaryKey: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PRIMARY KEY clause(s) not supported for Distributed tables",
		},
		{
			name: "invalid - Distributed with PARTITION BY",
			table: &TableInfo{
				Name:        "events_distributed",
				Database:    "analytics",
				Engine:      makeEngine("Distributed"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PARTITION BY clause(s) not supported for Distributed tables",
		},
		{
			name: "invalid - Distributed with ORDER BY",
			table: &TableInfo{
				Name:     "events_distributed",
				Database: "analytics",
				Engine:   makeEngine("Distributed"),
				OrderBy:  makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "ORDER BY clause(s) not supported for Distributed tables",
		},
		{
			name: "invalid - Distributed with SAMPLE BY",
			table: &TableInfo{
				Name:     "events_distributed",
				Database: "analytics",
				Engine:   makeEngine("Distributed"),
				SampleBy: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "SAMPLE BY clause(s) not supported for Distributed tables",
		},
		{
			name: "invalid - Distributed with multiple invalid clauses",
			table: &TableInfo{
				Name:        "events_distributed",
				Database:    "analytics",
				Engine:      makeEngine("Distributed"),
				PrimaryKey:  makeExpression("id"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
				OrderBy:     makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PRIMARY KEY, PARTITION BY, ORDER BY clause(s) not supported for Distributed tables",
		},
		{
			name: "invalid - Buffer with PRIMARY KEY",
			table: &TableInfo{
				Name:       "buffer_table",
				Database:   "analytics",
				Engine:     makeEngine("Buffer"),
				PrimaryKey: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PRIMARY KEY clause(s) not supported for Buffer tables",
		},
		{
			name: "invalid - Dictionary engine with ORDER BY",
			table: &TableInfo{
				Name:     "dict_table",
				Database: "analytics",
				Engine:   makeEngine("Dictionary"),
				OrderBy:  makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "ORDER BY clause(s) not supported for Dictionary tables",
		},
		{
			name: "invalid - View engine with PARTITION BY",
			table: &TableInfo{
				Name:        "view_table",
				Database:    "analytics",
				Engine:      makeEngine("View"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PARTITION BY clause(s) not supported for View tables",
		},
		{
			name: "invalid - LiveView with SAMPLE BY",
			table: &TableInfo{
				Name:     "live_view",
				Database: "analytics",
				Engine:   makeEngine("LiveView"),
				SampleBy: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "SAMPLE BY clause(s) not supported for LiveView tables",
		},
		{
			name: "invalid - Memory with PARTITION BY",
			table: &TableInfo{
				Name:        "memory_table",
				Database:    "analytics",
				Engine:      makeEngine("Memory"),
				PartitionBy: makeExpression("toYYYYMM(date)"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "PARTITION BY clause(s) not supported for Memory tables",
		},
		{
			name: "invalid - Memory with SAMPLE BY",
			table: &TableInfo{
				Name:     "memory_table",
				Database: "analytics",
				Engine:   makeEngine("Memory"),
				SampleBy: makeExpression("id"),
			},
			expectError: true,
			errorType:   ErrUnsupported,
			errorMsg:    "SAMPLE BY clause(s) not supported for Memory tables",
		},
		{
			name: "valid - nil engine",
			table: &TableInfo{
				Name:       "no_engine",
				Database:   "analytics",
				Engine:     nil,
				PrimaryKey: makeExpression("id"),
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTableClauses(tt.table)

			if tt.expectError {
				require.Error(t, err, "Expected error for test case: %s", tt.name)
				require.True(t, errors.Is(err, tt.errorType), "Expected error type %v, got %v", tt.errorType, err)
				if tt.errorMsg != "" {
					require.Contains(t, err.Error(), tt.errorMsg, "Expected error message to contain: %s", tt.errorMsg)
				}
			} else {
				require.NoError(t, err, "Expected no error for test case: %s", tt.name)
			}
		})
	}
}

func TestShouldCopyClause(t *testing.T) {
	tests := []struct {
		name       string
		engine     *parser.TableEngine
		clauseType string
		shouldCopy bool
	}{
		// MergeTree - supports all clauses
		{
			name:       "MergeTree supports PRIMARY KEY",
			engine:     makeEngine("MergeTree"),
			clauseType: "PRIMARY KEY",
			shouldCopy: true,
		},
		{
			name:       "MergeTree supports PARTITION BY",
			engine:     makeEngine("MergeTree"),
			clauseType: "PARTITION BY",
			shouldCopy: true,
		},
		{
			name:       "MergeTree supports ORDER BY",
			engine:     makeEngine("MergeTree"),
			clauseType: "ORDER BY",
			shouldCopy: true,
		},
		{
			name:       "MergeTree supports SAMPLE BY",
			engine:     makeEngine("MergeTree"),
			clauseType: "SAMPLE BY",
			shouldCopy: true,
		},

		// Distributed - restricts all main clauses
		{
			name:       "Distributed blocks PRIMARY KEY",
			engine:     makeEngine("Distributed"),
			clauseType: "PRIMARY KEY",
			shouldCopy: false,
		},
		{
			name:       "Distributed blocks PARTITION BY",
			engine:     makeEngine("Distributed"),
			clauseType: "PARTITION BY",
			shouldCopy: false,
		},
		{
			name:       "Distributed blocks ORDER BY",
			engine:     makeEngine("Distributed"),
			clauseType: "ORDER BY",
			shouldCopy: false,
		},
		{
			name:       "Distributed blocks SAMPLE BY",
			engine:     makeEngine("Distributed"),
			clauseType: "SAMPLE BY",
			shouldCopy: false,
		},

		// Buffer - restricts all main clauses
		{
			name:       "Buffer blocks PRIMARY KEY",
			engine:     makeEngine("Buffer"),
			clauseType: "PRIMARY KEY",
			shouldCopy: false,
		},
		{
			name:       "Buffer blocks PARTITION BY",
			engine:     makeEngine("Buffer"),
			clauseType: "PARTITION BY",
			shouldCopy: false,
		},
		{
			name:       "Buffer blocks ORDER BY",
			engine:     makeEngine("Buffer"),
			clauseType: "ORDER BY",
			shouldCopy: false,
		},
		{
			name:       "Buffer blocks SAMPLE BY",
			engine:     makeEngine("Buffer"),
			clauseType: "SAMPLE BY",
			shouldCopy: false,
		},

		// Memory - restricts PARTITION BY and SAMPLE BY only
		{
			name:       "Memory allows PRIMARY KEY",
			engine:     makeEngine("Memory"),
			clauseType: "PRIMARY KEY",
			shouldCopy: true,
		},
		{
			name:       "Memory allows ORDER BY",
			engine:     makeEngine("Memory"),
			clauseType: "ORDER BY",
			shouldCopy: true,
		},
		{
			name:       "Memory blocks PARTITION BY",
			engine:     makeEngine("Memory"),
			clauseType: "PARTITION BY",
			shouldCopy: false,
		},
		{
			name:       "Memory blocks SAMPLE BY",
			engine:     makeEngine("Memory"),
			clauseType: "SAMPLE BY",
			shouldCopy: false,
		},

		// View engines
		{
			name:       "View blocks PRIMARY KEY",
			engine:     makeEngine("View"),
			clauseType: "PRIMARY KEY",
			shouldCopy: false,
		},
		{
			name:       "LiveView blocks ORDER BY",
			engine:     makeEngine("LiveView"),
			clauseType: "ORDER BY",
			shouldCopy: false,
		},
		{
			name:       "Dictionary blocks PARTITION BY",
			engine:     makeEngine("Dictionary"),
			clauseType: "PARTITION BY",
			shouldCopy: false,
		},

		// Unknown engines
		{
			name:       "Unknown engine allows all clauses",
			engine:     makeEngine("UnknownEngine"),
			clauseType: "PRIMARY KEY",
			shouldCopy: true,
		},

		// Nil engine
		{
			name:       "Nil engine allows all clauses",
			engine:     nil,
			clauseType: "PRIMARY KEY",
			shouldCopy: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := shouldCopyClause(tt.engine, tt.clauseType)
			require.Equal(t, tt.shouldCopy, result, "Expected %v for %s engine with %s clause", tt.shouldCopy,
				func() string {
					if tt.engine == nil {
						return "nil"
					}
					return tt.engine.Name
				}(), tt.clauseType)
		})
	}
}
