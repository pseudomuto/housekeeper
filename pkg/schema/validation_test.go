package schema

import (
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestIsIntegrationEngine(t *testing.T) {
	tests := []struct {
		name     string
		engine   string
		expected bool
	}{
		{"Kafka engine", "Kafka", true},
		{"Kafka with params", "Kafka('topic', 'group')", true},
		{"MySQL engine", "MySQL", true},
		{"MySQL with params", "MySQL('host:3306', 'db', 'user', 'pass')", true},
		{"PostgreSQL engine", "PostgreSQL", true},
		{"RabbitMQ engine", "RabbitMQ", true},
		{"MongoDB engine", "MongoDB", true},
		{"S3 engine", "S3", true},
		{"HDFS engine", "HDFS", true},
		{"URL engine", "URL", true},
		{"File engine", "File", true},
		{"MergeTree engine", "MergeTree", false},
		{"MergeTree with params", "MergeTree()", false},
		{"ReplicatedMergeTree", "ReplicatedMergeTree('/path', 'replica')", false},
		{"Memory engine", "Memory", false},
		{"Distributed engine", "Distributed('cluster', 'db', 'table')", false},
		{"Empty engine", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isIntegrationEngine(tt.engine)
			require.Equal(t, tt.expected, result, "Expected %v for engine %s", tt.expected, tt.engine)
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
				Engine:   "MergeTree()",
			},
			expectError: false,
		},
		{
			name: "valid - integration engine modification (uses DROP+CREATE)",
			current: &TableInfo{
				Name:     "kafka_events",
				Database: "analytics",
				Engine:   "Kafka('topic', 'group')",
			},
			target: &TableInfo{
				Name:     "kafka_events",
				Database: "analytics",
				Engine:   "Kafka('topic', 'group')",
				Columns: []ColumnInfo{
					{Name: "new_col", DataType: "String"},
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
				Engine:   "MySQL('host:3306', 'db', 'user', 'pass')",
			},
			expectError: false,
		},
		{
			name: "invalid - cluster change",
			current: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   "MergeTree()",
				Cluster:  "staging",
			},
			target: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   "MergeTree()",
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
				Engine:   "MergeTree()",
			},
			target: &TableInfo{
				Name:     "events",
				Database: "analytics",
				Engine:   "ReplacingMergeTree()",
			},
			expectError: true,
			errorType:   ErrUnsupported,
		},
		{
			name: "invalid - system table modification",
			current: &TableInfo{
				Name:     "databases",
				Database: "system",
				Engine:   "SystemDatabases",
			},
			target: &TableInfo{
				Name:     "databases",
				Database: "system",
				Engine:   "SystemDatabases",
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
				Engine:   "MergeTree()",
			},
			expectError: true,
			errorType:   ErrUnsupported,
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
