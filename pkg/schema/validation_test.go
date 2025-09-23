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
