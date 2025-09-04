package project

import (
	"io"
	"io/fs"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestGenerateImage(t *testing.T) {
	tests := []struct {
		name      string
		sql       string
		fileTests []fileTest // separate tests for each file/content pair
	}{
		{
			name: "single database with table",
			sql: `
				CREATE DATABASE analytics ENGINE = Atomic COMMENT 'Analytics DB';
				CREATE TABLE analytics.events (
					id UInt64,
					name String
				) ENGINE = MergeTree() ORDER BY id;
			`,
			fileTests: []fileTest{
				{"db/main.sql", "-- housekeeper:import schemas/analytics/schema.sql"},
				{"db/schemas/analytics/schema.sql", "CREATE DATABASE `analytics` ENGINE = Atomic COMMENT 'Analytics DB'"},
				{"db/schemas/analytics/schema.sql", "-- housekeeper:import tables/events.sql"},
				{"db/schemas/analytics/tables/events.sql", "CREATE TABLE `analytics`.`events`"},
				{"db/schemas/analytics/tables/events.sql", "`id`   UInt64"},
				{"db/schemas/analytics/tables/events.sql", "`name` String"},
				{"db/schemas/analytics/tables/events.sql", "ENGINE = MergeTree()"},
				{"db/schemas/analytics/tables/events.sql", "ORDER BY `id`"},
			},
		},
		{
			name: "multiple databases with mixed objects",
			sql: `
				CREATE DATABASE analytics ENGINE = Atomic;
				CREATE DATABASE users ENGINE = Atomic;
				
				CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree() ORDER BY id;
				CREATE TABLE users.profiles (id UInt64) ENGINE = MergeTree() ORDER BY id;
				
				CREATE DICTIONARY analytics.lookup (
					id UInt64 IS_OBJECT_ID,
					name String
				) PRIMARY KEY id
				SOURCE(HTTP(url 'http://example.com'))
				LAYOUT(HASHED())
				LIFETIME(3600);
				
				CREATE VIEW users.active AS SELECT * FROM profiles WHERE active = 1;
			`,
			fileTests: []fileTest{
				{"db/main.sql", "-- housekeeper:import schemas/analytics/schema.sql"},
				{"db/main.sql", "-- housekeeper:import schemas/users/schema.sql"},
				{"db/schemas/analytics/schema.sql", "CREATE DATABASE `analytics` ENGINE = Atomic"},
				{"db/schemas/analytics/schema.sql", "-- housekeeper:import tables/events.sql"},
				{"db/schemas/analytics/schema.sql", "-- housekeeper:import dictionaries/lookup.sql"},
				{"db/schemas/users/schema.sql", "CREATE DATABASE `users` ENGINE = Atomic"},
				{"db/schemas/users/schema.sql", "-- housekeeper:import tables/profiles.sql"},
				{"db/schemas/users/schema.sql", "-- housekeeper:import views/active.sql"},
				{"db/schemas/analytics/tables/events.sql", "`id` UInt64"},
				{"db/schemas/users/tables/profiles.sql", "`id` UInt64"},
				{"db/schemas/analytics/dictionaries/lookup.sql", "CREATE DICTIONARY `analytics`.`lookup`"},
				{"db/schemas/analytics/dictionaries/lookup.sql", "PRIMARY KEY `id`"},
				{"db/schemas/users/views/active.sql", "CREATE VIEW `users`.`active`"},
			},
		},
		{
			name: "named collections are organized properly",
			sql: `
				CREATE DATABASE analytics ENGINE = Atomic;
				
				CREATE NAMED COLLECTION kafka_config AS
					host = 'localhost',
					port = 9092;
					
				CREATE TABLE analytics.events (
					id UInt64,
					message String
				) ENGINE = MergeTree() ORDER BY id;
			`,
			fileTests: []fileTest{
				{"db/main.sql", "-- housekeeper:import schemas/analytics/schema.sql"},
				{"db/main.sql", "-- housekeeper:import schemas/default/schema.sql"},
				{"db/schemas/analytics/schema.sql", "CREATE DATABASE `analytics` ENGINE = Atomic"},
				{"db/schemas/analytics/schema.sql", "-- housekeeper:import tables/events.sql"},
				{"db/schemas/default/schema.sql", "-- Named Collections"},
				{"db/schemas/default/schema.sql", "-- housekeeper:import collections/kafka_config.sql"},
				{"db/schemas/analytics/tables/events.sql", "CREATE TABLE `analytics`.`events`"},
				{"db/schemas/analytics/tables/events.sql", "`id`      UInt64"},
				{"db/schemas/analytics/tables/events.sql", "`message` String"},
				{"db/schemas/default/collections/kafka_config.sql", "CREATE NAMED COLLECTION `kafka_config`"},
				{"db/schemas/default/collections/kafka_config.sql", "`host` = 'localhost'"},
				{"db/schemas/default/collections/kafka_config.sql", "`port` = 9092"},
			},
		},
		{
			name: "table without explicit database uses default",
			sql: `
				CREATE TABLE events (id UInt64) ENGINE = MergeTree() ORDER BY id;
			`,
			fileTests: []fileTest{
				{"db/main.sql", "-- housekeeper:import schemas/default/schema.sql"},
				{"db/schemas/default/schema.sql", "-- housekeeper:import tables/events.sql"},
				{"db/schemas/default/tables/events.sql", "CREATE TABLE `events`"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse the SQL
			sql, err := parser.ParseString(tt.sql)
			require.NoError(t, err)

			// Create project instance
			proj := New(ProjectParams{
				Dir:       "/tmp/test",
				Formatter: format.New(format.Defaults),
			})

			// Generate the image
			fsImage, err := proj.generateImage(sql)
			require.NoError(t, err)

			// Check expected files and content
			for _, ft := range tt.fileTests {
				file, err := fsImage.Open(ft.path)
				require.NoError(t, err)
				defer file.Close()

				content, err := io.ReadAll(file)
				require.NoError(t, err)

				require.Contains(t, string(content), ft.expectedContent,
					"File %s should contain %q\nActual content:\n%s",
					ft.path, ft.expectedContent, string(content))
			}
		})
	}
}

type fileTest struct {
	path            string
	expectedContent string
}

func TestGenerateImage_FileStructure(t *testing.T) {
	sql := `
		CREATE DATABASE analytics ENGINE = Atomic;
		CREATE NAMED COLLECTION api_config AS host = 'api.example.com', port = 8080;
		CREATE TABLE analytics.events (id UInt64) ENGINE = MergeTree() ORDER BY id;
		CREATE DICTIONARY analytics.lookup (id UInt64) PRIMARY KEY id SOURCE(HTTP(url 'http://example.com')) LAYOUT(HASHED()) LIFETIME(3600);
		CREATE VIEW analytics.summary AS SELECT count() FROM events;
	`

	parsed, err := parser.ParseString(sql)
	require.NoError(t, err)

	// Create project instance
	proj := New(ProjectParams{
		Dir:       "/tmp/test",
		Formatter: format.New(format.Defaults),
	})

	fsImage, err := proj.generateImage(parsed)
	require.NoError(t, err)

	// Collect all files in the image
	var files []string
	err = fs.WalkDir(fsImage, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			files = append(files, path)
		}
		return nil
	})
	require.NoError(t, err)

	// Expected file structure
	expectedFiles := []string{
		"db/main.sql",
		"db/schemas/analytics/schema.sql",
		"db/schemas/default/schema.sql",
		"db/schemas/default/collections/api_config.sql",
		"db/schemas/analytics/tables/events.sql",
		"db/schemas/analytics/dictionaries/lookup.sql",
		"db/schemas/analytics/views/summary.sql",
	}

	require.ElementsMatch(t, expectedFiles, files)
}

func TestFormatStatement_UnsupportedType(t *testing.T) {
	// Test that formatStatement returns an error for unsupported types
	proj := New(ProjectParams{
		Dir:       "/tmp/test",
		Formatter: format.New(format.Defaults),
	})
	_, err := proj.formatStatement("invalid")
	require.Error(t, err)
	require.Contains(t, err.Error(), "unsupported statement type")
}

func TestGetDatabase(t *testing.T) {
	tests := []struct {
		name     string
		database *string
		expected string
	}{
		{
			name:     "nil database returns default",
			database: nil,
			expected: "default",
		},
		{
			name:     "empty database returns default",
			database: stringPtr(""),
			expected: "default",
		},
		{
			name:     "non-empty database returns value",
			database: stringPtr("analytics"),
			expected: "analytics",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := getDatabase(tt.database)
			require.Equal(t, tt.expected, result)
		})
	}
}

// Helper function to create string pointers
func stringPtr(s string) *string {
	return &s
}
