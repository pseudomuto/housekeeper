package utils_test

import (
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/utils"
	"github.com/stretchr/testify/require"
)

func TestSQLBuilder_CREATE(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name:     "CREATE DATABASE",
			builder:  func() *utils.SQLBuilder { return utils.NewSQLBuilder().Create("DATABASE").Name("test") },
			expected: "CREATE DATABASE `test`;",
		},
		{
			name: "CREATE TABLE",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Create("TABLE").QualifiedName(stringPtr("db"), "table")
			},
			expected: "CREATE TABLE `db`.`table`;",
		},
		{
			name: "CREATE DATABASE with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Create("DATABASE").Name("test").OnCluster("prod")
			},
			expected: "CREATE DATABASE `test` ON CLUSTER `prod`;",
		},
		{
			name: "CREATE DATABASE with engine and comment",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Create("DATABASE").Name("analytics").Engine("Atomic").Comment("Analytics database")
			},
			expected: "CREATE DATABASE `analytics` ENGINE = Atomic COMMENT 'Analytics database';",
		},
		{
			name:     "CREATE DATABASE IF NOT EXISTS",
			builder:  func() *utils.SQLBuilder { return utils.NewSQLBuilder().Create("DATABASE").IfNotExists().Name("test") },
			expected: "CREATE DATABASE IF NOT EXISTS `test`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_CREATE_OR_REPLACE(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name: "CREATE OR REPLACE DICTIONARY",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().CreateOrReplace("DICTIONARY").Name("test_dict")
			},
			expected: "CREATE OR REPLACE DICTIONARY `test_dict`;",
		},
		{
			name: "CREATE OR REPLACE VIEW with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().CreateOrReplace("VIEW").QualifiedName(stringPtr("db"), "view").OnCluster("cluster")
			},
			expected: "CREATE OR REPLACE VIEW `db`.`view` ON CLUSTER `cluster`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_DROP(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name:     "DROP DATABASE",
			builder:  func() *utils.SQLBuilder { return utils.NewSQLBuilder().Drop("DATABASE").Name("test") },
			expected: "DROP DATABASE `test`;",
		},
		{
			name:     "DROP DATABASE IF EXISTS",
			builder:  func() *utils.SQLBuilder { return utils.NewSQLBuilder().Drop("DATABASE").IfExists().Name("test") },
			expected: "DROP DATABASE IF EXISTS `test`;",
		},
		{
			name: "DROP TABLE with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Drop("TABLE").IfExists().QualifiedName(stringPtr("db"), "table").OnCluster("prod")
			},
			expected: "DROP TABLE IF EXISTS `db`.`table` ON CLUSTER `prod`;",
		},
		{
			name: "DROP FUNCTION with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Drop("FUNCTION").IfExists().Name("test_func").OnCluster("cluster")
			},
			expected: "DROP FUNCTION IF EXISTS `test_func` ON CLUSTER `cluster`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_ALTER(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name: "ALTER DATABASE MODIFY COMMENT",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Alter("DATABASE").Name("test").Modify("COMMENT").Escaped("New comment")
			},
			expected: "ALTER DATABASE `test` MODIFY COMMENT 'New comment';",
		},
		{
			name: "ALTER DATABASE with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Alter("DATABASE").Name("analytics").OnCluster("prod").Modify("COMMENT").Escaped("Updated")
			},
			expected: "ALTER DATABASE `analytics` ON CLUSTER `prod` MODIFY COMMENT 'Updated';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_RENAME(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name:     "RENAME DATABASE",
			builder:  func() *utils.SQLBuilder { return utils.NewSQLBuilder().Rename("DATABASE").Name("old_db").To("new_db") },
			expected: "RENAME DATABASE `old_db` TO `new_db`;",
		},
		{
			name: "RENAME TABLE with qualified names",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Rename("TABLE").QualifiedName(stringPtr("db"), "old_table").QualifiedTo(stringPtr("db"), "new_table")
			},
			expected: "RENAME TABLE `db`.`old_table` TO `db`.`new_table`;",
		},
		{
			name: "RENAME DICTIONARY with cluster",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().Rename("DICTIONARY").QualifiedName(stringPtr("db"), "old_dict").QualifiedTo(stringPtr("db"), "new_dict").OnCluster("cluster")
			},
			expected: "RENAME DICTIONARY `db`.`old_dict` TO `db`.`new_dict` ON CLUSTER `cluster`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_Comments(t *testing.T) {
	tests := []struct {
		name     string
		comment  string
		expected string
	}{
		{
			name:     "simple comment",
			comment:  "Test comment",
			expected: "CREATE DATABASE `test` COMMENT 'Test comment';",
		},
		{
			name:     "comment with apostrophe",
			comment:  "User's database",
			expected: "CREATE DATABASE `test` COMMENT 'User\\'s database';",
		},
		{
			name:     "empty comment",
			comment:  "",
			expected: "CREATE DATABASE `test`;",
		},
		{
			name:     "comment with multiple apostrophes",
			comment:  "It's a 'test' database",
			expected: "CREATE DATABASE `test` COMMENT 'It\\'s a \\'test\\' database';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Create("DATABASE").Name("test").Comment(tt.comment).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_OnCluster(t *testing.T) {
	tests := []struct {
		name     string
		cluster  string
		expected string
	}{
		{
			name:     "with cluster",
			cluster:  "production",
			expected: "CREATE DATABASE `test` ON CLUSTER `production`;",
		},
		{
			name:     "empty cluster",
			cluster:  "",
			expected: "CREATE DATABASE `test`;",
		},
		{
			name:     "cluster with special characters",
			cluster:  "prod-cluster",
			expected: "CREATE DATABASE `test` ON CLUSTER `prod-cluster`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Create("DATABASE").Name("test").OnCluster(tt.cluster).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_Engine(t *testing.T) {
	tests := []struct {
		name     string
		engine   string
		expected string
	}{
		{
			name:     "simple engine",
			engine:   "Atomic",
			expected: "CREATE DATABASE `test` ENGINE = Atomic;",
		},
		{
			name:     "engine with parameters",
			engine:   "MySQL('host:3306', 'db', 'user', 'pass')",
			expected: "CREATE DATABASE `test` ENGINE = MySQL('host:3306', 'db', 'user', 'pass');",
		},
		{
			name:     "empty engine",
			engine:   "",
			expected: "CREATE DATABASE `test`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Create("DATABASE").Name("test").Engine(tt.engine).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_QualifiedName(t *testing.T) {
	tests := []struct {
		name     string
		database *string
		table    string
		expected string
	}{
		{
			name:     "with database",
			database: stringPtr("analytics"),
			table:    "events",
			expected: "CREATE TABLE `analytics`.`events`;",
		},
		{
			name:     "without database",
			database: nil,
			table:    "events",
			expected: "CREATE TABLE `events`;",
		},
		{
			name:     "empty database",
			database: stringPtr(""),
			table:    "events",
			expected: "CREATE TABLE `events`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Create("TABLE").QualifiedName(tt.database, tt.table).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_AS(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		expected   string
	}{
		{
			name:       "simple function expression",
			expression: "(x) -> x * 2",
			expected:   "CREATE FUNCTION `test` AS (x) -> x * 2;",
		},
		{
			name:       "complex function expression",
			expression: "(a, b) -> if(equals(b, 0), 0, divide(a, b))",
			expected:   "CREATE FUNCTION `test` AS (a, b) -> if(equals(b, 0), 0, divide(a, b));",
		},
		{
			name:       "empty expression",
			expression: "",
			expected:   "CREATE FUNCTION `test`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Create("FUNCTION").Name("test").As(tt.expression).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_Raw(t *testing.T) {
	tests := []struct {
		name     string
		raw      string
		expected string
	}{
		{
			name:     "SYNC clause",
			raw:      "SYNC",
			expected: "DROP DATABASE `test` SYNC;",
		},
		{
			name:     "PERMANENTLY clause",
			raw:      "PERMANENTLY",
			expected: "DROP DATABASE `test` PERMANENTLY;",
		},
		{
			name:     "empty raw",
			raw:      "",
			expected: "DROP DATABASE `test`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Drop("DATABASE").Name("test").Raw(tt.raw).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_StringWithoutSemicolon(t *testing.T) {
	builder := utils.NewSQLBuilder().Create("DATABASE").Name("test").OnCluster("prod")

	withSemicolon := builder.String()
	withoutSemicolon := builder.StringWithoutSemicolon()

	require.Equal(t, "CREATE DATABASE `test` ON CLUSTER `prod`;", withSemicolon)
	require.Equal(t, "CREATE DATABASE `test` ON CLUSTER `prod`", withoutSemicolon)
}

func TestSQLBuilder_Escaped(t *testing.T) {
	tests := []struct {
		name     string
		value    string
		expected string
	}{
		{
			name:     "simple value",
			value:    "New comment",
			expected: "ALTER DATABASE `test` MODIFY COMMENT 'New comment';",
		},
		{
			name:     "value with apostrophe",
			value:    "User's database",
			expected: "ALTER DATABASE `test` MODIFY COMMENT 'User\\'s database';",
		},
		{
			name:     "empty value",
			value:    "",
			expected: "ALTER DATABASE `test` MODIFY COMMENT;",
		},
		{
			name:     "value with multiple apostrophes",
			value:    "It's a 'test' database",
			expected: "ALTER DATABASE `test` MODIFY COMMENT 'It\\'s a \\'test\\' database';",
		},
		{
			name:     "value with quotes and backslashes",
			value:    "Path: C:\\Users\\John's folder",
			expected: "ALTER DATABASE `test` MODIFY COMMENT 'Path: C:\\Users\\John\\'s folder';",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := utils.NewSQLBuilder().Alter("DATABASE").Name("test").Modify("COMMENT").Escaped(tt.value).String()
			require.Equal(t, tt.expected, result)
		})
	}
}

func TestSQLBuilder_ComplexExamples(t *testing.T) {
	tests := []struct {
		name     string
		builder  func() *utils.SQLBuilder
		expected string
	}{
		{
			name: "full CREATE DATABASE",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().
					Create("DATABASE").
					IfNotExists().
					Name("analytics").
					OnCluster("production").
					Engine("Atomic").
					Comment("Analytics database for reporting")
			},
			expected: "CREATE DATABASE IF NOT EXISTS `analytics` ON CLUSTER `production` ENGINE = Atomic COMMENT 'Analytics database for reporting';",
		},
		{
			name: "full DROP with all options",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().
					Drop("DATABASE").
					IfExists().
					Name("old_db").
					OnCluster("prod").
					Raw("SYNC")
			},
			expected: "DROP DATABASE IF EXISTS `old_db` ON CLUSTER `prod` SYNC;",
		},
		{
			name: "ALTER DATABASE with comment change",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().
					Alter("DATABASE").
					Name("analytics").
					OnCluster("production").
					Modify("COMMENT").
					Escaped("Updated analytics database")
			},
			expected: "ALTER DATABASE `analytics` ON CLUSTER `production` MODIFY COMMENT 'Updated analytics database';",
		},
		{
			name: "RENAME qualified objects",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().
					Rename("TABLE").
					QualifiedName(stringPtr("old_db"), "old_table").
					QualifiedTo(stringPtr("new_db"), "new_table").
					OnCluster("cluster")
			},
			expected: "RENAME TABLE `old_db`.`old_table` TO `new_db`.`new_table` ON CLUSTER `cluster`;",
		},
		{
			name: "CREATE FUNCTION",
			builder: func() *utils.SQLBuilder {
				return utils.NewSQLBuilder().
					Create("FUNCTION").
					Name("safe_divide").
					OnCluster("prod").
					As("(a, b) -> if(equals(b, 0), 0, divide(a, b))")
			},
			expected: "CREATE FUNCTION `safe_divide` ON CLUSTER `prod` AS (a, b) -> if(equals(b, 0), 0, divide(a, b));",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := tt.builder().String()
			require.Equal(t, tt.expected, result)
		})
	}
}
