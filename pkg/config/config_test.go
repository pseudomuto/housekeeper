package config_test

import (
	_ "embed"
	"os"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/housekeeper.yaml
var testConfigYAML string

func TestLoadConfig(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		config, err := LoadConfig(strings.NewReader(testConfigYAML))
		require.NoError(t, err)
		validateTestConfig(t, config)
	})

	t.Run("error", func(t *testing.T) {
		// Invalid YAML
		config, err := LoadConfig(strings.NewReader("invalid: yaml: ["))
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "failed to unmarshal schema config")

		// Empty input
		config, err = LoadConfig(strings.NewReader(""))
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "failed to unmarshal schema config")

		// Valid YAML with no project fields
		config, err = LoadConfig(strings.NewReader("other_key: value"))
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Equal(t, consts.DefaultClickHouseVersion, config.ClickHouse.Version)
		require.Equal(t, consts.DefaultClickHouseConfigDir, config.ClickHouse.ConfigDir)
		require.Equal(t, consts.DefaultClickHouseCluster, config.ClickHouse.Cluster)
	})
}

func TestLoadConfigFile(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		// Create temporary file with embedded YAML content
		tempFile, err := os.CreateTemp("", "schema_test_*.yaml")
		require.NoError(t, err)
		defer os.Remove(tempFile.Name())

		_, err = tempFile.WriteString(testConfigYAML)
		require.NoError(t, err)
		require.NoError(t, tempFile.Close())

		// Test loading from file
		config, err := LoadConfigFile(tempFile.Name())
		require.NoError(t, err)
		validateTestConfig(t, config)
	})

	t.Run("error", func(t *testing.T) {
		// Nonexistent file
		config, err := LoadConfigFile("nonexistent.yaml")
		require.Error(t, err)
		require.Nil(t, config)
		require.Contains(t, err.Error(), "failed to open file")

		// Create temporary directory to test directory access
		tempDir, err := os.MkdirTemp("", "schema_test_dir")
		require.NoError(t, err)
		defer os.RemoveAll(tempDir)

		// Directory instead of file
		config, err = LoadConfigFile(tempDir)
		require.Error(t, err)
		require.Nil(t, config)
		// Error message can vary by system, so check for either possibility
		require.True(t, strings.Contains(err.Error(), "failed to open file") ||
			strings.Contains(err.Error(), "failed to unmarshal schema config"))
	})
}

// validateTestConfig validates that a config contains the expected test data
func validateTestConfig(t *testing.T, config *Config) {
	t.Helper()
	require.NotNil(t, config)
	require.Equal(t, "25.7", config.ClickHouse.Version)
	require.Equal(t, "db/config.d", config.ClickHouse.ConfigDir)
	require.Equal(t, "cluster", config.ClickHouse.Cluster)
	require.Equal(t, "db/main.sql", config.Entrypoint)
	require.Equal(t, "db/migrations", config.Dir)
}

func TestLoadConfig_ClickHouseDefaults(t *testing.T) {
	t.Run("keeps configured values when set", func(t *testing.T) {
		yamlData := `
clickhouse:
  version: "24.8"
  config_dir: "custom/config"
  cluster: "production"
  ignore_databases:
    - testing_db
    - temp_db
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Equal(t, "24.8", config.ClickHouse.Version)
		require.Equal(t, "custom/config", config.ClickHouse.ConfigDir)
		require.Equal(t, "production", config.ClickHouse.Cluster)
		require.Equal(t, []string{"testing_db", "temp_db"}, config.ClickHouse.IgnoreDatabases)
	})

	t.Run("sets default values when empty", func(t *testing.T) {
		yamlData := `
clickhouse:
  version: ""
  config_dir: ""
  cluster: ""
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Equal(t, consts.DefaultClickHouseVersion, config.ClickHouse.Version)
		require.Equal(t, "25.7", config.ClickHouse.Version)
		require.Equal(t, consts.DefaultClickHouseConfigDir, config.ClickHouse.ConfigDir)
		require.Equal(t, "db/config.d", config.ClickHouse.ConfigDir)
		require.Equal(t, consts.DefaultClickHouseCluster, config.ClickHouse.Cluster)
		require.Equal(t, "cluster", config.ClickHouse.Cluster)
	})

	t.Run("sets default values when not specified", func(t *testing.T) {
		yamlData := `
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Equal(t, consts.DefaultClickHouseVersion, config.ClickHouse.Version)
		require.Equal(t, "25.7", config.ClickHouse.Version)
		require.Equal(t, consts.DefaultClickHouseConfigDir, config.ClickHouse.ConfigDir)
		require.Equal(t, "db/config.d", config.ClickHouse.ConfigDir)
		require.Equal(t, consts.DefaultClickHouseCluster, config.ClickHouse.Cluster)
		require.Equal(t, "cluster", config.ClickHouse.Cluster)
	})

	t.Run("sets defaults when clickhouse section missing", func(t *testing.T) {
		yamlData := `
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Equal(t, consts.DefaultClickHouseVersion, config.ClickHouse.Version)
		require.Equal(t, consts.DefaultClickHouseConfigDir, config.ClickHouse.ConfigDir)
		require.Equal(t, consts.DefaultClickHouseCluster, config.ClickHouse.Cluster)
	})
}

func TestLoadConfig_IgnoreDatabases(t *testing.T) {
	t.Run("parses ignore_databases list", func(t *testing.T) {
		yamlData := `
clickhouse:
  ignore_databases:
    - testing_db
    - staging_db
    - temp_analytics
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Len(t, config.ClickHouse.IgnoreDatabases, 3)
		require.Equal(t, []string{"testing_db", "staging_db", "temp_analytics"}, config.ClickHouse.IgnoreDatabases)
	})

	t.Run("empty ignore_databases when not specified", func(t *testing.T) {
		yamlData := `
clickhouse:
  version: "25.7"
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.Empty(t, config.ClickHouse.IgnoreDatabases)
	})

	t.Run("empty ignore_databases with empty array", func(t *testing.T) {
		yamlData := `
clickhouse:
  ignore_databases: []
entrypoint: test.sql
dir: migrations
`
		config, err := LoadConfig(strings.NewReader(yamlData))
		require.NoError(t, err)
		require.NotNil(t, config.ClickHouse.IgnoreDatabases)
		require.Empty(t, config.ClickHouse.IgnoreDatabases)
	})
}
