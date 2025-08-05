package schema_test

import (
	_ "embed"
	"os"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/schema"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/config.yaml
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

		// Valid YAML with no environments key
		config, err = LoadConfig(strings.NewReader("other_key: value"))
		require.NoError(t, err)
		require.NotNil(t, config)
		require.Empty(t, config.Envs)
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
	require.Len(t, config.Envs, 4)

	// Test environment with all fields
	local := config.Envs[0]
	require.Equal(t, "local", local.Name)
	require.Equal(t, "clickhouse://localhost:9000/dev", local.DevURL)
	require.Equal(t, "clickhouse://localhost:9000/prod", local.URL)
	require.Equal(t, "db/main.sql", local.Entrypoint)

	// Test environment with optional dev URL missing
	staging := config.Envs[1]
	require.Equal(t, "staging", staging.Name)
	require.Empty(t, staging.DevURL)
	require.Equal(t, "clickhouse://staging:9443/staging_db", staging.URL)
	require.Equal(t, "schemas/staging.sql", staging.Entrypoint)

	// Test environment with production settings
	production := config.Envs[2]
	require.Equal(t, "production", production.Name)
	require.Empty(t, production.DevURL)
	require.Equal(t, "clickhouse://prod:9443/prod_db", production.URL)
	require.Equal(t, "schemas/production.sql", production.Entrypoint)

	// Test environment with special characters
	special := config.Envs[3]
	require.Equal(t, "special-chars_123", special.Name)
	require.Contains(t, special.DevURL, "p@ss")
	require.Contains(t, special.URL, "complex!pass")
	require.Contains(t, special.Entrypoint, "special chars & symbols")
}
