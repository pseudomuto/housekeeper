package config_test

import (
	_ "embed"
	"os"
	"strings"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
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

func TestConfigGetFormatterOptions(t *testing.T) {
	tests := []struct {
		name        string
		configYAML  string
		expected    format.FormatterOptions
		description string
	}{
		{
			name:        "nil config returns defaults",
			configYAML:  "",
			expected:    format.Defaults,
			description: "When config is nil, should return default formatter options",
		},
		{
			name: "config with no format_options returns defaults",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
clickhouse:
  version: "25.7"
`,
			expected:    format.Defaults,
			description: "When config has no format_options section, should return defaults",
		},
		{
			name: "config with partial format_options merges with defaults",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  indent_size: 4
  uppercase_keywords: true
`,
			expected: format.FormatterOptions{
				IndentSize:             4,
				MaxLineLength:          format.Defaults.MaxLineLength,
				UppercaseKeywords:      true,
				AlignColumns:           format.Defaults.AlignColumns,
				MultilineFunctions:     format.Defaults.MultilineFunctions,
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames,
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,
				SmartFunctionPairing:   format.Defaults.SmartFunctionPairing,
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,
				PairSize:               format.Defaults.PairSize,
			},
			description: "Should merge user values with defaults, preserving unspecified options",
		},
		{
			name: "config with all format_options overrides defaults",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  indent_size: 8
  max_line_length: 80
  uppercase_keywords: true
  align_columns: false
  multiline_functions: false
  function_arg_threshold: 6
  multiline_function_names:
    - "customMultiIf"
  function_indent_size: 2
  smart_function_pairing: false
  paired_function_names:
    - "customIf"
    - "customCase"
  pair_size: 3
`,
			expected: format.FormatterOptions{
				IndentSize:             8,
				MaxLineLength:          80,
				UppercaseKeywords:      true,
				AlignColumns:           false,
				MultilineFunctions:     false,
				FunctionArgThreshold:   6,
				MultilineFunctionNames: []string{"customMultiIf"},
				FunctionIndentSize:     2,
				SmartFunctionPairing:   false,
				PairedFunctionNames:    []string{"customIf", "customCase"},
				PairSize:               3,
			},
			description: "Should use all user-specified values, overriding all defaults",
		},
		{
			name: "config with zero values should use zero values not defaults",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  indent_size: 0
  max_line_length: 0
  uppercase_keywords: false
  align_columns: false
  multiline_functions: false
  function_arg_threshold: 0
  function_indent_size: 0
  smart_function_pairing: false
  pair_size: 0
`,
			expected: format.FormatterOptions{
				IndentSize:             0,
				MaxLineLength:          0,
				UppercaseKeywords:      false,
				AlignColumns:           false,
				MultilineFunctions:     false,
				FunctionArgThreshold:   0,
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames, // not specified, should use default
				FunctionIndentSize:     0,
				SmartFunctionPairing:   false,
				PairedFunctionNames:    format.Defaults.PairedFunctionNames, // not specified, should use default
				PairSize:               0,
			},
			description: "Should respect explicitly set zero values, not replace them with defaults",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg *Config
			var err error

			if tt.configYAML != "" {
				cfg, err = LoadConfig(strings.NewReader(tt.configYAML))
				require.NoError(t, err)
			}

			result := cfg.GetFormatterOptions()
			require.Equal(t, tt.expected, result, tt.description)
		})
	}
}

func TestConfigGetFormatter(t *testing.T) {
	configYAML := `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  indent_size: 4
  uppercase_keywords: true
`

	cfg, err := LoadConfig(strings.NewReader(configYAML))
	require.NoError(t, err)

	formatter := cfg.GetFormatter()
	require.NotNil(t, formatter)

	// Verify the formatter was created with the merged options
	// We can't directly access formatter internals, but we can verify it was created
	require.IsType(t, &format.Formatter{}, formatter)
}

func TestConfigMergeLogic(t *testing.T) {
	tests := []struct {
		name           string
		formatOptions  *FormatterOptionsConfig
		expectedMerged format.FormatterOptions
		description    string
	}{
		{
			name:           "empty format options config merges with defaults",
			formatOptions:  &FormatterOptionsConfig{},
			expectedMerged: format.Defaults,
			description:    "Empty config should result in defaults after merge",
		},
		{
			name: "partial config merges correctly",
			formatOptions: &FormatterOptionsConfig{
				IndentSize:        intPtr(4),
				UppercaseKeywords: boolPtr(true),
				// Other fields are nil, should use defaults
			},
			expectedMerged: format.FormatterOptions{
				IndentSize:             4,
				MaxLineLength:          format.Defaults.MaxLineLength,
				UppercaseKeywords:      true,
				AlignColumns:           format.Defaults.AlignColumns,
				MultilineFunctions:     format.Defaults.MultilineFunctions,
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames,
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,
				SmartFunctionPairing:   format.Defaults.SmartFunctionPairing,
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,
				PairSize:               format.Defaults.PairSize,
			},
			description: "Partial config should override some values while preserving defaults for others",
		},
		{
			name: "zero values override defaults",
			formatOptions: &FormatterOptionsConfig{
				IndentSize:           intPtr(0),
				UppercaseKeywords:    boolPtr(false),
				AlignColumns:         boolPtr(false),
				SmartFunctionPairing: boolPtr(false),
				PairSize:             intPtr(0),
				// Other fields not set, should use defaults
			},
			expectedMerged: format.FormatterOptions{
				IndentSize:             0,
				MaxLineLength:          format.Defaults.MaxLineLength,
				UppercaseKeywords:      false,
				AlignColumns:           false,
				MultilineFunctions:     format.Defaults.MultilineFunctions,
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames,
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,
				SmartFunctionPairing:   false,
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,
				PairSize:               0,
			},
			description: "Zero values should override defaults when explicitly set",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a config with the test format options
			cfg := &Config{
				FormatOptions: tt.formatOptions,
			}

			result := cfg.GetFormatterOptions()
			require.Equal(t, tt.expectedMerged, result, tt.description)
		})
	}
}

func TestNilConfigGetFormatterOptions(t *testing.T) {
	var cfg *Config
	result := cfg.GetFormatterOptions()
	require.Equal(t, format.Defaults, result, "Nil config should return defaults")
}

func TestNilConfigGetFormatter(t *testing.T) {
	var cfg *Config
	formatter := cfg.GetFormatter()
	require.NotNil(t, formatter)
	require.IsType(t, &format.Formatter{}, formatter)
}

func TestBooleanFieldOverrideEdgeCases(t *testing.T) {
	// Test specific boolean override scenarios to ensure pointer logic works correctly
	tests := []struct {
		name        string
		configYAML  string
		expected    format.FormatterOptions
		description string
	}{
		{
			name: "override default true with explicit false",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  uppercase_keywords: false        # Default is true, override with false
  align_columns: false             # Default is true, override with false
  multiline_functions: false       # Default is true, override with false
  smart_function_pairing: false    # Default is true, override with false
`,
			expected: format.FormatterOptions{
				IndentSize:             format.Defaults.IndentSize,             // not specified, use default
				MaxLineLength:          format.Defaults.MaxLineLength,          // not specified, use default
				UppercaseKeywords:      false,                                  // explicitly set to false
				AlignColumns:           false,                                  // explicitly set to false
				MultilineFunctions:     false,                                  // explicitly set to false
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,   // not specified, use default
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames, // not specified, use default
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,     // not specified, use default
				SmartFunctionPairing:   false,                                  // explicitly set to false
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,    // not specified, use default
				PairSize:               format.Defaults.PairSize,               // not specified, use default
			},
			description: "Should override default true values with explicitly set false values",
		},
		{
			name: "set default true values explicitly to true",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  uppercase_keywords: true         # Default is true, explicitly set to true
  align_columns: true              # Default is true, explicitly set to true
  multiline_functions: true        # Default is true, explicitly set to true
  smart_function_pairing: true     # Default is true, explicitly set to true
`,
			expected: format.FormatterOptions{
				IndentSize:             format.Defaults.IndentSize,             // not specified, use default
				MaxLineLength:          format.Defaults.MaxLineLength,          // not specified, use default
				UppercaseKeywords:      true,                                   // explicitly set to true (same as default)
				AlignColumns:           true,                                   // explicitly set to true (same as default)
				MultilineFunctions:     true,                                   // explicitly set to true (same as default)
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,   // not specified, use default
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames, // not specified, use default
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,     // not specified, use default
				SmartFunctionPairing:   true,                                   // explicitly set to true (same as default)
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,    // not specified, use default
				PairSize:               format.Defaults.PairSize,               // not specified, use default
			},
			description: "Should respect explicitly set true values even when they match defaults",
		},
		{
			name: "mixed boolean overrides",
			configYAML: `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  uppercase_keywords: false        # Default is true, override with false
  align_columns: true              # Default is true, explicitly set to true
  multiline_functions: false       # Default is true, override with false
  smart_function_pairing: true     # Default is true, explicitly set to true
`,
			expected: format.FormatterOptions{
				IndentSize:             format.Defaults.IndentSize,             // not specified, use default
				MaxLineLength:          format.Defaults.MaxLineLength,          // not specified, use default
				UppercaseKeywords:      false,                                  // explicitly set to false
				AlignColumns:           true,                                   // explicitly set to true
				MultilineFunctions:     false,                                  // explicitly set to false
				FunctionArgThreshold:   format.Defaults.FunctionArgThreshold,   // not specified, use default
				MultilineFunctionNames: format.Defaults.MultilineFunctionNames, // not specified, use default
				FunctionIndentSize:     format.Defaults.FunctionIndentSize,     // not specified, use default
				SmartFunctionPairing:   true,                                   // explicitly set to true
				PairedFunctionNames:    format.Defaults.PairedFunctionNames,    // not specified, use default
				PairSize:               format.Defaults.PairSize,               // not specified, use default
			},
			description: "Should handle mixed boolean overrides correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg, err := LoadConfig(strings.NewReader(tt.configYAML))
			require.NoError(t, err)

			result := cfg.GetFormatterOptions()
			require.Equal(t, tt.expected, result, tt.description)
		})
	}
}

// Helper functions for creating pointers to primitive types
func intPtr(i int) *int {
	return &i
}

func boolPtr(b bool) *bool {
	return &b
}

func TestYAMLUnmarshalingPointerFields(t *testing.T) {
	// Test that YAML unmarshaling correctly creates non-nil pointers for explicitly set boolean values
	configYAML := `
entrypoint: db/main.sql
dir: db/migrations
format_options:
  uppercase_keywords: false
  align_columns: true
`

	cfg, err := LoadConfig(strings.NewReader(configYAML))
	require.NoError(t, err)
	require.NotNil(t, cfg.FormatOptions)

	// These fields were explicitly set in YAML, so pointers should be non-nil
	require.NotNil(t, cfg.FormatOptions.UppercaseKeywords, "UppercaseKeywords should have non-nil pointer when explicitly set")
	require.NotNil(t, cfg.FormatOptions.AlignColumns, "AlignColumns should have non-nil pointer when explicitly set")
	require.False(t, *cfg.FormatOptions.UppercaseKeywords, "UppercaseKeywords should be false")
	require.True(t, *cfg.FormatOptions.AlignColumns, "AlignColumns should be true")

	// These fields were not set in YAML, so pointers should be nil
	require.Nil(t, cfg.FormatOptions.MultilineFunctions, "MultilineFunctions should have nil pointer when not set")
	require.Nil(t, cfg.FormatOptions.SmartFunctionPairing, "SmartFunctionPairing should have nil pointer when not set")
	require.Nil(t, cfg.FormatOptions.IndentSize, "IndentSize should have nil pointer when not set")
}
