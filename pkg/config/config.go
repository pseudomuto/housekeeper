package config

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"gopkg.in/yaml.v3"
)

type (
	// ClickHouse represents ClickHouse-specific configuration settings.
	//
	// This struct contains configuration values that are specific to the ClickHouse
	// database system, including version compatibility and configuration file management.
	ClickHouse struct {
		// Version specifies the target ClickHouse version for schema compatibility
		// This helps ensure generated DDL is compatible with the specified version
		Version string `yaml:"version,omitempty"`

		// ConfigDir specifies the directory where ClickHouse configuration files are stored
		// This directory is used for managing ClickHouse server configuration fragments
		ConfigDir string `yaml:"config_dir,omitempty"`

		// Cluster specifies the default cluster name for distributed ClickHouse deployments
		// This is used for ON CLUSTER operations and distributed DDL statements
		Cluster string `yaml:"cluster,omitempty"`

		// IgnoreDatabases specifies a list of database names to exclude from schema operations
		// These databases will be ignored during dump and diff operations
		IgnoreDatabases []string `yaml:"ignore_databases,omitempty"`
	}

	// FormatterOptionsConfig represents format configuration settings that can be specified in YAML.
	//
	// This struct uses pointer fields to distinguish between explicitly set zero values and
	// unspecified values in the YAML configuration. This allows proper merging with default
	// values where only non-nil user values override the defaults.
	FormatterOptionsConfig struct {
		// IndentSize specifies the number of spaces to use for indentation
		IndentSize *int `yaml:"indent_size,omitempty"`

		// MaxLineLength suggests when to break long lines (0 = no limit)
		MaxLineLength *int `yaml:"max_line_length,omitempty"`

		// UppercaseKeywords controls whether SQL keywords are formatted in uppercase
		UppercaseKeywords *bool `yaml:"uppercase_keywords,omitempty"`

		// AlignColumns controls whether column definitions are aligned in CREATE TABLE statements
		AlignColumns *bool `yaml:"align_columns,omitempty"`

		// MultilineFunctions enables multi-line formatting for complex function expressions
		MultilineFunctions *bool `yaml:"multiline_functions,omitempty"`

		// FunctionArgThreshold is the number of function arguments that triggers multi-line formatting
		FunctionArgThreshold *int `yaml:"function_arg_threshold,omitempty"`

		// MultilineFunctionNames contains function names that should always be formatted multi-line
		MultilineFunctionNames []string `yaml:"multiline_function_names,omitempty"`

		// FunctionIndentSize specifies extra indentation for function arguments (defaults to IndentSize)
		FunctionIndentSize *int `yaml:"function_indent_size,omitempty"`

		// SmartFunctionPairing enables intelligent argument pairing for conditional functions
		SmartFunctionPairing *bool `yaml:"smart_function_pairing,omitempty"`

		// PairedFunctionNames specifies which function names should use paired argument formatting
		PairedFunctionNames []string `yaml:"paired_function_names,omitempty"`

		// PairSize specifies how many arguments constitute a pair for paired formatting
		PairSize *int `yaml:"pair_size,omitempty"`
	}

	// Config represents the project configuration for ClickHouse schema management.
	Config struct {
		// ClickHouse contains ClickHouse-specific configuration settings
		ClickHouse ClickHouse `yaml:"clickhouse"`

		// FormatOptions contains formatter configuration settings
		FormatOptions *FormatterOptionsConfig `yaml:"format_options,omitempty"`

		// Entrypoint specifies the main SQL file that serves as the entry point for the schema
		Entrypoint string `yaml:"entrypoint"`

		// Dir specifies the directory where migration files are stored
		Dir string `yaml:"dir"`
	}
)

// LoadConfig parses a schema configuration from the provided io.Reader.
//
// The function expects YAML-formatted configuration data that defines the project
// schema entry point and migration directory. It uses a streaming YAML decoder
// to handle configuration files efficiently. If no ClickHouse version is specified,
// it defaults to DefaultClickHouseVersion.
//
// Parameters:
//   - r: An io.Reader containing YAML configuration data
//
// Returns:
//   - *Config: Successfully parsed configuration
//   - error: Any parsing or validation errors encountered
//
// Example:
//
//	import (
//		"strings"
//		"github.com/pseudomuto/housekeeper/pkg/config"
//	)
//
//	yamlData := `
//	entrypoint: db/main.sql
//	dir: db/migrations
//	`
//
//	cfg, err := config.LoadConfig(strings.NewReader(yamlData))
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Printf("Schema entrypoint: %s\n", cfg.Entrypoint)
func LoadConfig(r io.Reader) (*Config, error) {
	var cfg Config
	if err := yaml.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal schema config")
	}

	// Set default ClickHouse configuration values if not specified
	if cfg.ClickHouse.Version == "" {
		cfg.ClickHouse.Version = consts.DefaultClickHouseVersion
	}
	if cfg.ClickHouse.ConfigDir == "" {
		cfg.ClickHouse.ConfigDir = consts.DefaultClickHouseConfigDir
	}
	if cfg.ClickHouse.Cluster == "" {
		cfg.ClickHouse.Cluster = consts.DefaultClickHouseCluster
	}

	return &cfg, nil
}

// LoadConfigFile loads a project configuration from the specified file path.
// This is a convenience function that opens the file and calls LoadConfig.
//
// Example:
//
//	cfg, err := config.LoadConfigFile("housekeeper.yaml")
//	if err != nil {
//		log.Fatal("Failed to load config:", err)
//	}
//
//	fmt.Printf("Entrypoint: %s, Migration dir: %s\n", cfg.Entrypoint, cfg.Dir)
func LoadConfigFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file: %s", path)
	}
	defer func() { _ = f.Close() }()

	return LoadConfig(f)
}

// GetFormatterOptions returns the merged formatter options, combining defaults with user configuration.
//
// This method starts with the default formatter options and applies any non-nil values
// from the user configuration. This ensures that unspecified values retain their defaults
// while allowing users to selectively override specific settings, including zero values.
func (c *Config) GetFormatterOptions() format.FormatterOptions {
	if c == nil || c.FormatOptions == nil {
		return format.Defaults
	}

	// Start with defaults and selectively apply user config
	result := format.Defaults

	f := c.FormatOptions
	if f.IndentSize != nil {
		result.IndentSize = *f.IndentSize
	}
	if f.MaxLineLength != nil {
		result.MaxLineLength = *f.MaxLineLength
	}
	if f.UppercaseKeywords != nil {
		result.UppercaseKeywords = *f.UppercaseKeywords
	}
	if f.AlignColumns != nil {
		result.AlignColumns = *f.AlignColumns
	}
	if f.MultilineFunctions != nil {
		result.MultilineFunctions = *f.MultilineFunctions
	}
	if f.FunctionArgThreshold != nil {
		result.FunctionArgThreshold = *f.FunctionArgThreshold
	}
	if f.MultilineFunctionNames != nil {
		result.MultilineFunctionNames = make([]string, len(f.MultilineFunctionNames))
		copy(result.MultilineFunctionNames, f.MultilineFunctionNames)
	}
	if f.FunctionIndentSize != nil {
		result.FunctionIndentSize = *f.FunctionIndentSize
	}
	if f.SmartFunctionPairing != nil {
		result.SmartFunctionPairing = *f.SmartFunctionPairing
	}
	if f.PairedFunctionNames != nil {
		result.PairedFunctionNames = make([]string, len(f.PairedFunctionNames))
		copy(result.PairedFunctionNames, f.PairedFunctionNames)
	}
	if f.PairSize != nil {
		result.PairSize = *f.PairSize
	}

	return result
}

// GetFormatter creates a new formatter instance using the merged formatter options.
//
// This is a convenience method that combines GetFormatterOptions with formatter creation.
// It returns a properly configured formatter that respects both default settings and
// user overrides from the configuration file.
func (c *Config) GetFormatter() *format.Formatter {
	return format.New(c.GetFormatterOptions())
}
