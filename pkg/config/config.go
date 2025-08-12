package config

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
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
	}

	// Config represents the project configuration for ClickHouse schema management.
	Config struct {
		// ClickHouse contains ClickHouse-specific configuration settings
		ClickHouse ClickHouse `yaml:"clickhouse"`

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
