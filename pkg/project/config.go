package project

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultClickHouseVersion is the default ClickHouse version used when none is specified
	DefaultClickHouseVersion = "25.7"

	// DefaultClickHouseConfigDir is the default directory for ClickHouse configuration files
	DefaultClickHouseConfigDir = "db/config.d"

	// DefaultClickHouseCluster is the default cluster name used when none is specified
	DefaultClickHouseCluster = "cluster"
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

	// Config represents the complete schema configuration containing multiple environments.
	//
	// Each configuration file can define multiple database environments with their
	// schema entry points and migration directories. This allows for flexible
	// deployment scenarios across development, staging, and production environments.
	Config struct {
		// ClickHouse contains ClickHouse-specific configuration settings
		ClickHouse ClickHouse `yaml:"clickhouse"`

		// Envs contains the list of configured database environments
		Envs []*Env `yaml:"environments"`
	}

	// Env represents a single database environment configuration.
	//
	// Each environment defines the schema entry point and migration directory
	// for that specific environment.
	Env struct {
		// Name is the unique identifier for this environment (e.g., "local", "staging", "production")
		Name string `yaml:"name"`

		// Entrypoint specifies the main SQL file that serves as the entry point for this environment's schema
		Entrypoint string `yaml:"entrypoint"`

		// Dir specifies the directory where migration files for this environment are stored
		Dir string `yaml:"dir"`
	}
)

// LoadConfig parses a schema configuration from the provided io.Reader.
//
// The function expects YAML-formatted configuration data that defines database
// environments with their schema entry points and migration directories. It uses
// a streaming YAML decoder to handle potentially large configuration files
// efficiently. If no ClickHouse version is specified, it defaults to DefaultClickHouseVersion.
//
// Parameters:
//   - r: An io.Reader containing YAML configuration data
//
// Returns:
//   - *Config: Successfully parsed configuration with all environments
//   - error: Any parsing or validation errors encountered
//
// Example:
//
//	import (
//		"strings"
//		"github.com/pseudomuto/housekeeper/pkg/project"
//	)
//
//	yamlData := `
//	environments:
//	  - name: local
//	    entrypoint: db/main.sql
//	    dir: db/migrations/local
//	`
//
//	config, err := project.LoadConfig(strings.NewReader(yamlData))
//	if err != nil {
//		panic(err)
//	}
//
//	fmt.Printf("Loaded %d environments\n", len(config.Envs))
func LoadConfig(r io.Reader) (*Config, error) {
	var cfg Config
	if err := yaml.NewDecoder(r).Decode(&cfg); err != nil {
		return nil, errors.Wrap(err, "failed to unmarshal schema config")
	}

	// Set default ClickHouse configuration values if not specified
	if cfg.ClickHouse.Version == "" {
		cfg.ClickHouse.Version = DefaultClickHouseVersion
	}
	if cfg.ClickHouse.ConfigDir == "" {
		cfg.ClickHouse.ConfigDir = DefaultClickHouseConfigDir
	}
	if cfg.ClickHouse.Cluster == "" {
		cfg.ClickHouse.Cluster = DefaultClickHouseCluster
	}

	return &cfg, nil
}

// LoadConfigFile loads a project configuration from the specified file path.
// This is a convenience function that opens the file and calls LoadConfig.
//
// Example:
//
//	config, err := project.LoadConfigFile("housekeeper.yaml")
//	if err != nil {
//		log.Fatal("Failed to load config:", err)
//	}
//
//	// Access environment configurations
//	for _, env := range config.Envs {
//		fmt.Printf("Environment: %s (entrypoint: %s)\n", env.Name, env.Entrypoint)
//	}
func LoadConfigFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file: %s", path)
	}
	defer func() { _ = f.Close() }()

	return LoadConfig(f)
}
