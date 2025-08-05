package schema

import (
	"io"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type (
	// Config represents the complete schema configuration containing multiple environments.
	//
	// Each configuration file can define multiple database environments with their
	// specific connection details and schema entry points. This allows for flexible
	// deployment scenarios across development, staging, and production environments.
	Config struct {
		// Envs contains the list of configured database environments
		Envs []*Env `yaml:"environments"`
	}

	// Env represents a single database environment configuration.
	//
	// Each environment defines connection URLs for both development and production
	// scenarios, along with the schema entry point file that should be used for
	// that specific environment.
	Env struct {
		// Name is the unique identifier for this environment (e.g., "local", "staging", "production")
		Name string `yaml:"name"`

		// Entrypoint specifies the main SQL file that serves as the entry point for this environment's schema
		Entrypoint string `yaml:"entrypoint"`

		// DevURL is the optional development database connection URL
		// This is typically used for local development and testing scenarios
		DevURL string `yaml:"dev"`

		// URL is the primary database connection URL for this environment
		// This is used for production deployments and migrations
		URL string `yaml:"url"`
	}
)

// LoadConfig parses a schema configuration from the provided io.Reader.
//
// The function expects YAML-formatted configuration data that defines database
// environments with their connection details and schema entry points. It uses
// a streaming YAML decoder to handle potentially large configuration files
// efficiently.
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
//		"github.com/pseudomuto/housekeeper/pkg/schema"
//	)
//
//	yamlData := `
//	environments:
//	  - name: local
//	    dev: clickhouse://localhost:9000/dev
//	    url: clickhouse://localhost:9000/prod
//	    entrypoint: db/main.sql
//	`
//
//	config, err := schema.LoadConfig(strings.NewReader(yamlData))
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

	return &cfg, nil
}

// LoadConfigFile loads and parses a schema configuration from a file.
//
// This is a convenience function that handles file opening, reading, and closing
// automatically. It delegates the actual parsing to LoadConfig after opening
// the specified file. The file is automatically closed when the function returns,
// regardless of success or failure.
//
// Parameters:
//   - path: Filesystem path to the YAML configuration file
//
// Returns:
//   - *Config: Successfully parsed configuration with all environments
//   - error: File opening, reading, or parsing errors with context
//
// Example:
//
//	import "github.com/pseudomuto/housekeeper/pkg/schema"
//
//	// Load configuration from file
//	config, err := schema.LoadConfigFile("schema.yaml")
//	if err != nil {
//		log.Fatalf("Failed to load config: %v", err)
//	}
//
//	// Access environment configurations
//	for _, env := range config.Envs {
//		fmt.Printf("Environment: %s (%s)\n", env.Name, env.URL)
//	}
//
// Common usage in CLI applications:
//
//	configPath := flag.String("config", "schema.yaml", "Path to schema config")
//	flag.Parse()
//
//	config, err := schema.LoadConfigFile(*configPath)
//	if err != nil {
//		return fmt.Errorf("loading config: %w", err)
//	}
func LoadConfigFile(path string) (*Config, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to open file: %s", path)
	}
	defer func() { _ = f.Close() }()

	return LoadConfig(f)
}
