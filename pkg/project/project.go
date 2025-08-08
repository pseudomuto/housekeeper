package project

import (
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed embed/main.sql
	defaultMainSQL []byte

	//go:embed embed/housekeeper.yaml
	defaultHouseKeeper []byte

	image = fstest.MapFS{
		"db":                {Mode: os.ModeDir | 0o755},
		"db/migrations":     {Mode: os.ModeDir | 0o755},
		"db/migrations/dev": {Mode: os.ModeDir | 0o755},
		"db/schemas":        {Mode: os.ModeDir | 0o755},
		"db/main.sql":       {Data: defaultMainSQL},
		"housekeeper.yaml":  {Data: defaultHouseKeeper},
	}
)

type (
	// InitOptions contains options for project initialization
	InitOptions struct {
		// Cluster specifies the ClickHouse cluster name to use in configuration
		// If empty, the default cluster name will be used
		Cluster string
	}

	Project struct {
		root   string
		config *Config
	}
)

// New creates a new Project instance for managing ClickHouse schema projects.
// The path should point to an existing directory that will serve as the project root.
//
// Example:
//
//	// Create a new project in an existing directory
//	project := project.New("/path/to/my/clickhouse/project")
//
//	// Initialize the project structure and configuration
//	if err := project.Initialize(); err != nil {
//		log.Fatal(err)
//	}
//
//	// Parse schema for a specific environment
//	grammar, err := project.ParseSchema("production")
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Parsed %d DDL statements\n", len(grammar.Statements))
func New(path string) *Project {
	return &Project{root: path}
}

// Initialize sets up the project directory structure and loads the configuration.
// This method is idempotent - it will only create missing files and directories,
// preserving any existing content. It creates the standard housekeeper project
// structure including db/, migrations/, and schema directories along with
// configuration files.
//
// Example:
//
//	project := project.New("/path/to/my/project")
//	if err := project.Initialize(); err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Project is now ready for schema parsing
//	grammar, err := project.ParseSchema("development")
//	if err != nil {
//		log.Fatal("Failed to parse schema:", err)
//	}
//
// Initialize sets up the project directory structure and loads the configuration
// with the provided initialization options. This method is idempotent - it will only create
// missing files and directories, preserving any existing content.
//
// The options parameter allows customizing the initialization process, such as specifying
// a custom ClickHouse cluster name. To use defaults, pass an empty InitOptions{}.
//
// Example:
//
//	project := project.New("/path/to/my/project")
//	options := project.InitOptions{Cluster: "production"}
//	if err := project.Initialize(options); err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Or with defaults:
//	if err := project.Initialize(project.InitOptions{}); err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
func (p *Project) Initialize(options InitOptions) error {
	// Ensure the root directory exists and is valid
	if err := p.ensureDirectory(); err != nil {
		return err
	}

	perm := os.FileMode(0o644)

	// Walk the embedded FS and create missing files/directories
	for path, entry := range image {
		fullPath := filepath.Join(p.root, path)

		// Check if the entry already exists
		if _, err := os.Stat(fullPath); err == nil {
			// Entry exists, skip it
			continue
		} else if !os.IsNotExist(err) {
			// Some other error occurred
			return errors.Wrapf(err, "failed to stat %s", fullPath)
		}

		// Entry doesn't exist, create it
		if entry.Mode.IsDir() {
			// Create directory
			if err := os.MkdirAll(fullPath, entry.Mode.Perm()); err != nil {
				return errors.Wrapf(err, "failed to create directory %s", fullPath)
			}

			continue
		}

		// Ensure parent directory exists
		parentDir := filepath.Dir(fullPath)
		if err := os.MkdirAll(parentDir, 0o755); err != nil {
			return errors.Wrapf(err, "failed to create parent directory %s", parentDir)
		}

		// Create file with embedded content
		if err := os.WriteFile(fullPath, entry.Data, perm); err != nil {
			return errors.Wrapf(err, "failed to write file %s", fullPath)
		}
	}

	cfg, err := LoadConfigFile(filepath.Join(p.root, "housekeeper.yaml"))
	if err != nil {
		return errors.Wrap(err, "failed to load housekeeper.yaml")
	}

	// Apply custom options if provided
	if options.Cluster != "" {
		cfg.ClickHouse.Cluster = options.Cluster

		// Write the updated config back to the file
		configPath := filepath.Join(p.root, "housekeeper.yaml")
		configFile, err := os.Create(configPath)
		if err != nil {
			return errors.Wrapf(err, "failed to open config file for writing: %s", configPath)
		}
		defer configFile.Close()

		// Use yaml.NewEncoder to write the updated config
		encoder := yaml.NewEncoder(configFile)
		if err := encoder.Encode(cfg); err != nil {
			return errors.Wrap(err, "failed to write updated config")
		}
		if err := encoder.Close(); err != nil {
			return errors.Wrap(err, "failed to close yaml encoder")
		}
	}

	p.config = cfg

	// Create ClickHouse config directory if it doesn't exist
	configDirPath := filepath.Join(p.root, cfg.ClickHouse.ConfigDir)
	if _, err := os.Stat(configDirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(configDirPath, 0o755); err != nil {
			return errors.Wrapf(err, "failed to create ClickHouse config directory %s", configDirPath)
		}
	}

	return nil
}

func (p *Project) ensureDirectory() error {
	dir, err := os.Stat(p.root)
	if err != nil {
		return errors.Wrapf(err, "failed to stat dir: %s", p.root)
	}

	if !dir.IsDir() {
		return errors.Wrapf(err, "%s is not a directory", p.root)
	}

	return nil
}

func (p *Project) withEnv(env string, fn func(*Env) error) error {
	if p.config == nil {
		return errors.New("project not initialized - call Initialize() first")
	}

	var found *Env
	for _, e := range p.config.Envs {
		if strings.EqualFold(e.Name, env) {
			found = e
		}
	}

	if found == nil {
		return errors.Errorf("Env not found: %s", env)
	}

	pwd, _ := os.Getwd()
	defer func() { _ = os.Chdir(pwd) }()

	if err := os.Chdir(p.root); err != nil {
		return errors.Wrapf(err, "failed to change to project root: %s", p.root)
	}

	return fn(found)
}
