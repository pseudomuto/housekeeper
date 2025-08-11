package project

import (
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"go.uber.org/fx"
	"gopkg.in/yaml.v3"
)

var (
	//go:embed embed/main.sql
	defaultMainSQL []byte

	//go:embed embed/housekeeper.yaml
	defaultHouseKeeper []byte

	//go:embed embed/_clickhouse.xml
	defaultClickHouseXML []byte

	//go:embed embed/housekeeper.sql
	defaultHousekeeperSQL []byte

	image = fstest.MapFS{
		"db":                                     {Mode: os.ModeDir | consts.ModeDir},
		"db/config.d":                            {Mode: os.ModeDir | consts.ModeDir},
		"db/config.d/_clickhouse.xml":            {Data: defaultClickHouseXML},
		"db/main.sql":                            {Data: defaultMainSQL},
		"db/migrations":                          {Mode: os.ModeDir | consts.ModeDir},
		"db/schemas":                             {Mode: os.ModeDir | consts.ModeDir},
		"db/schemas/housekeeper":                 {Mode: os.ModeDir | consts.ModeDir},
		"db/schemas/housekeeper/housekeeper.sql": {Data: defaultHousekeeperSQL},
		"housekeeper.yaml":                       {Data: defaultHouseKeeper},
	}
)

type (
	// InitOptions contains options for project initialization
	InitOptions struct {
		// Cluster specifies the ClickHouse cluster name to use in configuration
		// If empty, the default cluster name will be used
		Cluster string
	}

	NewProjectParams struct {
		fx.In

		Dir    string `name:"dir"`
		Config *Config
	}

	Project struct {
		root   string
		config *Config
	}
)

// New creates a new Project instance for managing ClickHouse schema projects.
// Uses dependency injection pattern with NewProjectParams struct containing
// the project directory and loaded configuration.
//
// Example:
//
//	// Create a project with loaded configuration
//	config, err := project.LoadConfigFile("/path/to/housekeeper.yaml")
//	if err != nil {
//		log.Fatal(err)
//	}
//	proj := project.New(project.NewProjectParams{
//		Dir:    "/path/to/my/clickhouse/project",
//		Config: config,
//	})
//
//	// Parse project schema (no initialization needed)
//	grammar, err := proj.ParseSchema()
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Parsed %d DDL statements\n", len(grammar.Statements))
func New(p NewProjectParams) *Project {
	return &Project{
		root:   p.Dir,
		config: p.Config,
	}
}

// Initialize sets up a new project directory structure and returns a configured Project instance.
// This function is idempotent - it will only create missing files and directories,
// preserving any existing content. It creates the standard housekeeper project
// structure including db/, migrations/, and schema directories along with
// configuration files.
//
// Example:
//
//	// Initialize a new project with default options
//	proj, err := project.Initialize("/path/to/my/project", project.InitOptions{})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Project is ready for use
//	grammar, err := proj.ParseSchema()
//	if err != nil {
//		log.Fatal("Failed to parse schema:", err)
//	}
//
//	// Initialize with custom cluster
//	proj, err = project.Initialize("/path/to/my/project", project.InitOptions{
//		Cluster: "production",
//	})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
func Initialize(path string, options InitOptions) (*Project, error) {
	// Create a temporary project to initialize the directory structure
	tempProject := &Project{root: path}
	if err := tempProject.initialize(options); err != nil {
		return nil, err
	}

	// Load the configuration and return a properly configured project
	cfg, err := LoadConfigFile(filepath.Join(path, "housekeeper.yaml"))
	if err != nil {
		return nil, errors.Wrap(err, "failed to load housekeeper.yaml")
	}

	// Create ClickHouse config directory if it doesn't exist
	configDirPath := filepath.Join(path, cfg.ClickHouse.ConfigDir)
	if _, err := os.Stat(configDirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(configDirPath, consts.ModeDir); err != nil {
			return nil, errors.Wrapf(err, "failed to create ClickHouse config directory %s", configDirPath)
		}
	}

	return New(NewProjectParams{
		Dir:    path,
		Config: cfg,
	}), nil
}

// initialize sets up the project directory structure and configuration files.
// This is an internal method used by the top-level Initialize function.
func (p *Project) initialize(options InitOptions) error {
	// Ensure the root directory exists and is valid
	if err := p.ensureDirectory(); err != nil {
		return err
	}

	perm := consts.ModeFile

	// Determine the cluster name to use
	clusterName := options.Cluster
	if clusterName == "" {
		clusterName = "cluster" // default cluster name
	}

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
		if err := os.MkdirAll(parentDir, consts.ModeDir); err != nil {
			return errors.Wrapf(err, "failed to create parent directory %s", parentDir)
		}

		// Special handling for _clickhouse.xml to replace cluster name
		fileContent := entry.Data
		if path == "db/config.d/_clickhouse.xml" {
			// Replace $$CLUSTER placeholder with the actual cluster name
			xmlContent := string(defaultClickHouseXML)
			xmlContent = strings.ReplaceAll(xmlContent, "$$CLUSTER", clusterName)
			fileContent = []byte(xmlContent)
		}

		// Create file with content
		if err := os.WriteFile(fullPath, fileContent, perm); err != nil {
			return errors.Wrapf(err, "failed to write file %s", fullPath)
		}
	}

	if options.Cluster == "" {
		return nil
	}

	// Apply custom options if provided
	cfg, err := LoadConfigFile(filepath.Join(p.root, "housekeeper.yaml"))
	if err != nil {
		return errors.Wrap(err, "failed to load housekeeper.yaml")
	}

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

	// Create ClickHouse config directory if it doesn't exist
	configDirPath := filepath.Join(p.root, cfg.ClickHouse.ConfigDir)
	if _, err := os.Stat(configDirPath); os.IsNotExist(err) {
		if err := os.MkdirAll(configDirPath, consts.ModeDir); err != nil {
			return errors.Wrapf(err, "failed to create ClickHouse config directory %s", configDirPath)
		}
	}

	return nil
}

func (p *Project) MigrationsDir() string {
	return filepath.Join(p.root, "db", "migrations")
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

// withConfig executes the provided function with access to the project's configuration.
// This method ensures the working directory is set to the project root during execution
// and automatically restores the original working directory when complete.
//
// The function is passed a pointer to the Config struct, allowing read and write access
// to configuration values. This is the preferred way to access project configuration
// in a consistent, safe manner.
//
// Example:
//
//	var version string
//	err := project.WithConfig(func(cfg *Config) error {
//		version = cfg.ClickHouse.Version
//		return nil
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//	fmt.Printf("ClickHouse version: %s\n", version)
func (p *Project) withConfig(fn func(*Config) error) error {
	if p.config == nil {
		return errors.New("project configuration not loaded")
	}

	pwd, _ := os.Getwd()
	defer func() { _ = os.Chdir(pwd) }()

	if err := os.Chdir(p.root); err != nil {
		return errors.Wrapf(err, "failed to change to project root: %s", p.root)
	}

	return fn(p.config)
}
