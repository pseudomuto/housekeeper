package project

import (
	_ "embed"
	"os"
	"path/filepath"
	"strings"
	"testing/fstest"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/config"
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

		Config *config.Config
	}

	// Project represents a ClickHouse schema management project with its configuration.
	// The project always operates in the current working directory.
	Project struct {
		Config *config.Config
	}
)

// New creates a new Project instance for managing ClickHouse schema projects.
// Uses dependency injection pattern with NewProjectParams struct containing
// the project directory and loaded configuration.
//
// Example:
//
//	// Create a project with loaded configuration
//	cfg, err := config.LoadConfigFile("housekeeper.yaml")
//	if err != nil {
//		log.Fatal(err)
//	}
//	proj := project.New(project.NewProjectParams{
//		Config: cfg,
//	})
//
//	// Compile and parse project schema
//	var buf bytes.Buffer
//	if err := schema.Compile(cfg.Entrypoint, &buf); err != nil {
//		log.Fatal(err)
//	}
//
//	sql, err := parser.ParseString(buf.String())
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	fmt.Printf("Parsed %d DDL statements\n", len(sql.Statements))
func New(p NewProjectParams) *Project {
	return &Project{
		Config: p.Config,
	}
}

// Initialize sets up a new project directory structure.
// This function is idempotent - it will only create missing files and directories,
// preserving any existing content. It creates the standard housekeeper project
// structure including db/, migrations/, and schema directories along with
// configuration files. The function temporarily changes to the target directory
// to perform initialization, then restores the original working directory.
//
// Example:
//
//	// Initialize a new project with default options
//	err := project.Initialize("/path/to/my/project", project.InitOptions{})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Change to project directory and create project instance
//	os.Chdir("/path/to/my/project")
//	cfg, err := config.LoadConfigFile("housekeeper.yaml")
//	if err != nil {
//		log.Fatal("Failed to load config:", err)
//	}
//	proj := project.New(project.NewProjectParams{Config: cfg})
//
//	// Initialize with custom cluster
//	err = project.Initialize("/path/to/my/project", project.InitOptions{
//		Cluster: "production",
//	})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
func Initialize(path string, options InitOptions) error {
	// Save current directory and change to target path
	origDir, err := os.Getwd()
	if err != nil {
		return errors.Wrap(err, "failed to get current directory")
	}

	// Change to the target directory
	if err := os.Chdir(path); err != nil {
		return errors.Wrapf(err, "failed to change to directory %s", path)
	}
	defer func() { _ = os.Chdir(origDir) }()

	// Determine the cluster name to use
	clusterName := options.Cluster
	if clusterName == "" {
		clusterName = "cluster" // default cluster name
	}

	// Walk the embedded FS and create missing files/directories
	for path, entry := range image {
		fullPath := path

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
		if err := os.WriteFile(fullPath, fileContent, consts.ModeFile); err != nil {
			return errors.Wrapf(err, "failed to write file %s", fullPath)
		}
	}

	// Load the configuration to create ClickHouse config directory
	cfg, err := config.LoadConfigFile("housekeeper.yaml")
	if err != nil {
		return errors.Wrap(err, "failed to load housekeeper.yaml")
	}

	// Apply custom cluster option if provided
	if options.Cluster != "" {
		cfg.ClickHouse.Cluster = options.Cluster

		// Write the updated config back to the file
		configPath := "housekeeper.yaml"
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

	// Create ClickHouse config directory if it doesn't exist
	if _, err := os.Stat(cfg.ClickHouse.ConfigDir); os.IsNotExist(err) {
		if err := os.MkdirAll(cfg.ClickHouse.ConfigDir, consts.ModeDir); err != nil {
			return errors.Wrapf(err, "failed to create ClickHouse config directory %s", cfg.ClickHouse.ConfigDir)
		}
	}

	return nil
}

func (p *Project) MigrationsDir() string {
	return filepath.Join("db", "migrations")
}
