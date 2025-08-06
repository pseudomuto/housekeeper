package project

import (
	_ "embed"
	"os"
	"path/filepath"
	"testing/fstest"

	"github.com/pkg/errors"
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

type Project struct {
	root   string
	config *Config
}

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
func (p *Project) Initialize() error {
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

	p.config = cfg
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
