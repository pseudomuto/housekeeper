package project

import (
	"bytes"
	_ "embed"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"testing/fstest"
	"text/template"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"go.uber.org/fx"
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

	// templateData contains all the data available to templates during initialization
	templateData struct {
		Cluster string
	}

	// Project represents a ClickHouse schema management project.
	// The project operates within the specified root directory.
	Project struct {
		RootDir string
		fmtr    *format.Formatter
	}

	ProjectParams struct {
		fx.In

		Dir       string `name:"project_dir"`
		Formatter *format.Formatter
	}
)

// New creates a new Project instance for managing ClickHouse schema projects.
// Takes the root directory path where the project is located.
//
// Example:
//
//	// Create a project for a specific directory
//	proj := project.New("/path/to/my/project")
//
//	// Initialize a new project structure
//	err := proj.Initialize(project.InitOptions{})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// Get migrations directory path
//	migrationsDir := proj.MigrationsDir()
//	fmt.Printf("Migrations directory: %s\n", migrationsDir)
func New(p ProjectParams) *Project {
	return &Project{
		RootDir: p.Dir,
		fmtr:    p.Formatter,
	}
}

// Initialize sets up a new project directory structure.
// This method is idempotent - it will only create missing files and directories,
// preserving any existing content. It creates the standard housekeeper project
// structure including db/, migrations/, and schema directories along with
// configuration files in the project's root directory.
//
// Example:
//
//	// Create and initialize a new project
//	proj := project.New(project.ProjectParams{
//		Dir:       "/path/to/my/project",
//		Formatter: format.New(format.Defaults),
//	})
//	err := proj.Initialize(project.InitOptions{})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Initialize with custom cluster
//	proj := project.New(project.ProjectParams{
//		Dir:       "/path/to/my/project",
//		Formatter: format.New(format.Defaults),
//	})
//	err := proj.Initialize(project.InitOptions{
//		Cluster: "production",
//	})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
func (p *Project) Initialize(options InitOptions) error {
	// Ensure the project directory exists
	if err := os.MkdirAll(p.RootDir, consts.ModeDir); err != nil {
		return errors.Wrapf(err, "failed to create project directory %s", p.RootDir)
	}

	// Prepare template data
	data := templateData(options)
	if data.Cluster == "" {
		data.Cluster = "cluster" // default cluster name
	}

	// Use the unified overlayFS method to materialize the embedded image
	return p.overlayFS(image, &data)
}

// Dir returns the root directory of the project.
func (p *Project) Dir() string {
	return p.RootDir
}

func (p *Project) MigrationsDir() string {
	return filepath.Join(p.RootDir, "db", "migrations")
}

// BootstrapFromSchema creates project files from a parsed SQL schema.
// This method is used by the bootstrap command to extract schema from an
// existing ClickHouse instance and organize it into a project structure.
//
// The function is idempotent - existing files are preserved to avoid
// overwriting user modifications.
//
// Parameters:
//   - sql: The parsed SQL containing DDL statements to organize into project structure
//
// Returns an error if schema generation or file operations fail.
func (p *Project) BootstrapFromSchema(sql *parser.SQL) error {
	// Generate file system image from parsed SQL
	fsImage, err := p.generateImage(sql)
	if err != nil {
		return errors.Wrap(err, "failed to generate schema image")
	}

	// Overlay the generated image without template processing
	return p.overlayFS(fsImage, nil)
}

// overlayFS writes the contents of an fs.FS to the project directory,
// creating necessary directory structure and files that don't already exist.
// If templateData is provided, files containing template syntax will be rendered.
// This provides a unified way to materialize virtual file systems onto disk.
//
// The function is idempotent - existing files are preserved to avoid
// overwriting user modifications.
//
// Parameters:
//   - source: The virtual file system to overlay
//   - data: Optional template data for rendering files with Go template syntax
//
// Returns an error if any file or directory operation fails.
func (p *Project) overlayFS(source fs.FS, data *templateData) error {
	return fs.WalkDir(source, ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip the root directory
		if path == "." {
			return nil
		}

		targetPath := filepath.Join(p.RootDir, path)

		if d.IsDir() {
			// Create directory if it doesn't exist
			if err := os.MkdirAll(targetPath, consts.ModeDir); err != nil {
				return errors.Wrapf(err, "failed to create directory %s", targetPath)
			}
			return nil
		}

		// Check if file already exists
		if _, err := os.Stat(targetPath); err == nil {
			// File exists, skip it to preserve user modifications
			return nil
		} else if !os.IsNotExist(err) {
			// Some other error occurred
			return errors.Wrapf(err, "failed to stat file %s", targetPath)
		}

		// Read the file content from source
		file, err := source.Open(path)
		if err != nil {
			return errors.Wrapf(err, "failed to open source file %s", path)
		}
		defer file.Close()

		content, err := io.ReadAll(file)
		if err != nil {
			return errors.Wrapf(err, "failed to read source file %s", path)
		}

		// Apply template rendering if data is provided
		if data != nil {
			content, err = p.renderTemplate(path, content, *data)
			if err != nil {
				return errors.Wrapf(err, "failed to render template for %s", path)
			}
		}

		// Ensure parent directory exists
		parentDir := filepath.Dir(targetPath)
		if err := os.MkdirAll(parentDir, consts.ModeDir); err != nil {
			return errors.Wrapf(err, "failed to create parent directory %s", parentDir)
		}

		// Write the file
		if err := os.WriteFile(targetPath, content, consts.ModeFile); err != nil {
			return errors.Wrapf(err, "failed to write file %s", targetPath)
		}

		return nil
	})
}

// renderTemplate efficiently renders file content as a Go template.
// It only parses content as a template if it contains template syntax ({{ or }}).
// This provides maximum efficiency for files that don't need templating.
func (p *Project) renderTemplate(name string, content []byte, data templateData) ([]byte, error) {
	contentStr := string(content)

	// Fast path: if content doesn't contain template syntax, return as-is
	// This avoids template parsing overhead for most files
	if !bytes.Contains(content, []byte("{{")) && !bytes.Contains(content, []byte("}}")) {
		return content, nil
	}

	// Content contains template syntax, parse and execute
	tmpl, err := template.New(name).Parse(contentStr)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to parse template")
	}

	var buf bytes.Buffer
	if err := tmpl.Execute(&buf, data); err != nil {
		return nil, errors.Wrapf(err, "failed to execute template")
	}

	return buf.Bytes(), nil
}
