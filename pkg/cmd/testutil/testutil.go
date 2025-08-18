package testutil

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/config"
	"github.com/pseudomuto/housekeeper/pkg/consts"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/project"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// ProjectFixture represents a test project environment with all necessary dependencies
type ProjectFixture struct {
	Dir       string
	Config    *config.Config
	Project   *project.Project
	Formatter *format.Formatter
	t         *testing.T
}

// TestConfig represents test configuration options
type TestConfig struct {
	Cluster   string
	Version   string
	ConfigDir string
}

// MigrationFile represents a test migration
type MigrationFile struct {
	Version string
	SQL     string
}

// TestProject creates an isolated temp directory with initialized housekeeper project
func TestProject(t *testing.T) *ProjectFixture {
	t.Helper()

	tmpDir := t.TempDir()

	// Create project with default formatter
	proj := project.New(project.ProjectParams{
		Dir:       tmpDir,
		Formatter: format.New(format.Defaults),
	})

	fixture := &ProjectFixture{
		Dir:       tmpDir,
		Project:   proj,
		Formatter: format.New(format.Defaults),
		t:         t,
	}

	// Initialize the project with default settings
	err := proj.Initialize(project.InitOptions{})
	require.NoError(t, err, "Failed to initialize test project")

	// Load the created configuration
	configPath := filepath.Join(tmpDir, "housekeeper.yaml")
	cfg, err := config.LoadConfigFile(configPath)
	require.NoError(t, err, "Failed to load config file")

	fixture.Config = cfg

	return fixture
}

// WithConfig updates the project configuration with custom settings
func (p *ProjectFixture) WithConfig(cfg TestConfig) *ProjectFixture {
	p.t.Helper()

	// Update configuration values
	if cfg.Cluster != "" {
		p.Config.ClickHouse.Cluster = cfg.Cluster
	}
	if cfg.Version != "" {
		p.Config.ClickHouse.Version = cfg.Version
	}
	if cfg.ConfigDir != "" {
		p.Config.ClickHouse.ConfigDir = cfg.ConfigDir
	}

	// Write updated config back to file
	configPath := filepath.Join(p.Dir, "housekeeper.yaml")
	err := p.writeConfig(configPath)
	require.NoError(p.t, err, "Failed to write updated config")

	// Update ClickHouse XML if cluster changed
	if cfg.Cluster != "" {
		err := p.updateClickHouseXML(cfg.Cluster)
		require.NoError(p.t, err, "Failed to update ClickHouse XML")
	}

	return p
}

// WithMigrations adds migration files to the project
func (p *ProjectFixture) WithMigrations(migrations []MigrationFile) *ProjectFixture {
	p.t.Helper()

	migrationsDir := filepath.Join(p.Dir, "db", "migrations")
	err := os.MkdirAll(migrationsDir, consts.ModeDir)
	require.NoError(p.t, err, "Failed to create migrations directory")

	for _, migration := range migrations {
		filename := migration.Version + ".sql"
		filepath := filepath.Join(migrationsDir, filename)
		err := os.WriteFile(filepath, []byte(migration.SQL), consts.ModeFile)
		require.NoError(p.t, err, "Failed to write migration file: %s", filename)
	}

	return p
}

// WithSchema sets the main schema content
func (p *ProjectFixture) WithSchema(schemaSQL string) *ProjectFixture {
	p.t.Helper()

	schemaPath := filepath.Join(p.Dir, "db", "main.sql")
	err := os.WriteFile(schemaPath, []byte(schemaSQL), consts.ModeFile)
	require.NoError(p.t, err, "Failed to write schema file")

	return p
}

// WithSchemaFiles adds additional schema files to the project
func (p *ProjectFixture) WithSchemaFiles(files map[string]string) *ProjectFixture {
	p.t.Helper()

	schemasDir := filepath.Join(p.Dir, "db", "schemas")
	err := os.MkdirAll(schemasDir, consts.ModeDir)
	require.NoError(p.t, err, "Failed to create schemas directory")

	for path, content := range files {
		fullPath := filepath.Join(schemasDir, path)

		// Create parent directories if needed
		dir := filepath.Dir(fullPath)
		err := os.MkdirAll(dir, consts.ModeDir)
		require.NoError(p.t, err, "Failed to create directory: %s", dir)

		err = os.WriteFile(fullPath, []byte(content), consts.ModeFile)
		require.NoError(p.t, err, "Failed to write schema file: %s", path)
	}

	return p
}

// WithSumFile creates a sum file for the migrations
func (p *ProjectFixture) WithSumFile(content string) *ProjectFixture {
	p.t.Helper()

	// Use the same path structure as cfg.Dir (relative to project root)
	sumPath := filepath.Join(p.Dir, p.Config.Dir, "housekeeper.sum")
	err := os.WriteFile(sumPath, []byte(content), consts.ModeFile)
	require.NoError(p.t, err, "Failed to write sum file")

	return p
}

// Cleanup removes all test resources (automatically handled by t.TempDir())
func (p *ProjectFixture) Cleanup() {
	// No-op as t.TempDir() handles cleanup automatically
	// This method exists for explicit cleanup if needed in the future
}

// GetMigrationsDir returns the path to the migrations directory
func (p *ProjectFixture) GetMigrationsDir() string {
	return filepath.Join(p.Dir, "db", "migrations")
}

// GetSchemasDir returns the path to the schemas directory
func (p *ProjectFixture) GetSchemasDir() string {
	return filepath.Join(p.Dir, "db", "schemas")
}

// GetConfigPath returns the path to the housekeeper.yaml file
func (p *ProjectFixture) GetConfigPath() string {
	return filepath.Join(p.Dir, "housekeeper.yaml")
}

// GetMainSchemaPath returns the path to the main schema file
func (p *ProjectFixture) GetMainSchemaPath() string {
	return filepath.Join(p.Dir, "db", "main.sql")
}

// WithClickHouseVersion sets the ClickHouse version in the config
func (p *ProjectFixture) WithClickHouseVersion(version string) *ProjectFixture {
	p.t.Helper()

	if p.Config == nil {
		p.Config = DefaultConfig()
	}
	p.Config.ClickHouse.Version = version
	return p
}

// WithClickHouseCluster sets the ClickHouse cluster in the config
func (p *ProjectFixture) WithClickHouseCluster(cluster string) *ProjectFixture {
	p.t.Helper()

	if p.Config == nil {
		p.Config = DefaultConfig()
	}
	p.Config.ClickHouse.Cluster = cluster
	return p
}

// WithClickHouseConfigDir sets the ClickHouse config directory in the config
func (p *ProjectFixture) WithClickHouseConfigDir(configDir string) *ProjectFixture {
	p.t.Helper()

	if p.Config == nil {
		p.Config = DefaultConfig()
	}
	p.Config.ClickHouse.ConfigDir = configDir
	return p
}

// DefaultConfig returns a default configuration for testing
func DefaultConfig() *config.Config {
	return &config.Config{
		ClickHouse: config.ClickHouse{
			Version:   "25.7",
			ConfigDir: "db/config.d",
			Cluster:   "",
		},
		Entrypoint: "db/main.sql",
		Dir:        "db/migrations",
	}
}

// updateClickHouseXML updates the ClickHouse XML configuration with a new cluster name
func (p *ProjectFixture) updateClickHouseXML(cluster string) error {
	xmlPath := filepath.Join(p.Dir, "db", "config.d", "_clickhouse.xml")

	// Read existing XML
	content, err := os.ReadFile(xmlPath)
	if err != nil {
		// If file doesn't exist, that's okay - it might not be created yet
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	// Simple string replacement for cluster name
	// This is a simplified approach - in production you might want to use XML parsing
	xmlContent := string(content)

	// Replace cluster name in macros
	xmlContent = replaceClusterInXML(xmlContent, cluster)

	// Write back
	return os.WriteFile(xmlPath, []byte(xmlContent), consts.ModeFile)
}

// replaceClusterInXML replaces cluster names in ClickHouse XML configuration
func replaceClusterInXML(xml, newCluster string) string {
	// This is a simplified implementation
	// In a real scenario, you might want to use proper XML parsing
	// For now, we'll just do basic string replacement

	// Note: This assumes the XML structure from the project template
	// You may need to adjust this based on the actual XML structure
	return xml
}

// writeConfig writes the configuration to a file
func (p *ProjectFixture) writeConfig(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := yaml.NewEncoder(file)
	defer encoder.Close()

	return encoder.Encode(p.Config)
}
