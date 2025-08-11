// Package project provides comprehensive ClickHouse project management capabilities
// including project initialization, configuration management, and schema compilation
// with import directive support.
//
// # Project Management
//
// The project package enables structured management of ClickHouse schemas through
// a standardized project layout and configuration system. It provides idempotent
// project initialization that creates the necessary directory structure and
// configuration files while preserving existing content.
//
// # Key Features
//
//   - Project initialization with standard directory layout
//   - Multi-environment configuration support
//   - Schema compilation with recursive import processing
//   - Template-based project bootstrapping
//   - Integration with the ClickHouse DDL parser
//   - Docker-based ClickHouse management for migration testing
//
// # Project Structure
//
// A housekeeper project follows this standard layout:
//
//	project-root/
//	├── housekeeper.yaml        # Environment configuration
//	├── db/
//	│   ├── main.sql            # Main schema entrypoint
//	│   ├── migrations/
//	│   │   └── dev/            # Development migrations
//	│   └── schemas/            # Organized schema files
//
// # Import System
//
// The schema compilation system supports recursive imports using special
// comment directives:
//
//	-- housekeeper:import path/to/schema.sql
//
// Import paths are resolved relative to the current file's directory, enabling
// modular schema organization and reusability across environments.
//
// # Usage Example
//
//	// Initialize a new project
//	proj, err := project.Initialize("/path/to/my/project", project.InitOptions{})
//	if err != nil {
//		log.Fatal("Failed to initialize project:", err)
//	}
//
//	// Or load existing project
//	config, err := project.LoadConfigFile("/path/to/housekeeper.yaml")
//	if err != nil {
//		log.Fatal("Failed to load config:", err)
//	}
//	proj := project.New(project.NewProjectParams{
//		Dir:    "/path/to/my/project",
//		Config: config,
//	})
//
//	// Parse schema
//	grammar, err := proj.ParseSchema()
//	if err != nil {
//		log.Fatal("Failed to parse schema:", err)
//	}
//
//	// Process the parsed DDL statements
//	for _, stmt := range grammar.Statements {
//		if stmt.CreateTable != nil {
//			fmt.Printf("Found table: %s\n", stmt.CreateTable.Name)
//		}
//	}
//
//	// Use Docker for migration testing
//	dm := project.NewDockerManager()
//	defer dm.Stop(ctx)
//
//	if err := dm.Start(ctx); err != nil {
//		log.Fatal("Failed to start ClickHouse:", err)
//	}
//
//	// Apply migrations and test schema
//	dsn := dm.GetDSN() // localhost:9000
package project
