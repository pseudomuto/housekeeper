// Package schema provides configuration management for ClickHouse schema environments.
//
// This package handles loading and parsing YAML configuration files that define
// multiple database environments with their connection details and schema entry points.
// It supports both development and production URL configurations for flexible
// deployment scenarios.
//
// Key features:
//   - YAML-based configuration management
//   - Multiple environment support (local, staging, production, etc.)
//   - Separate development and production database URLs
//   - Schema entry point specification for each environment
//   - Comprehensive error handling with context wrapping
//
// Configuration Format:
//
// The YAML configuration file should follow this structure:
//
//	environments:
//	  - name: local
//	    dev: clickhouse://localhost:9000/dev
//	    url: clickhouse://localhost:9000/prod
//	    entrypoint: db/main.sql
//	  - name: staging
//	    dev: clickhouse://staging-dev:9000
//	    url: clickhouse://staging:9000
//	    entrypoint: db/staging.sql
//	  - name: production
//	    url: clickhouse://prod:9000
//	    entrypoint: db/production.sql
//
// Usage:
//
//	import (
//		"fmt"
//		"github.com/pseudomuto/housekeeper/pkg/schema"
//	)
//
//	// Load configuration from file
//	config, err := schema.LoadConfigFile("schema.yaml")
//	if err != nil {
//		panic(err)
//	}
//
//	// Access environments
//	for _, env := range config.Envs {
//		fmt.Printf("Environment: %s\n", env.Name)
//		fmt.Printf("URL: %s\n", env.URL)
//		if env.DevURL != "" {
//			fmt.Printf("Dev URL: %s\n", env.DevURL)
//		}
//		fmt.Printf("Entry point: %s\n", env.Entrypoint)
//	}
//
//	// Load from io.Reader
//	file, _ := os.Open("config.yaml")
//	config, err = schema.LoadConfig(file)
//
// The package integrates with the broader Housekeeper migration tool to provide
// environment-specific database connections and schema management capabilities.
package schema
