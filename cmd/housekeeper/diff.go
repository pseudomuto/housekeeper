package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/pseudomuto/housekeeper/pkg/migrator"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/urfave/cli/v3"
)

// diffCommand creates the diff command for comparing schema with database state
func diffCommand() *cli.Command {
	return &cli.Command{
		Name:  "diff",
		Usage: "Compare schema definition with current database state",
		Description: `Compare the desired schema SQL files with the current database 
state and generate migration files for any differences found.`,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "schema",
				Aliases: []string{"s"},
				Usage:   "Path to schema SQL files",
				Value:   "./schema",
			},
			&cli.StringFlag{
				Name:    "migrations",
				Aliases: []string{"m"},
				Usage:   "Path to migrations directory",
				Value:   "./migrations",
			},
			&cli.StringFlag{
				Name:    "name",
				Aliases: []string{"n"},
				Usage:   "Name for the migration",
			},
		},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			dsn := cmd.String("dsn")
			schemaPath := cmd.String("schema")
			migrationsPath := cmd.String("migrations")
			migrationName := cmd.String("name")

			return runDiff(ctx, dsn, schemaPath, migrationsPath, migrationName)
		},
	}
}

// runDiff executes the diff operation to compare schema with database state
func runDiff(ctx context.Context, dsn, schemaPath, migrationsPath, migrationName string) error {
	// Parse target schema (databases and dictionaries)
	fmt.Println("Parsing schema files...")
	targetGrammar, err := parser.ParseSQLFromDirectory(schemaPath)
	if err != nil {
		return fmt.Errorf("failed to parse schema: %w", err)
	}

	// Connect to ClickHouse
	fmt.Printf("Connecting to ClickHouse at %s...\n", dsn)
	client, err := clickhouse.NewClient(ctx, dsn)
	if err != nil {
		return fmt.Errorf("failed to connect to ClickHouse: %w", err)
	}
	defer client.Close()

	// Get current schema (databases and dictionaries)
	fmt.Println("Reading current schema...")
	currentGrammar, err := client.GetSchema(ctx)
	if err != nil {
		return fmt.Errorf("failed to get current schema: %w", err)
	}

	// Generate migration for database operations
	if migrationName == "" {
		migrationName = "auto_migration"
	}

	fmt.Println("Generating schema migration...")
	migration, err := migrator.GenerateMigration(currentGrammar, targetGrammar, migrationName)
	if err != nil {
		if err.Error() == "no differences found" {
			fmt.Println("No differences found. Schema is up to date.")
			return nil
		}
		return fmt.Errorf("failed to generate migration: %w", err)
	}

	// Display differences by parsing the generated migration
	fmt.Printf("\nGenerated migration with the following changes:\n")
	upLines := strings.Split(migration.Up, "\n")
	changeCount := 0
	for _, line := range upLines {
		if strings.HasPrefix(line, "-- Create database") ||
			strings.HasPrefix(line, "-- Alter database") ||
			strings.HasPrefix(line, "-- Drop database") ||
			strings.HasPrefix(line, "-- Create dictionary") ||
			strings.HasPrefix(line, "-- Replace dictionary") ||
			strings.HasPrefix(line, "-- Drop dictionary") {
			changeCount++
			fmt.Printf("%d. %s\n", changeCount, strings.TrimPrefix(line, "-- "))
		}
	}

	// Create migrations directory if it doesn't exist
	if err := os.MkdirAll(migrationsPath, 0o755); err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// Write migration files
	upFile := filepath.Join(migrationsPath, fmt.Sprintf("%s_%s.up.sql", migration.Version, migration.Name))
	downFile := filepath.Join(migrationsPath, fmt.Sprintf("%s_%s.down.sql", migration.Version, migration.Name))

	if err := os.WriteFile(upFile, []byte(migration.Up), 0o600); err != nil {
		return fmt.Errorf("failed to write up migration: %w", err)
	}

	if err := os.WriteFile(downFile, []byte(migration.Down), 0o600); err != nil {
		return fmt.Errorf("failed to write down migration: %w", err)
	}

	fmt.Printf("\nMigration files created:\n")
	fmt.Printf("  Up:   %s\n", upFile)
	fmt.Printf("  Down: %s\n", downFile)

	return nil
}
