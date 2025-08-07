package clickhouse

import (
	"context"
	"fmt"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// DumpSchema retrieves all schema objects (databases, tables, dictionaries, views)
// and returns them as a parsed SQL structure ready for use with migration generation.
//
// This function combines all individual extraction functions to provide a complete view of the
// ClickHouse schema. It's the primary function for getting schema information for migration
// generation and schema comparison operations.
//
// The extraction follows this order:
//  1. Databases - extracted first as they define the namespace
//  2. Tables - extracted with full DDL statements
//  3. Dictionaries - dictionary definitions with source/layout/lifetime
//  4. Views - both regular and materialized views (extracted last since they may depend on dictionaries)
//
// All system objects are automatically excluded and all DDL statements are validated.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	schema, err := clickhouse.DumpSchema(ctx, client)
//	if err != nil {
//		log.Fatalf("Failed to get schema: %v", err)
//	}
//
//	// Use with migration generation
//	migration, err := migrator.GenerateMigration(currentSchema, targetSchema, "update")
//	if err != nil {
//		log.Fatal(err)
//	}
//
// Returns a parser.SQL containing all schema objects or an error if extraction fails.
func DumpSchema(ctx context.Context, client *Client) (*parser.SQL, error) {
	var allStatements []*parser.Statement

	// Extract databases
	databases, err := extractDatabases(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to extract databases: %w", err)
	}
	allStatements = append(allStatements, databases.Statements...)

	// Extract tables
	tables, err := extractTables(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to extract tables: %w", err)
	}
	allStatements = append(allStatements, tables.Statements...)

	// Extract dictionaries
	dictionaries, err := extractDictionaries(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to extract dictionaries: %w", err)
	}
	allStatements = append(allStatements, dictionaries.Statements...)

	// Extract views (after dictionaries since materialized views might depend on them)
	views, err := extractViews(ctx, client)
	if err != nil {
		return nil, fmt.Errorf("failed to extract views: %w", err)
	}
	allStatements = append(allStatements, views.Statements...)

	// Combine all statements into a single SQL structure
	return &parser.SQL{Statements: allStatements}, nil
}
