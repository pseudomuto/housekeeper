package clickhouse

import (
	"context"

	"github.com/pkg/errors"
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
		return nil, errors.Wrap(err, "failed to extract databases")
	}
	allStatements = append(allStatements, databases.Statements...)

	// Extract tables
	tables, err := extractTables(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract tables")
	}
	allStatements = append(allStatements, tables.Statements...)

	// Extract dictionaries
	dictionaries, err := extractDictionaries(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract dictionaries")
	}
	allStatements = append(allStatements, dictionaries.Statements...)

	// Extract views (after dictionaries since materialized views might depend on them)
	views, err := extractViews(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract views")
	}
	allStatements = append(allStatements, views.Statements...)

	// Inject ON CLUSTER clauses if cluster is specified
	if client.options.Cluster != "" {
		allStatements = injectOnCluster(allStatements, client.options.Cluster)
	}

	// Combine all statements into a single SQL structure
	return &parser.SQL{Statements: allStatements}, nil
}

// injectOnCluster adds ON CLUSTER clauses to all DDL statements when cluster is specified.
// This addresses the limitation in ClickHouse where system tables don't include ON CLUSTER
// information in dumped DDL statements. When running against a distributed ClickHouse cluster,
// this ensures all extracted DDL can be properly applied to cluster deployments.
//
// The function handles:
//   - CREATE DATABASE statements
//   - CREATE TABLE statements
//   - CREATE DICTIONARY statements
//   - CREATE VIEW statements (both regular and materialized)
//
// Other statement types (ALTER, DROP, etc.) are left unchanged as they're not typically
// part of schema extraction output.
func injectOnCluster(statements []*parser.Statement, cluster string) []*parser.Statement {
	clusterName := &cluster

	for _, stmt := range statements {
		switch {
		case stmt.CreateDatabase != nil:
			stmt.CreateDatabase.OnCluster = clusterName
		case stmt.CreateTable != nil:
			stmt.CreateTable.OnCluster = clusterName
		case stmt.CreateDictionary != nil:
			stmt.CreateDictionary.OnCluster = clusterName
		case stmt.CreateView != nil:
			stmt.CreateView.OnCluster = clusterName
		}
	}

	return statements
}
