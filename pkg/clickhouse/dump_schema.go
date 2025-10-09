package clickhouse

import (
	"context"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// DumpSchema retrieves all schema objects (databases, tables, named collections, dictionaries, views, users)
// and returns them as a parsed SQL structure ready for use with migration generation.
//
// This function combines all individual extraction functions to provide a complete view of the
// ClickHouse schema. It's the primary function for getting schema information for migration
// generation and schema comparison operations.
//
// The extraction follows this order:
//  1. Databases - extracted first as they define the namespace
//  2. Tables - extracted with full DDL statements
//  3. Named Collections - connection configurations that dictionaries might reference
//  4. Dictionaries - dictionary definitions with source/layout/lifetime
//  5. Views - both regular and materialized views (extracted since they may depend on dictionaries)
//  6. Users - user definitions (extracted last since they don't depend on other schema objects)
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
//	// Use with diff generation
//	diff, err := schema.GenerateDiff(currentSchema, targetSchema)
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

	// Extract named collections (before tables since integration engine tables might use them)
	namedCollections, err := extractNamedCollections(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract named collections")
	}
	allStatements = append(allStatements, namedCollections.Statements...)

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

	// Extract users (after all other schema objects since users don't depend on anything)
	users, err := extractUsers(ctx, client)
	if err != nil {
		return nil, errors.Wrap(err, "failed to extract users")
	}
	allStatements = append(allStatements, users.Statements...)

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
//   - CREATE NAMED COLLECTION statements
//   - CREATE DICTIONARY statements
//   - CREATE VIEW statements (both regular and materialized)
//   - CREATE USER statements
//
// Housekeeper internal objects (database 'housekeeper' and its objects) are excluded
// from ON CLUSTER injection as they should be shard-local for migration tracking.
//
// Other statement types (ALTER, DROP, etc.) are left unchanged as they're not typically
// part of schema extraction output.
func injectOnCluster(statements []*parser.Statement, cluster string) []*parser.Statement {
	// If cluster is empty, return statements unchanged
	if cluster == "" {
		return statements
	}

	clusterName := &cluster

	for _, stmt := range statements {
		switch {
		case stmt.CreateDatabase != nil:
			if !isHousekeeperDatabase(stmt.CreateDatabase.Name) {
				stmt.CreateDatabase.OnCluster = clusterName
			}
		case stmt.CreateTable != nil:
			dbName := getDatabaseName(stmt.CreateTable.Database)
			if !isHousekeeperDatabase(dbName) {
				stmt.CreateTable.OnCluster = clusterName
			}
		case stmt.CreateNamedCollection != nil:
			// Named collections are cluster-wide by nature
			stmt.CreateNamedCollection.OnCluster = clusterName
		case stmt.CreateDictionary != nil:
			dbName := getDatabaseName(stmt.CreateDictionary.Database)
			if !isHousekeeperDatabase(dbName) {
				stmt.CreateDictionary.OnCluster = clusterName
			}
		case stmt.CreateView != nil:
			dbName := getDatabaseName(stmt.CreateView.Database)
			if !isHousekeeperDatabase(dbName) {
				stmt.CreateView.OnCluster = clusterName
			}
		case stmt.CreateUser != nil:
			// Users are cluster-wide by nature
			stmt.CreateUser.OnCluster = clusterName
		}
	}

	return statements
}

// isHousekeeperDatabase determines if a database belongs to housekeeper's internal tracking system.
// Housekeeper databases and their objects should be shard-local and never created with ON CLUSTER clauses.
func isHousekeeperDatabase(database string) bool {
	return database == "housekeeper"
}

// getDatabaseName extracts the database name from a pointer, defaulting to "default" if nil.
func getDatabaseName(database *string) string {
	if database != nil && *database != "" {
		return *database
	}
	return "default"
}
