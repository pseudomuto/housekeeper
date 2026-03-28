package parser

// InjectOnCluster adds ON CLUSTER clauses to all DDL statements when cluster is specified.
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
//   - CREATE ROLE statements
//   - GRANT/REVOKE statements
//
// Housekeeper internal objects (database 'housekeeper' and its objects) are excluded
// from ON CLUSTER injection as they should be shard-local for migration tracking.
//
// Other statement types (ALTER, DROP, etc.) are left unchanged as they're not typically
// part of schema extraction output.
func InjectOnCluster(statements []*Statement, cluster string) []*Statement {
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
		case stmt.CreateRole != nil:
			// Roles are cluster-wide by nature
			stmt.CreateRole.OnCluster = clusterName
		case stmt.Grant != nil:
			// Grants are cluster-wide by nature
			stmt.Grant.OnCluster = clusterName
		case stmt.Revoke != nil:
			// Revokes are cluster-wide by nature
			stmt.Revoke.OnCluster = clusterName
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
