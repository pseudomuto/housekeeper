package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// extractDatabases retrieves all database definitions from the ClickHouse instance.
// This function queries the system.databases table to get complete database information
// and returns them as parsed DDL statements.
//
// System databases (system, information_schema) are automatically excluded.
// All generated DDL statements are validated using the parser before being returned.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	databases, err := client.GetDatabases(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract databases: %v", err)
//	}
//
//	// Process the parsed database statements
//	for _, stmt := range databases.Statements {
//		if stmt.CreateDatabase != nil {
//			fmt.Printf("Database: %s\n", stmt.CreateDatabase.Name)
//		}
//	}
//
// Returns a *parser.SQL containing database CREATE statements or an error if extraction fails.
func extractDatabases(ctx context.Context, client *Client) (*parser.SQL, error) {
	query := `
		SELECT 
			name,
			engine,
			comment
		FROM system.databases 
		WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY name
	`

	rows, err := client.conn.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query databases")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var name, engine string
		var comment sql.NullString

		if err := rows.Scan(&name, &engine, &comment); err != nil {
			return nil, errors.Wrap(err, "failed to scan database row")
		}

		// Generate CREATE DATABASE statement
		ddl := generateDatabaseDDL(name, engine, comment.String)

		// Validate the generated statement using our parser
		if err := validateDDLStatement(ddl); err != nil {
			return nil, errors.Wrapf(err, "generated invalid DDL for database %s", name)
		}

		statements = append(statements, ddl)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating database rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseSQL(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined database DDL")
	}

	return sqlResult, nil
}

// generateDatabaseDDL creates a CREATE DATABASE DDL statement from database metadata.
func generateDatabaseDDL(name, engine, comment string) string {
	var parts []string

	parts = append(parts, "CREATE DATABASE", name)

	if engine != "" && engine != "Atomic" {
		// Only specify engine if it's not the default Atomic engine
		parts = append(parts, "ENGINE =", engine)
	}

	if comment != "" {
		parts = append(parts, "COMMENT", fmt.Sprintf("'%s'", strings.ReplaceAll(comment, "'", "\\'")))
	}

	return strings.Join(parts, " ") + ";"
}
