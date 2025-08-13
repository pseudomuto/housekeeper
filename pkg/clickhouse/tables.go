package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// extractTables retrieves all table definitions from the ClickHouse instance.
// This function queries the system.tables table to get complete table information
// and returns them as parsed DDL statements.
//
// System tables and tables from system databases are automatically excluded.
// Views (both regular and materialized) are handled separately by ExtractViews.
// All DDL statements are validated using the parser before being returned.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	tables, err := client.GetTables(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract tables: %v", err)
//	}
//
//	// Process the parsed table statements
//	for _, stmt := range tables.Statements {
//		if stmt.CreateTable != nil {
//			name := stmt.CreateTable.Name
//			if stmt.CreateTable.Database != nil {
//				name = *stmt.CreateTable.Database + "." + name
//			}
//			fmt.Printf("Table: %s\n", name)
//		}
//	}
//
// Returns a *parser.SQL containing table CREATE statements or an error if extraction fails.
func extractTables(ctx context.Context, client *Client) (*parser.SQL, error) {
	condition, params := buildSystemDatabaseExclusion("database")
	query := fmt.Sprintf(`
		SELECT 
			create_table_query
		FROM system.tables
		WHERE %s
		  AND engine NOT IN ('View', 'MaterializedView')  -- Views are handled separately
		  AND is_temporary = 0
		  AND name NOT LIKE '.inner_id.%%'  -- Exclude internal materialized view tables
		  AND name NOT LIKE '.inner.%%'  -- Exclude other internal tables
		ORDER BY database, name
	`, condition)

	rows, err := client.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query tables")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var createQuery string
		if err := rows.Scan(&createQuery); err != nil {
			return nil, errors.Wrap(err, "failed to scan table row")
		}

		// Skip if the query is empty
		if createQuery == "" {
			continue
		}

		// Clean up the CREATE statement
		cleanedQuery := cleanCreateStatement(createQuery)

		// Skip dictionary definitions that might appear in system.tables
		if strings.Contains(cleanedQuery, "CREATE DICTIONARY") {
			continue
		}

		// Validate the statement using our parser
		if err := validateDDLStatement(cleanedQuery); err != nil {
			// Include the problematic query in the error for debugging
			return nil, errors.Wrapf(err, "generated invalid DDL for table (query: %s)", cleanedQuery)
		}

		statements = append(statements, cleanedQuery)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating table rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseString(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined table DDL")
	}

	return sqlResult, nil
}
