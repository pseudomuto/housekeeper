package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// extractViews retrieves all view definitions (both regular and materialized) from the ClickHouse instance.
// This function queries the system.tables table to get complete view information and returns them
// as parsed DDL statements, handling both regular views and materialized views.
//
// System views are automatically excluded. All DDL statements are validated
// using the parser before being returned.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	views, err := client.GetViews(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract views: %v", err)
//	}
//
//	// Process the parsed view statements
//	for _, stmt := range views.Statements {
//		if stmt.CreateView != nil {
//			viewType := "VIEW"
//			if stmt.CreateView.Materialized {
//				viewType = "MATERIALIZED VIEW"
//			}
//			name := stmt.CreateView.Name
//			if stmt.CreateView.Database != nil {
//				name = *stmt.CreateView.Database + "." + name
//			}
//			fmt.Printf("%s: %s\n", viewType, name)
//		}
//	}
//
// Returns a *parser.SQL containing view CREATE statements or an error if extraction fails.
func extractViews(ctx context.Context, client *Client) (*parser.SQL, error) {
	query := `
		SELECT 
			create_table_query
		FROM system.tables
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		  AND engine IN ('View', 'MaterializedView')
		ORDER BY database, name
	`

	rows, err := client.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query views: %w", err)
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var createQuery string
		if err := rows.Scan(&createQuery); err != nil {
			return nil, fmt.Errorf("failed to scan view row: %w", err)
		}

		// Clean up the CREATE statement
		// ClickHouse returns CREATE VIEW name (col1 Type1, ...) AS SELECT
		// but our parser expects CREATE VIEW name AS SELECT
		cleanedQuery := cleanCreateStatement(createQuery)
		openParen := strings.Index(cleanedQuery, "(")
		asPos := strings.Index(cleanedQuery, "AS")
		cleanedQuery = cleanedQuery[:openParen] + cleanedQuery[asPos:]

		// Validate the statement using our parser
		if err := validateDDLStatement(cleanedQuery); err != nil {
			// Include the problematic query in the error for debugging
			return nil, fmt.Errorf("generated invalid DDL for view (query: %s): %w", cleanedQuery, err)
		}

		statements = append(statements, cleanedQuery)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating view rows: %w", err)
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseSQL(combinedSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse combined view DDL: %w", err)
	}

	return sqlResult, nil
}
