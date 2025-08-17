package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
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
	condition, params := buildSystemDatabaseExclusion("database")
	query := fmt.Sprintf(`
		SELECT 
			create_table_query
		FROM system.tables
		WHERE %s
		  AND engine IN ('View', 'MaterializedView')
		ORDER BY database, name
	`, condition)

	rows, err := client.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query views")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var createQuery string
		if err := rows.Scan(&createQuery); err != nil {
			return nil, errors.Wrap(err, "failed to scan view row")
		}

		// Clean up the CREATE statement
		cleanedQuery := cleanCreateStatement(createQuery)

		// ClickHouse may return CREATE VIEW name (col1 Type1, ...) AS SELECT
		// but our parser expects CREATE VIEW name AS SELECT
		// We need to remove the column definitions if they exist

		// Find the positions safely
		viewNameEnd := strings.Index(cleanedQuery, "(")
		asPos := strings.Index(cleanedQuery, " AS ")

		if viewNameEnd > 0 && asPos > viewNameEnd {
			// There are column definitions between the view name and AS
			// Remove them by taking everything before the opening paren and after AS
			cleanedQuery = cleanedQuery[:viewNameEnd] + cleanedQuery[asPos:]
		}
		// Otherwise, the query is already in the correct format

		// Validate the statement using our parser
		if err := validateDDLStatement(cleanedQuery); err != nil {
			// Include the problematic query in the error for debugging
			return nil, errors.Wrapf(err, "generated invalid DDL for view (query: %s)", cleanedQuery)
		}

		statements = append(statements, cleanedQuery)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating view rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseString(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined view DDL")
	}

	return sqlResult, nil
}
