package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// cleanViewStatement cleans up a CREATE VIEW/MATERIALIZED VIEW statement from ClickHouse
// to make it parseable by our parser. This handles:
// - Column definitions that ClickHouse adds after TO/APPEND TO table_name
// - DEFINER clause that ClickHouse adds
// - Ensuring proper semicolon termination
func cleanViewStatement(createQuery string) string {
	cleaned := strings.TrimSpace(createQuery)
	if !strings.HasSuffix(cleaned, ";") {
		cleaned += ";"
	}

	// Remove DEFINER clause: "DEFINER = username SQL SECURITY DEFINER"
	// This appears before AS in ClickHouse output
	definerPattern := regexp.MustCompile(`\s+DEFINER\s*=\s*\S+\s+SQL\s+SECURITY\s+DEFINER`)
	cleaned = definerPattern.ReplaceAllString(cleaned, "")

	// ClickHouse may return CREATE VIEW name (col1 Type1, ...) AS SELECT
	// or CREATE MATERIALIZED VIEW name ... APPEND TO table_name (col1 Type1, ...) AS SELECT
	// We need to remove the column definitions if they exist
	// Find the first ( that appears after the view name and before AS
	asPos := strings.Index(cleaned, " AS ")
	if asPos > 0 {
		// Find the first ( before AS
		prefixPart := cleaned[:asPos]
		parenPos := strings.Index(prefixPart, "(")
		if parenPos > 0 {
			// There are column definitions between the opening paren and AS
			// Remove them by taking everything before the opening paren and after AS
			cleaned = cleaned[:parenPos] + cleaned[asPos:]
		}
	}

	return cleaned
}

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
	condition, params := buildDatabaseExclusion("database", client.options.IgnoreDatabases)
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

		// Clean up the CREATE statement - first remove ClickHouse-specific clauses
		cleanedQuery := cleanViewStatement(createQuery)

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
