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
	// or CREATE MATERIALIZED VIEW name ... APPEND TO table_name (col1 Type1, ...) AS SELECT/WITH
	// We need to remove the column definitions if they exist
	//
	// Find the main "AS" that introduces the SELECT query (not CTEs like "cte_name AS (SELECT...)")
	// The main AS is followed by either SELECT or WITH (for CTEs)
	mainAsPattern := regexp.MustCompile(`\)\s*AS\s+(SELECT|WITH)\s`)
	mainAsMatch := mainAsPattern.FindStringIndex(cleaned)

	if mainAsMatch != nil {
		// Found pattern like ") AS SELECT" or ") AS WITH"
		// The column definitions are between the first ( and this )
		prefixEnd := mainAsMatch[0] + 1 // Position right after the closing )
		prefixPart := cleaned[:prefixEnd]

		// Find the opening ( of column definitions (not function calls)
		// It should be after APPEND TO table_name or after view_name
		parenPos := findColumnDefOpenParen(prefixPart)
		if parenPos > 0 {
			// Remove column definitions: take before ( and from AS onwards
			asPos := mainAsMatch[0] + 1 // Position of the space before AS
			cleaned = cleaned[:parenPos] + cleaned[asPos:]
		}
	}

	return cleaned
}

// findColumnDefOpenParen finds the opening parenthesis of column definitions
// in a materialized view statement prefix (everything up to and including the closing paren).
// Returns -1 if no column definitions are found.
func findColumnDefOpenParen(prefix string) int {
	// The column definitions open paren should be:
	// 1. After "APPEND TO table_name" or "TO table_name" for MVs
	// 2. After the view name for regular views
	// 3. NOT be part of a function call like LowCardinality(...)

	// Look for patterns like "TO table_name\n(" or "TO table_name ("
	toPattern := regexp.MustCompile(`TO\s+[\w.]+\s*\(`)
	toMatch := toPattern.FindStringIndex(prefix)
	if toMatch != nil {
		// Return position of the ( in the match
		return strings.LastIndex(prefix[:toMatch[1]], "(")
	}

	// For regular views without TO clause, look for "VIEW name\n(" or "VIEW name ("
	// after any ON CLUSTER clause
	viewPattern := regexp.MustCompile(`VIEW\s+[\w.]+(?:\s+ON\s+CLUSTER\s+[\w.]+)?\s*\(`)
	viewMatch := viewPattern.FindStringIndex(prefix)
	if viewMatch != nil {
		return strings.LastIndex(prefix[:viewMatch[1]], "(")
	}

	return -1
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
