package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// GetFunctions retrieves all user-defined function definitions from the ClickHouse instance.
// It queries the system.functions table and reconstructs CREATE FUNCTION statements
// for user-defined functions only (system functions are filtered out).
//
// Uses the origin = 'SQLUserDefined' column for precise filtering, which is available
// in all supported ClickHouse versions (24.0+).
//
// Returns a *parser.SQL containing all function CREATE statements, or an error if the query fails.
func (c *Client) GetFunctions(ctx context.Context) (*parser.SQL, error) {
	query := `
		SELECT 
			name,
			create_query
		FROM system.functions 
		WHERE origin = 'SQLUserDefined'
		ORDER BY name
	`

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query system.functions")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var name, createQuery string
		if err := rows.Scan(&name, &createQuery); err != nil {
			return nil, errors.Wrap(err, "failed to scan function row")
		}

		// Parse the existing create query to modify it for cluster support
		if c.options.Cluster != "" {
			// Insert ON CLUSTER clause into the CREATE FUNCTION statement
			modifiedQuery := c.addOnClusterToFunction(createQuery)
			statements = append(statements, modifiedQuery)
		} else {
			statements = append(statements, createQuery)
		}
	}

	if len(statements) == 0 {
		return &parser.SQL{}, nil
	}

	// Parse the generated SQL
	sql := strings.Join(statements, "\n\n")
	parsedSQL, err := parser.ParseString(sql)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse generated function SQL")
	}

	return parsedSQL, nil
}

// addOnClusterToFunction injects ON CLUSTER clause into CREATE FUNCTION statement
func (c *Client) addOnClusterToFunction(createQuery string) string {
	if c.options.Cluster == "" {
		return createQuery
	}

	// Find the position to insert ON CLUSTER
	// Pattern: CREATE FUNCTION name ON CLUSTER cluster AS ...
	upperQuery := strings.ToUpper(createQuery)
	functionPos := strings.Index(upperQuery, "CREATE FUNCTION")
	if functionPos == -1 {
		return createQuery // Invalid query, return as-is
	}

	asPos := strings.Index(upperQuery, " AS ")
	if asPos == -1 {
		return createQuery // Invalid query, return as-is
	}

	// Check if ON CLUSTER already exists
	if strings.Contains(upperQuery[functionPos:asPos], "ON CLUSTER") {
		return createQuery // Already has ON CLUSTER
	}

	// Find the function name end position to insert ON CLUSTER
	// Look for the space before "AS"
	beforeAs := strings.LastIndex(createQuery[:asPos], " ")
	if beforeAs == -1 {
		return createQuery // Can't find insertion point
	}

	// Insert ON CLUSTER clause
	clusterClause := fmt.Sprintf(" ON CLUSTER `%s`", c.options.Cluster)
	result := createQuery[:beforeAs] + clusterClause + createQuery[beforeAs:]

	return result
}
