package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
)

// systemUsers contains the list of ClickHouse system users that should be excluded
// from schema extraction operations. These users are managed by ClickHouse internally
// and should not be included in user schema operations.
var systemUsers = []string{
	"default",
}

// extractUsers retrieves all user definitions from the ClickHouse instance.
// This function queries the system.users table to get user information and uses
// SHOW CREATE USER to retrieve the complete DDL statements.
//
// System users (e.g., 'default') are automatically excluded. All DDL statements
// are validated using the parser before being returned.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	users, err := extractUsers(ctx, client)
//	if err != nil {
//		log.Fatalf("Failed to extract users: %v", err)
//	}
//
//	// Process the parsed user statements
//	for _, stmt := range users.Statements {
//		if stmt.CreateUser != nil {
//			fmt.Printf("User: %s\n", stmt.CreateUser.Name)
//		}
//	}
//
// Returns a *parser.SQL containing user CREATE statements or an error if extraction fails.
func extractUsers(ctx context.Context, client *Client) (*parser.SQL, error) {
	// First, get a list of all users (excluding system ones)
	query := buildUserQuery()

	rows, err := client.conn.Query(ctx, query, interfaceSlice(systemUsers)...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query users")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, errors.Wrap(err, "failed to scan user row")
		}

		// Use SHOW CREATE USER to get the DDL
		userName := utils.BacktickIdentifier(name)
		showQuery := "SHOW CREATE USER " + userName

		showRows, err := client.conn.Query(ctx, showQuery)
		if err != nil {
			// SHOW CREATE USER can fail for users with special authentication methods
			// Skip these with a warning to avoid failing the entire schema extraction
			fmt.Printf("Warning: Cannot show user %s - likely authentication configuration issue: %v\n", userName, err)
			continue
		}

		if showRows.Next() {
			var createQuery string
			if err := showRows.Scan(&createQuery); err != nil {
				showRows.Close()
				return nil, errors.Wrapf(err, "failed to scan show create result for user %s", userName)
			}

			// Clean up the CREATE statement
			cleanedQuery := cleanCreateStatement(createQuery)

			// Validate the statement using our parser
			if err := validateDDLStatement(cleanedQuery); err != nil {
				showRows.Close()
				// Include the problematic query in the error for debugging
				return nil, errors.Wrapf(err, "generated invalid DDL for user %s (query: %s)", userName, cleanedQuery)
			}

			statements = append(statements, cleanedQuery)
		} else {
			// User exists but SHOW CREATE returns no results
			fmt.Printf("Warning: User %s exists but SHOW CREATE returned no results - skipping\n", userName)
		}
		showRows.Close()
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating user rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseString(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined user DDL")
	}

	return sqlResult, nil
}

// buildUserQuery creates the SQL query for extracting users from system.users
func buildUserQuery() string {
	// Create placeholders for each system user
	placeholders := make([]string, len(systemUsers))
	for i := range systemUsers {
		placeholders[i] = "?"
	}

	return fmt.Sprintf(`
		SELECT name
		FROM system.users
		WHERE name NOT IN (%s)
		ORDER BY name
	`, strings.Join(placeholders, ", "))
}

// interfaceSlice converts a string slice to an interface slice for use with query parameters
func interfaceSlice(strings []string) []any {
	result := make([]any, len(strings))
	for i, s := range strings {
		result[i] = s
	}
	return result
}
