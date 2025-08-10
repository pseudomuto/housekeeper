package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// extractDictionaries retrieves all dictionary definitions from the ClickHouse instance.
// This function queries the system.dictionaries table to get complete dictionary information
// and returns them as parsed DDL statements.
//
// System dictionaries are automatically excluded. All DDL statements are validated
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
//	dictionaries, err := client.GetDictionaries(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract dictionaries: %v", err)
//	}
//
//	// Process the parsed dictionary statements
//	for _, stmt := range dictionaries.Statements {
//		if stmt.CreateDictionary != nil {
//			name := stmt.CreateDictionary.Name
//			if stmt.CreateDictionary.Database != nil {
//				name = *stmt.CreateDictionary.Database + "." + name
//			}
//			fmt.Printf("Dictionary: %s\n", name)
//		}
//	}
//
// Returns a *parser.SQL containing dictionary CREATE statements or an error if extraction fails.
func extractDictionaries(ctx context.Context, client *Client) (*parser.SQL, error) {
	// First, get a list of all dictionaries (excluding system ones)
	query := `
		SELECT 
			database, 
			name
		FROM system.dictionaries
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := client.conn.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query dictionaries")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var database, name string
		if err := rows.Scan(&database, &name); err != nil {
			return nil, errors.Wrap(err, "failed to scan dictionary row")
		}

		// Use SHOW CREATE DICTIONARY to get the DDL
		fullName := fmt.Sprintf("`%s`.`%s`", database, name)
		showQuery := "SHOW CREATE DICTIONARY " + fullName

		showRows, err := client.conn.Query(ctx, showQuery)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to show create dictionary %s", fullName)
		}

		if showRows.Next() {
			var createQuery string
			if err := showRows.Scan(&createQuery); err != nil {
				showRows.Close()
				return nil, errors.Wrapf(err, "failed to scan show create result for dictionary %s", fullName)
			}

			// Clean up the CREATE statement
			cleanedQuery := cleanCreateStatement(createQuery)

			// Validate the statement using our parser
			if err := validateDDLStatement(cleanedQuery); err != nil {
				showRows.Close()
				// Include the problematic query in the error for debugging
				return nil, errors.Wrapf(err, "generated invalid DDL for dictionary %s (query: %s)", fullName, cleanedQuery)
			}

			statements = append(statements, cleanedQuery)
		}
		showRows.Close()
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating dictionary rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseString(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined dictionary DDL")
	}

	return sqlResult, nil
}
