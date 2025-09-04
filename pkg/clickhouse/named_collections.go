package clickhouse

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/pseudomuto/housekeeper/pkg/utils"
)

// extractNamedCollections retrieves all named collection definitions from the ClickHouse instance.
// This function queries the system.named_collections table to get collection information
// and returns them as parsed DDL statements.
//
// Named collections store connection parameters for external data sources in a centralized
// and secure way, avoiding credentials in DDL statements.
//
// Collections with names starting with 'builtin_' are automatically excluded from extraction
// as these are typically configuration-managed collections defined in ClickHouse XML/YAML config
// files rather than DDL-managed collections.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	collections, err := client.GetNamedCollections(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract named collections: %v", err)
//	}
//
//	// Process the parsed named collection statements
//	for _, stmt := range collections.Statements {
//		if stmt.CreateNamedCollection != nil {
//			name := stmt.CreateNamedCollection.Name
//			fmt.Printf("Named Collection: %s\n", name)
//		}
//	}
//
// Returns a *parser.SQL containing named collection CREATE statements or an error if extraction fails.
func extractNamedCollections(ctx context.Context, client *Client) (*parser.SQL, error) {
	// Note: system.named_collections table structure in ClickHouse:
	// - name: String - The name of the collection
	// - key: String - Parameter key
	// - value: String - Parameter value (may be masked for sensitive data)
	// - overridable: UInt8 - Whether the parameter can be overridden (1 = true, 0 = false)
	// - comment: String - Optional comment for the collection (may not exist in older versions)

	if !isNamedCollectionsSupported(ctx, client) {
		// Named collections not supported in this ClickHouse version
		return &parser.SQL{Statements: []*parser.Statement{}}, nil
	}

	query, supported := buildNamedCollectionsQuery(ctx, client)
	if !supported {
		// Unsupported table structure
		return &parser.SQL{Statements: []*parser.Statement{}}, nil
	}

	collections, err := queryNamedCollectionsData(ctx, client, query)
	if err != nil {
		return nil, err
	}

	addCommentsToCollections(ctx, client, collections)

	return generateNamedCollectionStatements(collections, client.options.Cluster)
}

// namedCollectionData holds the data for a single named collection
type namedCollectionData struct {
	name       string
	parameters map[string]parameterData
	comment    string
}

// parameterData holds the data for a single parameter
type parameterData struct {
	value       string
	overridable bool
}

// generateCreateNamedCollectionStatement generates a CREATE NAMED COLLECTION statement
func generateCreateNamedCollectionStatement(collection *namedCollectionData, cluster string) string {
	var parts []string

	// CREATE NAMED COLLECTION name
	parts = append(parts, "CREATE NAMED COLLECTION", utils.BacktickIdentifier(collection.name))

	// ON CLUSTER if specified
	if cluster != "" {
		parts = append(parts, "ON CLUSTER", utils.BacktickIdentifier(cluster))
	}

	parts = append(parts, "AS")

	// Build the main statement
	stmt := strings.Join(parts, " ")

	// Add parameters
	paramLines := make([]string, 0, len(collection.parameters))

	// Sort parameter names for deterministic output
	paramNames := make([]string, 0, len(collection.parameters))
	for key := range collection.parameters {
		paramNames = append(paramNames, key)
	}
	sort.Strings(paramNames)

	// Check if all parameters have the same overridable setting
	allOverridable, allNotOverridable := true, true
	for _, param := range collection.parameters {
		if param.overridable {
			allNotOverridable = false
		} else {
			allOverridable = false
		}
	}

	// If all parameters have the same overridable setting, we can use global override
	useGlobalOverride := allOverridable || allNotOverridable

	for _, key := range paramNames {
		param := collection.parameters[key]

		// Format the value - add quotes if it's a string value
		value := param.value
		if !utils.IsNumericValue(value) && !utils.IsBooleanValue(value) && value != "NULL" {
			value = fmt.Sprintf("'%s'", escapeString(value))
		}

		paramLine := fmt.Sprintf("    %s = %s", utils.BacktickIdentifier(key), value)

		// Add per-parameter override only if we're not using global override
		if !useGlobalOverride {
			if param.overridable {
				paramLine += " OVERRIDABLE"
			} else {
				paramLine += " NOT OVERRIDABLE"
			}
		}

		paramLines = append(paramLines, paramLine)
	}

	if len(paramLines) > 0 {
		stmt += "\n" + strings.Join(paramLines, ",\n")
	}

	// Add global override if applicable
	if useGlobalOverride && len(collection.parameters) > 0 {
		if allOverridable {
			stmt += "\nOVERRIDABLE"
		} else {
			stmt += "\nNOT OVERRIDABLE"
		}
	}

	// Add comment if present
	if collection.comment != "" {
		stmt += fmt.Sprintf("\nCOMMENT '%s'", escapeString(collection.comment))
	}

	stmt += ";"

	return stmt
}

// escapeString escapes single quotes in a string
func escapeString(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// GetNamedCollections retrieves all named collection definitions from the ClickHouse instance
func (c *Client) GetNamedCollections(ctx context.Context) (*parser.SQL, error) {
	return extractNamedCollections(ctx, c)
}

// isNamedCollectionsSupported checks if system.named_collections table exists
func isNamedCollectionsSupported(ctx context.Context, client *Client) bool {
	checkQuery := `
		SELECT count(*) 
		FROM system.tables 
		WHERE database = 'system' AND name = 'named_collections'
	`

	var tableExists uint64
	err := client.conn.QueryRow(ctx, checkQuery).Scan(&tableExists)
	return err == nil && tableExists > 0
}

// buildNamedCollectionsQuery builds the appropriate query based on ClickHouse version
func buildNamedCollectionsQuery(ctx context.Context, client *Client) (string, bool) {
	columnsQuery := `
		SELECT name
		FROM system.columns
		WHERE database = 'system' AND table = 'named_collections'
	`

	columnRows, err := client.conn.Query(ctx, columnsQuery)
	if err != nil {
		return "", false
	}
	defer columnRows.Close()

	availableColumns := make(map[string]bool)
	for columnRows.Next() {
		var columnName string
		if err := columnRows.Scan(&columnName); err != nil {
			return "", false
		}
		availableColumns[columnName] = true
	}

	if availableColumns["key"] && availableColumns["value"] {
		// Newer structure with key-value pairs
		return `
			SELECT 
				name,
				key,
				value,
				overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`, true
	} else if availableColumns["collection"] {
		// Older structure where collection is stored as Map(String, String)
		return `
			SELECT 
				name,
				arrayJoin(mapKeys(collection)) AS key,
				collection[key] AS value,
				1 AS overridable
			FROM system.named_collections
			WHERE name NOT LIKE 'builtin_%'
			ORDER BY name, key
		`, true
	}

	return "", false
}

// queryNamedCollectionsData executes the query and collects named collection data
func queryNamedCollectionsData(ctx context.Context, client *Client, query string) (map[string]*namedCollectionData, error) {
	rows, err := client.conn.Query(ctx, query)
	if err != nil {
		// Named collections might not be available in older ClickHouse versions
		if strings.Contains(err.Error(), "doesn't exist") {
			return make(map[string]*namedCollectionData), nil
		}
		return nil, errors.Wrap(err, "failed to query named collections")
	}
	defer rows.Close()

	collections := make(map[string]*namedCollectionData)

	for rows.Next() {
		var name, key, value string
		var overridable uint8

		if err := rows.Scan(&name, &key, &value, &overridable); err != nil {
			return nil, errors.Wrap(err, "failed to scan named collection row")
		}

		if _, exists := collections[name]; !exists {
			collections[name] = &namedCollectionData{
				name:       name,
				parameters: make(map[string]parameterData),
			}
		}

		collections[name].parameters[key] = parameterData{
			value:       value,
			overridable: overridable == 1,
		}
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating named collection rows")
	}

	return collections, nil
}

// addCommentsToCollections adds comments to collections if available
func addCommentsToCollections(ctx context.Context, client *Client, collections map[string]*namedCollectionData) {
	commentQuery := `
		SELECT DISTINCT
			name,
			comment
		FROM system.named_collections
		WHERE comment != '' AND name NOT LIKE 'builtin_%'
	`

	commentRows, err := client.conn.Query(ctx, commentQuery)
	if err == nil {
		defer commentRows.Close()
		for commentRows.Next() {
			var name, comment string
			if err := commentRows.Scan(&name, &comment); err == nil {
				if collection, exists := collections[name]; exists {
					collection.comment = comment
				}
			}
		}
	}
	// Ignore comment errors as the column might not exist
}

// generateNamedCollectionStatements generates and parses CREATE NAMED COLLECTION statements
func generateNamedCollectionStatements(collections map[string]*namedCollectionData, cluster string) (*parser.SQL, error) {
	statements := make([]string, 0, len(collections))

	// Sort collection names for deterministic output
	collectionNames := make([]string, 0, len(collections))
	for name := range collections {
		collectionNames = append(collectionNames, name)
	}
	sort.Strings(collectionNames)

	for _, name := range collectionNames {
		collection := collections[name]
		stmt := generateCreateNamedCollectionStatement(collection, cluster)

		// Validate the statement using our parser
		if err := validateDDLStatement(stmt); err != nil {
			return nil, errors.Wrapf(err, "generated invalid DDL for named collection %s", name)
		}

		statements = append(statements, stmt)
	}

	// Parse all statements
	fullSQL := strings.Join(statements, "\n\n")
	if fullSQL == "" {
		return &parser.SQL{Statements: []*parser.Statement{}}, nil
	}

	result, err := parser.ParseString(fullSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse generated named collection DDL")
	}

	return result, nil
}
