package clickhouse

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

type (
	// Client represents a ClickHouse database connection
	Client struct {
		conn driver.Conn
	}

	// DatabaseInfo holds information about a database needed to recreate it
	DatabaseInfo struct {
		Name      string
		Engine    string
		Comment   string
		OnCluster string
	}
)

// NewClient creates a new ClickHouse client connection.
// The DSN should be in the format "host:port" (e.g., "localhost:9000").
//
// Example:
//
//	client, err := clickhouse.NewClient("localhost:9000")
//	if err != nil {
//	    log.Fatal(err)
//	}
//	defer client.Close()
//	
//	// Get current schema (databases and dictionaries)
//	grammar, err := client.GetSchema(context.Background())
//	if err != nil {
//	    log.Fatal(err)
//	}
//	
//	// Print all parsed statements
//	for _, stmt := range grammar.Statements {
//	    if stmt.CreateDatabase != nil {
//	        fmt.Printf("Database: %s\n", stmt.CreateDatabase.Name)
//	    }
//	    if stmt.CreateDictionary != nil {
//	        name := stmt.CreateDictionary.Name
//	        if stmt.CreateDictionary.Database != nil {
//	            name = *stmt.CreateDictionary.Database + "." + name
//	        }
//	        fmt.Printf("Dictionary: %s\n", name)
//	    }
//	}
func NewClient(dsn string) (*Client, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{dsn},
	})
	if err != nil {
		return nil, err
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}

	return &Client{conn: conn}, nil
}

// Close closes the ClickHouse connection
func (c *Client) Close() error {
	return c.conn.Close()
}


func (c *Client) listDatabases(ctx context.Context) ([]string, error) {
	rows, err := c.conn.Query(ctx, "SHOW DATABASES")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var databases []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		databases = append(databases, name)
	}

	return databases, nil
}

// ExecuteMigration executes a migration SQL script against the database.
// The SQL can contain multiple statements separated by semicolons.
func (c *Client) ExecuteMigration(ctx context.Context, sql string) error {
	return c.conn.Exec(ctx, sql)
}

// GetSchemaRecreationStatements returns all DDL statements necessary to recreate the current database schema.
// This focuses on database operations only (CREATE DATABASE statements).
// Returns statements that can be executed to recreate the database portion of the schema from scratch.
func (c *Client) GetSchemaRecreationStatements(ctx context.Context) ([]string, error) {
	var statements []string

	databases, err := c.listDatabases(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list databases: %w", err)
	}

	for _, dbName := range databases {
		// Skip system databases
		if dbName == "system" || dbName == "information_schema" || dbName == "INFORMATION_SCHEMA" {
			continue
		}

		// Get database details
		dbInfo, err := c.getDatabaseInfo(ctx, dbName)
		if err != nil {
			return nil, fmt.Errorf("failed to get database info for %s: %w", dbName, err)
		}

		// Generate CREATE DATABASE statement
		createStmt := c.generateCreateDatabaseStatement(dbInfo)

		// Validate the generated statement using our parser
		if err := c.validateDDLStatement(createStmt); err != nil {
			return nil, fmt.Errorf("generated invalid DDL for database %s: %w", dbName, err)
		}

		statements = append(statements, createStmt)
	}

	return statements, nil
}

// getDatabaseInfo retrieves database metadata from ClickHouse system tables
func (c *Client) getDatabaseInfo(ctx context.Context, dbName string) (*DatabaseInfo, error) {
	query := `
		SELECT 
			name,
			engine,
			comment,
			CASE 
				WHEN create_table_query LIKE '%ON CLUSTER%' 
				THEN extractBetween(create_table_query, 'ON CLUSTER ', ' ')
				ELSE ''
			END as cluster
		FROM system.databases 
		WHERE name = ?
	`

	var info DatabaseInfo
	var comment sql.NullString

	err := c.conn.QueryRow(ctx, query, dbName).Scan(
		&info.Name,
		&info.Engine,
		&comment,
		&info.OnCluster,
	)
	if err != nil {
		return nil, err
	}

	if comment.Valid {
		info.Comment = comment.String
	}

	return &info, nil
}

// generateCreateDatabaseStatement creates a CREATE DATABASE DDL statement from database info
func (c *Client) generateCreateDatabaseStatement(info *DatabaseInfo) string {
	var parts []string

	parts = append(parts, "CREATE DATABASE", info.Name)

	if info.OnCluster != "" {
		parts = append(parts, "ON CLUSTER", info.OnCluster)
	}

	if info.Engine != "" && info.Engine != "Atomic" {
		// Only specify engine if it's not the default Atomic engine
		parts = append(parts, "ENGINE =", info.Engine)
	}

	if info.Comment != "" {
		parts = append(parts, "COMMENT", fmt.Sprintf("'%s'", strings.ReplaceAll(info.Comment, "'", "\\'")))
	}

	return strings.Join(parts, " ") + ";"
}

// validateDDLStatement ensures the generated DDL statement is valid by parsing it
func (c *Client) validateDDLStatement(ddl string) error {
	_, err := parser.ParseSQL(ddl)
	return err
}

// GetSchema returns complete schema information including databases and dictionaries.
// This method retrieves both database and dictionary DDL statements from ClickHouse.
func (c *Client) GetSchema(ctx context.Context) (*parser.Grammar, error) {
	var statements []string

	// Get database statements
	dbStatements, err := c.GetSchemaRecreationStatements(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get database statements: %w", err)
	}
	statements = append(statements, dbStatements...)

	// Get dictionary statements
	dictStatements, err := c.getDictionaryRecreationStatements(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get dictionary statements: %w", err)
	}
	statements = append(statements, dictStatements...)

	// Combine all statements into a single SQL string
	combinedSQL := strings.Join(statements, "\n")

	// Parse the combined SQL using our parser
	grammar, err := parser.ParseSQL(combinedSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated DDL: %w", err)
	}

	return grammar, nil
}

// GetDatabasesOnly returns database-only schema information using the parser approach.
// This method focuses solely on database operations and returns a Grammar with parsed database statements.
func (c *Client) GetDatabasesOnly(ctx context.Context) (*parser.Grammar, error) {
	statements, err := c.GetSchemaRecreationStatements(ctx)
	if err != nil {
		return nil, err
	}

	// Combine all statements into a single SQL string
	combinedSQL := strings.Join(statements, "\n")

	// Parse the combined SQL using our parser
	grammar, err := parser.ParseSQL(combinedSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse generated DDL: %w", err)
	}

	return grammar, nil
}

// getDictionaryRecreationStatements returns CREATE DICTIONARY statements to recreate all dictionaries
func (c *Client) getDictionaryRecreationStatements(ctx context.Context) ([]string, error) {
	var statements []string

	// Query system.dictionaries to get all dictionary information
	query := `
		SELECT 
			database,
			name,
			create_table_query
		FROM system.dictionaries
		WHERE database NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA')
		ORDER BY database, name
	`

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query dictionaries: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var database, name, createQuery string
		if err := rows.Scan(&database, &name, &createQuery); err != nil {
			return nil, fmt.Errorf("failed to scan dictionary row: %w", err)
		}

		// Clean up the CREATE statement - remove IF NOT EXISTS and ensure it ends with semicolon
		cleanedQuery := strings.TrimSpace(createQuery)
		if !strings.HasSuffix(cleanedQuery, ";") {
			cleanedQuery += ";"
		}

		// Validate the generated statement using our parser
		if err := c.validateDDLStatement(cleanedQuery); err != nil {
			return nil, fmt.Errorf("generated invalid DDL for dictionary %s.%s: %w", database, name, err)
		}

		statements = append(statements, cleanedQuery)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating dictionary rows: %w", err)
	}

	return statements, nil
}
