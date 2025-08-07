package project

import (
	"bytes"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strings"
	"testing/fstest"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// databaseObjects holds the organized statements for a database
type databaseObjects struct {
	database     *parser.CreateDatabaseStmt
	tables       []*parser.CreateTableStmt
	dictionaries []*parser.CreateDictionaryStmt
	views        []*parser.CreateViewStmt
}

// GenerateImage creates a file system image from parsed SQL statements,
// organizing them into a structured project layout suitable for overlaying
// on a directory structure.
//
// The generated fs.FS contains:
//   - db/main.sql: Main schema file with imports to all databases
//   - db/schemas/<database>/schema.sql: Database schema file with imports
//   - db/schemas/<database>/tables/<table>.sql: Individual table files
//   - db/schemas/<database>/dictionaries/<dict>.sql: Individual dictionary files
//   - db/schemas/<database>/views/<view>.sql: Individual view files
//
// Example usage:
//
//	grammar, _ := parser.ParseSQL("CREATE DATABASE analytics; CREATE TABLE analytics.events (...);")
//	fsImage, err := project.GenerateImage(grammar)
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	// The fsImage can be overlayed on a directory or used for testing
//	file, _ := fsImage.Open("db/main.sql")
//	content, _ := io.ReadAll(file)
//	fmt.Println(string(content))
func GenerateImage(sql *parser.SQL) (fs.FS, error) {
	// Track databases and their objects for organizing files
	dbObjects := make(map[string]*databaseObjects)
	mainImports := []string{}

	ensureDB := func(name string) {
		if dbObjects[name] == nil {
			dbObjects[name] = &databaseObjects{
				tables:       []*parser.CreateTableStmt{},
				dictionaries: []*parser.CreateDictionaryStmt{},
				views:        []*parser.CreateViewStmt{},
			}
		}
	}

	// First pass: organize statements by database
	for _, stmt := range sql.Statements {
		if stmt.CreateDatabase != nil {
			dbName := stmt.CreateDatabase.Name
			ensureDB(dbName)
			dbObjects[dbName].database = stmt.CreateDatabase
		} else if stmt.CreateTable != nil {
			dbName := getDatabase(stmt.CreateTable.Database)
			ensureDB(dbName)
			dbObjects[dbName].tables = append(dbObjects[dbName].tables, stmt.CreateTable)
		} else if stmt.CreateDictionary != nil {
			dbName := getDatabase(stmt.CreateDictionary.Database)
			ensureDB(dbName)
			dbObjects[dbName].dictionaries = append(dbObjects[dbName].dictionaries, stmt.CreateDictionary)
		} else if stmt.CreateView != nil {
			dbName := getDatabase(stmt.CreateView.Database)
			ensureDB(dbName)
			dbObjects[dbName].views = append(dbObjects[dbName].views, stmt.CreateView)
		}
	}

	// Create the file system map
	fsMap := make(fstest.MapFS)

	// Second pass: create files for each database
	dbNames := make([]string, 0, len(dbObjects))
	for dbName := range dbObjects {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	for _, dbName := range dbNames {
		objects := dbObjects[dbName]

		// Create database schema file
		schemaContent, err := generateDatabaseSchemaContent(objects)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to generate database schema for %s", dbName)
		}
		fsMap[filepath.Join("db", "schemas", dbName, "schema.sql")] = &fstest.MapFile{
			Data: []byte(schemaContent),
		}

		// Create individual object files
		if err := addTableFiles(fsMap, dbName, objects.tables); err != nil {
			return nil, errors.Wrapf(err, "failed to add table files for %s", dbName)
		}
		if err := addDictionaryFiles(fsMap, dbName, objects.dictionaries); err != nil {
			return nil, errors.Wrapf(err, "failed to add dictionary files for %s", dbName)
		}
		if err := addViewFiles(fsMap, dbName, objects.views); err != nil {
			return nil, errors.Wrapf(err, "failed to add view files for %s", dbName)
		}

		// Add import to main.sql
		relativeImport := fmt.Sprintf("schemas/%s/schema.sql", dbName)
		mainImports = append(mainImports, relativeImport)
	}

	// Create main.sql with imports
	mainContent := generateMainSchemaContent(mainImports)
	fsMap[filepath.Join("db", "main.sql")] = &fstest.MapFile{
		Data: []byte(mainContent),
	}

	return fsMap, nil
}

// getDatabase extracts the database name from a pointer, defaulting to "default"
func getDatabase(database *string) string {
	if database != nil && *database != "" {
		return *database
	}
	return "default"
}

// formatStatement formats a DDL statement into SQL string
func formatStatement(stmt any) (string, error) {
	var statement *parser.Statement
	switch s := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		statement = &parser.Statement{CreateDatabase: s}
	case *parser.CreateTableStmt:
		statement = &parser.Statement{CreateTable: s}
	case *parser.CreateDictionaryStmt:
		statement = &parser.Statement{CreateDictionary: s}
	case *parser.CreateViewStmt:
		statement = &parser.Statement{CreateView: s}
	default:
		return "", errors.New("unsupported statement type")
	}

	var buf bytes.Buffer
	if err := format.Format(&buf, format.Defaults, statement); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// generateDatabaseSchemaContent creates the content for a database schema file
func generateDatabaseSchemaContent(objects *databaseObjects) (string, error) {
	var content strings.Builder
	var imports []string

	// Add database creation if present
	if objects.database != nil {
		stmt, err := formatStatement(objects.database)
		if err != nil {
			return "", err
		}
		content.WriteString(stmt)
		content.WriteString("\n\n")
	}

	// Add imports for tables
	if len(objects.tables) > 0 {
		content.WriteString("-- Tables\n")
		for _, table := range objects.tables {
			importPath := fmt.Sprintf("tables/%s.sql", table.Name)
			imports = append(imports, importPath)
		}
	}

	// Add imports for dictionaries
	if len(objects.dictionaries) > 0 {
		content.WriteString("-- Dictionaries\n")
		for _, dict := range objects.dictionaries {
			importPath := fmt.Sprintf("dictionaries/%s.sql", dict.Name)
			imports = append(imports, importPath)
		}
	}

	// Add imports for views
	if len(objects.views) > 0 {
		content.WriteString("-- Views\n")
		for _, view := range objects.views {
			importPath := fmt.Sprintf("views/%s.sql", view.Name)
			imports = append(imports, importPath)
		}
	}

	// Add import directives
	for _, imp := range imports {
		content.WriteString(fmt.Sprintf("-- housekeeper:import %s\n", imp))
	}

	return content.String(), nil
}

// addTableFiles adds table files to the file system map
func addTableFiles(fsMap fstest.MapFS, dbName string, tables []*parser.CreateTableStmt) error {
	for _, table := range tables {
		stmt, err := formatStatement(table)
		if err != nil {
			return err
		}

		path := filepath.Join("db", "schemas", dbName, "tables", table.Name+".sql")
		fsMap[path] = &fstest.MapFile{
			Data: []byte(stmt),
		}
	}
	return nil
}

// addDictionaryFiles adds dictionary files to the file system map
func addDictionaryFiles(fsMap fstest.MapFS, dbName string, dictionaries []*parser.CreateDictionaryStmt) error {
	for _, dict := range dictionaries {
		stmt, err := formatStatement(dict)
		if err != nil {
			return err
		}

		path := filepath.Join("db", "schemas", dbName, "dictionaries", dict.Name+".sql")
		fsMap[path] = &fstest.MapFile{
			Data: []byte(stmt),
		}
	}
	return nil
}

// addViewFiles adds view files to the file system map
func addViewFiles(fsMap fstest.MapFS, dbName string, views []*parser.CreateViewStmt) error {
	for _, view := range views {
		stmt, err := formatStatement(view)
		if err != nil {
			return err
		}

		path := filepath.Join("db", "schemas", dbName, "views", view.Name+".sql")
		fsMap[path] = &fstest.MapFile{
			Data: []byte(stmt),
		}
	}
	return nil
}

// generateMainSchemaContent creates the content for the main schema file
func generateMainSchemaContent(imports []string) string {
	var content strings.Builder

	content.WriteString("-- Main schema file generated from ClickHouse bootstrap\n")
	content.WriteString("-- This file imports all database schemas extracted from your ClickHouse instance\n\n")

	for _, imp := range imports {
		content.WriteString(fmt.Sprintf("-- housekeeper:import %s\n", imp))
	}

	return content.String()
}
