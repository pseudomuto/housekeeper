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
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// databaseObjects holds the organized statements for a database
type databaseObjects struct {
	database     *parser.CreateDatabaseStmt
	collections  []*parser.CreateNamedCollectionStmt
	tables       []*parser.CreateTableStmt
	dictionaries []*parser.CreateDictionaryStmt
	views        []*parser.CreateViewStmt
}

// generateImage creates a file system image from parsed SQL statements,
// organizing them into a structured project layout suitable for overlaying
// on a directory structure.
//
// The generated fs.FS contains:
//   - db/main.sql: Main schema file with imports to all databases
//   - db/schemas/<database>/schema.sql: Database schema file with imports
//   - db/schemas/<database>/collections/<collection>.sql: Individual named collection files
//   - db/schemas/<database>/tables/<table>.sql: Individual table files
//   - db/schemas/<database>/dictionaries/<dict>.sql: Individual dictionary files
//   - db/schemas/<database>/views/<view>.sql: Individual view files
func (p *Project) generateImage(sql *parser.SQL) (fs.FS, error) {
	dbObjects := organizeStatementsByDatabase(sql)
	fsMap := make(fstest.MapFS)

	mainImports, err := p.generateDatabaseFiles(fsMap, dbObjects)
	if err != nil {
		return nil, err
	}

	// Create main.sql with imports
	mainContent := generateMainSchemaContent(mainImports)
	fsMap[filepath.Join("db", "main.sql")] = &fstest.MapFile{
		Data: []byte(mainContent),
	}

	return fsMap, nil
}

// organizeStatementsByDatabase groups SQL statements by database
func organizeStatementsByDatabase(sql *parser.SQL) map[string]*databaseObjects {
	dbObjects := make(map[string]*databaseObjects)

	ensureDB := func(name string) {
		if dbObjects[name] == nil {
			dbObjects[name] = &databaseObjects{
				collections:  []*parser.CreateNamedCollectionStmt{},
				tables:       []*parser.CreateTableStmt{},
				dictionaries: []*parser.CreateDictionaryStmt{},
				views:        []*parser.CreateViewStmt{},
			}
		}
	}

	// Organize statements by database
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
		} else if stmt.CreateNamedCollection != nil {
			// Named collections are global but we'll put them in the default database for organization
			dbName := "default"
			ensureDB(dbName)
			dbObjects[dbName].collections = append(dbObjects[dbName].collections, stmt.CreateNamedCollection)
		} else if stmt.CreateView != nil {
			dbName := getDatabase(stmt.CreateView.Database)
			ensureDB(dbName)
			dbObjects[dbName].views = append(dbObjects[dbName].views, stmt.CreateView)
		}
	}

	return dbObjects
}

// generateDatabaseFiles creates files for each database and returns import list
func (p *Project) generateDatabaseFiles(fsMap fstest.MapFS, dbObjects map[string]*databaseObjects) ([]string, error) {
	mainImports := make([]string, 0, len(dbObjects))

	// Get sorted database names for consistent ordering
	dbNames := make([]string, 0, len(dbObjects))
	for dbName := range dbObjects {
		dbNames = append(dbNames, dbName)
	}
	sort.Strings(dbNames)

	for _, dbName := range dbNames {
		objects := dbObjects[dbName]

		// Create database schema file
		schemaContent, err := p.generateDatabaseSchemaContent(objects)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to generate database schema for %s", dbName)
		}
		fsMap[filepath.Join("db", "schemas", dbName, "schema.sql")] = &fstest.MapFile{
			Data: []byte(schemaContent),
		}

		// Create individual object files
		if err := p.addCollectionFiles(fsMap, dbName, objects.collections); err != nil {
			return nil, errors.Wrapf(err, "failed to add collection files for %s", dbName)
		}
		if err := p.addTableFiles(fsMap, dbName, objects.tables); err != nil {
			return nil, errors.Wrapf(err, "failed to add table files for %s", dbName)
		}
		if err := p.addDictionaryFiles(fsMap, dbName, objects.dictionaries); err != nil {
			return nil, errors.Wrapf(err, "failed to add dictionary files for %s", dbName)
		}
		if err := p.addViewFiles(fsMap, dbName, objects.views); err != nil {
			return nil, errors.Wrapf(err, "failed to add view files for %s", dbName)
		}

		mainImports = append(mainImports, fmt.Sprintf("schemas/%s/schema.sql", dbName))
	}

	return mainImports, nil
}

// getDatabase extracts the database name from a pointer, defaulting to "default"
func getDatabase(database *string) string {
	if database != nil && *database != "" {
		return *database
	}
	return "default"
}

// formatStatement formats a DDL statement into SQL string
func (p *Project) formatStatement(stmt any) (string, error) {
	var statement *parser.Statement
	switch s := stmt.(type) {
	case *parser.CreateDatabaseStmt:
		statement = &parser.Statement{CreateDatabase: s}
	case *parser.CreateNamedCollectionStmt:
		statement = &parser.Statement{CreateNamedCollection: s}
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
	if err := p.fmtr.Format(&buf, statement); err != nil {
		return "", err
	}

	return buf.String(), nil
}

// generateDatabaseSchemaContent creates the content for a database schema file
func (p *Project) generateDatabaseSchemaContent(objects *databaseObjects) (string, error) {
	var content strings.Builder
	var imports []string

	// Add database creation if present
	if objects.database != nil {
		stmt, err := p.formatStatement(objects.database)
		if err != nil {
			return "", err
		}
		content.WriteString(stmt)
		content.WriteString("\n\n")
	}

	// Add imports for named collections
	if len(objects.collections) > 0 {
		content.WriteString("-- Named Collections\n")
		for _, collection := range objects.collections {
			importPath := fmt.Sprintf("collections/%s.sql", collection.Name)
			imports = append(imports, importPath)
		}
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
func (p *Project) addTableFiles(fsMap fstest.MapFS, dbName string, tables []*parser.CreateTableStmt) error {
	for _, table := range tables {
		stmt, err := p.formatStatement(table)
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
func (p *Project) addCollectionFiles(fsMap fstest.MapFS, dbName string, collections []*parser.CreateNamedCollectionStmt) error {
	for _, collection := range collections {
		stmt, err := p.formatStatement(collection)
		if err != nil {
			return err
		}

		path := filepath.Join("db", "schemas", dbName, "collections", collection.Name+".sql")
		fsMap[path] = &fstest.MapFile{
			Data: []byte(stmt),
		}
	}
	return nil
}

func (p *Project) addDictionaryFiles(fsMap fstest.MapFS, dbName string, dictionaries []*parser.CreateDictionaryStmt) error {
	for _, dict := range dictionaries {
		stmt, err := p.formatStatement(dict)
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
func (p *Project) addViewFiles(fsMap fstest.MapFS, dbName string, views []*parser.CreateViewStmt) error {
	for _, view := range views {
		stmt, err := p.formatStatement(view)
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
