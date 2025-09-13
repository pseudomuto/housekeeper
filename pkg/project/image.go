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
	tables       []*parser.CreateTableStmt
	dictionaries []*parser.CreateDictionaryStmt
	views        []*parser.CreateViewStmt
}

// globalObjects holds the organized global statements (not tied to a specific database)
type globalObjects struct {
	roles       []*parser.CreateRoleStmt
	grants      []*parser.GrantStmt
	collections []*parser.CreateNamedCollectionStmt
}

// organizeStatementsByDatabase groups SQL statements by database and separates global objects
func organizeStatementsByDatabase(sql *parser.SQL) (map[string]*databaseObjects, *globalObjects) {
	dbObjects := make(map[string]*databaseObjects)
	global := &globalObjects{
		roles:       []*parser.CreateRoleStmt{},
		grants:      []*parser.GrantStmt{},
		collections: []*parser.CreateNamedCollectionStmt{},
	}

	ensureDB := func(name string) {
		if dbObjects[name] == nil {
			dbObjects[name] = &databaseObjects{
				tables:       []*parser.CreateTableStmt{},
				dictionaries: []*parser.CreateDictionaryStmt{},
				views:        []*parser.CreateViewStmt{},
			}
		}
	}

	// Organize statements by database or global scope
	for _, stmt := range sql.Statements {
		if stmt.CreateDatabase != nil { // nolint: nestif
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
		} else if stmt.CreateNamedCollection != nil {
			// Named collections are global objects
			global.collections = append(global.collections, stmt.CreateNamedCollection)
		} else if stmt.CreateRole != nil {
			// Roles are global objects
			global.roles = append(global.roles, stmt.CreateRole)
		} else if stmt.Grant != nil {
			// Grants are global objects
			global.grants = append(global.grants, stmt.Grant)
		}
	}

	return dbObjects, global
}

// groupGrantsByRole organizes grants by their target role
func groupGrantsByRole(grants []*parser.GrantStmt) map[string][]*parser.GrantStmt {
	grantsByRole := make(map[string][]*parser.GrantStmt)
	for _, grant := range grants {
		if grant.To != nil {
			for _, grantee := range grant.To.Items {
				if grantee.Name != "" {
					grantsByRole[grantee.Name] = append(grantsByRole[grantee.Name], grant)
				}
			}
		}
	}
	return grantsByRole
}

// createRoleNamesSet creates a set of role names for efficient lookup
func createRoleNamesSet(roles []*parser.CreateRoleStmt) map[string]bool {
	roleNames := make(map[string]bool)
	for _, role := range roles {
		roleNames[role.Name] = true
	}
	return roleNames
}

// hasOrphanGrants checks if there are any grants not assigned to defined roles
func hasOrphanGrants(grants []*parser.GrantStmt, roles []*parser.CreateRoleStmt) bool {
	if len(grants) == 0 {
		return false
	}

	roleNames := createRoleNamesSet(roles)

	for _, grant := range grants {
		if grant.To != nil {
			isOrphan := true
			for _, grantee := range grant.To.Items {
				if grantee.Name != "" && roleNames[grantee.Name] {
					isOrphan = false
					break
				}
			}
			if isOrphan {
				return true
			}
		} else {
			// Grant with no grantees is considered orphan
			return true
		}
	}
	return false
}

// findOrphanGrants identifies grants that are not assigned to any defined role
func findOrphanGrants(grants []*parser.GrantStmt, roles []*parser.CreateRoleStmt) []*parser.GrantStmt {
	if len(grants) == 0 {
		return nil
	}

	roleNames := createRoleNamesSet(roles)

	var orphanGrants []*parser.GrantStmt
	for _, grant := range grants {
		isOrphan := true
		if grant.To != nil {
			for _, grantee := range grant.To.Items {
				if grantee.Name != "" && roleNames[grantee.Name] {
					isOrphan = false
					break
				}
			}
		}
		if isOrphan {
			orphanGrants = append(orphanGrants, grant)
		}
	}
	return orphanGrants
}

// hasGlobalObjects checks if there are any global objects to generate
func hasGlobalObjects(global *globalObjects) bool {
	return len(global.roles) > 0 || len(global.grants) > 0 || len(global.collections) > 0
}

// getDatabase extracts the database name from a pointer, defaulting to "default"
func getDatabase(database *string) string {
	if database != nil && *database != "" {
		return *database
	}
	return "default"
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

// generateImage creates a file system image from parsed SQL statements,
// organizing them into a structured project layout suitable for overlaying
// on a directory structure.
//
// The generated fs.FS contains:
//   - db/main.sql: Main schema file with imports to all databases and global objects
//   - db/schemas/_global/schema.sql: Global objects schema file with imports
//   - db/schemas/_global/roles/<role>.sql: Individual role files with their grants
//   - db/schemas/_global/collections/<collection>.sql: Individual named collection files
//   - db/schemas/<database>/schema.sql: Database schema file with imports
//   - db/schemas/<database>/tables/<table>.sql: Individual table files
//   - db/schemas/<database>/dictionaries/<dict>.sql: Individual dictionary files
//   - db/schemas/<database>/views/<view>.sql: Individual view files
func (p *Project) generateImage(sql *parser.SQL) (fs.FS, error) {
	dbObjects, globalObjs := organizeStatementsByDatabase(sql)
	fsMap := make(fstest.MapFS)

	// Generate global objects first if any exist
	var mainImports []string
	if hasGlobalObjects(globalObjs) {
		if err := p.generateGlobalFiles(fsMap, globalObjs); err != nil {
			return nil, errors.Wrap(err, "failed to generate global files")
		}
		mainImports = append(mainImports, "schemas/_global/schema.sql")
	}

	// Generate database files
	dbImports, err := p.generateDatabaseFiles(fsMap, dbObjects)
	if err != nil {
		return nil, err
	}
	mainImports = append(mainImports, dbImports...)

	// Create main.sql with imports
	mainContent := generateMainSchemaContent(mainImports)
	fsMap[filepath.Join("db", "main.sql")] = &fstest.MapFile{
		Data: []byte(mainContent),
	}

	return fsMap, nil
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
	case *parser.CreateRoleStmt:
		statement = &parser.Statement{CreateRole: s}
	case *parser.GrantStmt:
		statement = &parser.Statement{Grant: s}
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

// addCollectionFiles adds collection files to the global directory
func (p *Project) addCollectionFiles(fsMap fstest.MapFS, collections []*parser.CreateNamedCollectionStmt) error {
	for _, collection := range collections {
		stmt, err := p.formatStatement(collection)
		if err != nil {
			return err
		}

		path := filepath.Join("db", "schemas", "_global", "collections", collection.Name+".sql")
		fsMap[path] = &fstest.MapFile{
			Data: []byte(stmt),
		}
	}
	return nil
}

// addSingleRoleFile creates a file for a single role with its associated grants
func (p *Project) addSingleRoleFile(
	fsMap fstest.MapFS,
	role *parser.CreateRoleStmt,
	roleGrants []*parser.GrantStmt,
) error {
	var content strings.Builder

	// Format the CREATE ROLE statement
	stmt, err := p.formatStatement(role)
	if err != nil {
		return err
	}
	content.WriteString(stmt)

	// Add related grants if any
	if len(roleGrants) > 0 {
		content.WriteString("\n")
		for _, grant := range roleGrants {
			grantStmt, err := p.formatStatement(grant)
			if err != nil {
				return err
			}
			content.WriteString(grantStmt)
		}
	}

	path := filepath.Join("db", "schemas", "_global", "roles", role.Name+".sql")
	fsMap[path] = &fstest.MapFile{
		Data: []byte(content.String()),
	}
	return nil
}

// addOrphanGrantsFile creates a file for grants not associated with any role
func (p *Project) addOrphanGrantsFile(fsMap fstest.MapFS, orphanGrants []*parser.GrantStmt) error {
	if len(orphanGrants) == 0 {
		return nil
	}

	var content strings.Builder
	for _, grant := range orphanGrants {
		stmt, err := p.formatStatement(grant)
		if err != nil {
			return err
		}
		content.WriteString(stmt)
	}

	path := filepath.Join("db", "schemas", "_global", "roles", "grants.sql")
	fsMap[path] = &fstest.MapFile{
		Data: []byte(content.String()),
	}
	return nil
}

// addRoleFiles adds role files to the global directory
func (p *Project) addRoleFiles(
	fsMap fstest.MapFS,
	roles []*parser.CreateRoleStmt,
	grants []*parser.GrantStmt,
) error {
	// Group grants by role for better organization
	grantsByRole := groupGrantsByRole(grants)

	// Create individual role files with their grants
	for _, role := range roles {
		roleGrants := grantsByRole[role.Name]
		if err := p.addSingleRoleFile(fsMap, role, roleGrants); err != nil {
			return err
		}
	}

	// Handle grants to users or other non-role entities
	orphanGrants := findOrphanGrants(grants, roles)
	if err := p.addOrphanGrantsFile(fsMap, orphanGrants); err != nil {
		return err
	}

	return nil
}

// addDictionaryFiles adds dictionary files to the file system map
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

// generateGlobalFiles creates files for global objects (_global directory)
func (p *Project) generateGlobalFiles(fsMap fstest.MapFS, global *globalObjects) error {
	// Generate global schema file
	schemaContent := p.generateGlobalSchemaContent(global)
	fsMap[filepath.Join("db", "schemas", "_global", "schema.sql")] = &fstest.MapFile{
		Data: []byte(schemaContent),
	}

	// Create role files
	if len(global.roles) > 0 || len(global.grants) > 0 {
		if err := p.addRoleFiles(fsMap, global.roles, global.grants); err != nil {
			return errors.Wrap(err, "failed to add role files")
		}
	}

	// Create collection files
	if len(global.collections) > 0 {
		if err := p.addCollectionFiles(fsMap, global.collections); err != nil {
			return errors.Wrap(err, "failed to add collection files")
		}
	}

	return nil
}

// generateGlobalSchemaContent creates the content for the global schema file
func (p *Project) generateGlobalSchemaContent(global *globalObjects) string {
	var content strings.Builder
	var imports []string

	content.WriteString("-- Global objects (roles, grants, named collections)\n")
	content.WriteString("-- These objects exist at the cluster level and are not tied to specific databases\n\n")

	// Add imports for roles
	if len(global.roles) > 0 {
		content.WriteString("-- Roles and Permissions\n")
		for _, role := range global.roles {
			importPath := fmt.Sprintf("roles/%s.sql", role.Name)
			imports = append(imports, importPath)
		}
	}

	// Check for orphan grants
	if hasOrphanGrants(global.grants, global.roles) {
		imports = append(imports, "roles/grants.sql")
	}

	// Add imports for named collections
	if len(global.collections) > 0 {
		content.WriteString("-- Named Collections\n")
		for _, collection := range global.collections {
			importPath := fmt.Sprintf("collections/%s.sql", collection.Name)
			imports = append(imports, importPath)
		}
	}

	// Add import directives
	for _, imp := range imports {
		content.WriteString(fmt.Sprintf("-- housekeeper:import %s\n", imp))
	}

	return content.String()
}
