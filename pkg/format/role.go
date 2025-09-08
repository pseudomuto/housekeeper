package format

import (
	"fmt"
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// createRole formats a CREATE ROLE statement
func (f *Formatter) createRole(w io.Writer, stmt *parser.CreateRoleStmt) error {
	var parts []string

	// CREATE [OR REPLACE] ROLE [IF NOT EXISTS]
	if stmt.OrReplace {
		parts = append(parts, f.keyword("CREATE OR REPLACE ROLE"))
	} else {
		parts = append(parts, f.keyword("CREATE ROLE"))
		if stmt.IfNotExists {
			parts = append(parts, f.keyword("IF NOT EXISTS"))
		}
	}

	// Role name
	parts = append(parts, f.identifier(stmt.Name))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// SETTINGS
	if stmt.Settings != nil {
		parts = append(parts, f.formatRoleSettings(stmt.Settings))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// alterRole formats an ALTER ROLE statement
func (f *Formatter) alterRole(w io.Writer, stmt *parser.AlterRoleStmt) error {
	var parts []string

	// ALTER ROLE
	parts = append(parts, f.keyword("ALTER ROLE"))

	// IF EXISTS
	if stmt.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	// Role name
	parts = append(parts, f.identifier(stmt.Name))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// RENAME TO
	if stmt.RenameTo != nil {
		parts = append(parts, f.keyword("RENAME TO"), f.identifier(*stmt.RenameTo))
	}

	// SETTINGS
	if stmt.Settings != nil {
		parts = append(parts, f.formatRoleSettings(stmt.Settings))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// dropRole formats a DROP ROLE statement
func (f *Formatter) dropRole(w io.Writer, stmt *parser.DropRoleStmt) error {
	var parts []string

	// DROP ROLE
	parts = append(parts, f.keyword("DROP ROLE"))

	// IF EXISTS
	if stmt.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	// Role names
	names := make([]string, len(stmt.Names))
	for i, name := range stmt.Names {
		names[i] = f.identifier(name)
	}
	parts = append(parts, strings.Join(names, ", "))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// setRole formats a SET ROLE statement
func (f *Formatter) setRole(w io.Writer, stmt *parser.SetRoleStmt) error {
	var parts []string

	// SET ROLE
	parts = append(parts, f.keyword("SET ROLE"))

	// Role specification
	if stmt.Default {
		parts = append(parts, f.keyword("DEFAULT"))
	} else if stmt.None {
		parts = append(parts, f.keyword("NONE"))
	} else if stmt.All {
		parts = append(parts, f.keyword("ALL"))
	} else if stmt.AllExcept != nil {
		parts = append(parts, f.keyword("ALL EXCEPT"))
		parts = append(parts, f.formatRoleList(stmt.AllExcept))
	} else if stmt.Roles != nil {
		parts = append(parts, f.formatRoleList(stmt.Roles))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// setDefaultRole formats a SET DEFAULT ROLE statement
func (f *Formatter) setDefaultRole(w io.Writer, stmt *parser.SetDefaultRoleStmt) error {
	var parts []string

	// SET DEFAULT ROLE
	parts = append(parts, f.keyword("SET DEFAULT ROLE"))

	// Role specification
	if stmt.None {
		parts = append(parts, f.keyword("NONE"))
	} else if stmt.All {
		parts = append(parts, f.keyword("ALL"))
	} else if stmt.AllExcept != nil {
		parts = append(parts, f.keyword("ALL EXCEPT"))
		parts = append(parts, f.formatRoleList(stmt.AllExcept))
	} else if stmt.Roles != nil {
		parts = append(parts, f.formatRoleList(stmt.Roles))
	}

	// TO users
	parts = append(parts, f.keyword("TO"))
	users := make([]string, len(stmt.ToUsers))
	for i, user := range stmt.ToUsers {
		users[i] = f.identifier(user)
	}
	parts = append(parts, strings.Join(users, ", "))

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// grant formats a GRANT statement
func (f *Formatter) grant(w io.Writer, stmt *parser.GrantStmt) error {
	var parts []string

	// GRANT
	parts = append(parts, f.keyword("GRANT"))

	// Privileges
	parts = append(parts, f.formatPrivilegeList(stmt.Privileges))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// ON target
	if stmt.On != nil {
		parts = append(parts, f.keyword("ON"), f.formatGrantTarget(stmt.On))
	}

	// TO grantees
	parts = append(parts, f.keyword("TO"))
	parts = append(parts, f.formatGranteeList(stmt.To))

	// WITH options
	if stmt.WithGrant {
		parts = append(parts, f.keyword("WITH GRANT OPTION"))
	}
	if stmt.WithReplace {
		parts = append(parts, f.keyword("WITH REPLACE OPTION"))
	}
	if stmt.WithAdmin {
		parts = append(parts, f.keyword("WITH ADMIN OPTION"))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// revoke formats a REVOKE statement
func (f *Formatter) revoke(w io.Writer, stmt *parser.RevokeStmt) error {
	var parts []string

	// REVOKE
	parts = append(parts, f.keyword("REVOKE"))

	// Options
	if stmt.GrantOption {
		parts = append(parts, f.keyword("GRANT OPTION FOR"))
	}
	if stmt.AdminOption {
		parts = append(parts, f.keyword("ADMIN OPTION FOR"))
	}

	// Privileges
	parts = append(parts, f.formatPrivilegeList(stmt.Privileges))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// ON target
	if stmt.On != nil {
		parts = append(parts, f.keyword("ON"), f.formatGrantTarget(stmt.On))
	}

	// FROM grantees
	parts = append(parts, f.keyword("FROM"))
	parts = append(parts, f.formatGranteeList(stmt.From))

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// formatRoleSettings formats role settings
func (f *Formatter) formatRoleSettings(settings *parser.RoleSettings) string {
	if settings == nil || len(settings.Settings) == 0 {
		return ""
	}

	var parts []string
	parts = append(parts, f.keyword("SETTINGS"))

	settingStrs := make([]string, len(settings.Settings))
	for i, setting := range settings.Settings {
		if setting.Value != nil {
			settingStrs[i] = fmt.Sprintf("%s = %s", f.identifier(setting.Name), *setting.Value)
		} else {
			settingStrs[i] = f.identifier(setting.Name)
		}
	}
	parts = append(parts, strings.Join(settingStrs, ", "))

	return strings.Join(parts, " ")
}

// formatRoleList formats a list of role names
func (f *Formatter) formatRoleList(list *parser.RoleList) string {
	if list == nil || len(list.Names) == 0 {
		return ""
	}

	names := make([]string, len(list.Names))
	for i, name := range list.Names {
		names[i] = f.identifier(name)
	}
	return strings.Join(names, ", ")
}

// formatPrivilegeList formats a list of privileges
func (f *Formatter) formatPrivilegeList(list *parser.PrivilegeList) string {
	if list == nil || len(list.Items) == 0 {
		return ""
	}

	items := make([]string, len(list.Items))
	for i, item := range list.Items {
		if item.All {
			items[i] = f.keyword("ALL")
		} else if len(item.Columns) > 0 {
			cols := make([]string, len(item.Columns))
			for j, col := range item.Columns {
				cols[j] = f.identifier(col)
			}
			items[i] = fmt.Sprintf("%s(%s)", f.identifier(item.Name), strings.Join(cols, ", "))
		} else {
			items[i] = f.identifier(item.Name)
		}
	}
	return strings.Join(items, ", ")
}

// formatGrantTarget formats the target of a GRANT/REVOKE
func (f *Formatter) formatGrantTarget(target *parser.GrantTarget) string {
	if target == nil {
		return ""
	}

	// Check for *.*
	if target.Star1 != nil && target.Star2 != nil {
		return "*.*"
	}

	// Check for database.table
	if target.Database != nil && target.Table != nil {
		db := f.identifier(*target.Database)
		table := *target.Table
		if table != "*" {
			table = f.identifier(table)
		}
		return fmt.Sprintf("%s.%s", db, table)
	}

	return ""
}

// formatGranteeList formats a list of grantees
func (f *Formatter) formatGranteeList(list *parser.GranteeList) string {
	if list == nil || len(list.Items) == 0 {
		return ""
	}

	items := make([]string, len(list.Items))
	for i, item := range list.Items {
		if item.IsCurrent {
			items[i] = f.keyword("CURRENT_USER")
		} else {
			items[i] = f.identifier(item.Name)
		}
	}
	return strings.Join(items, ", ")
}
