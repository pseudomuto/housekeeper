package format

import (
	"io"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// removeQuotes removes surrounding single quotes from a string.
// The parser captures string literals with their quotes (e.g., 'special-user'),
// but we need to strip them before backticking to avoid `'special-user'`.
func removeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}
	return s
}

// createUser formats a CREATE USER statement
func (f *Formatter) createUser(w io.Writer, stmt *parser.CreateUserStmt) error {
	var parts []string

	// CREATE USER
	parts = append(parts, f.keyword("CREATE USER"))

	// IF NOT EXISTS | OR REPLACE
	if stmt.Modifier != nil {
		if stmt.Modifier.IfNotExists {
			parts = append(parts, f.keyword("IF NOT EXISTS"))
		} else if stmt.Modifier.OrReplace {
			parts = append(parts, f.keyword("OR REPLACE"))
		}
	}

	// User name
	parts = append(parts, f.identifier(stmt.Name))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	// IDENTIFICATION
	if stmt.Identification != nil {
		identPart := f.formatUserIdentification(stmt.Identification)
		if identPart != "" {
			parts = append(parts, identPart)
		}
	}

	// HOST
	if stmt.Host != nil {
		hostPart := f.formatUserHost(stmt.Host)
		if hostPart != "" {
			parts = append(parts, hostPart)
		}
	}

	// VALID UNTIL
	if stmt.ValidUntil != nil {
		parts = append(parts, f.keyword("VALID UNTIL"), *stmt.ValidUntil)
	}

	// IN (access storage type)
	if stmt.AccessStorageType != nil {
		parts = append(parts, f.keyword("IN"), *stmt.AccessStorageType)
	}

	// DEFAULT ROLE
	if stmt.Roles != nil {
		rolesPart := f.formatRoles(stmt.Roles)
		if rolesPart != "" {
			parts = append(parts, rolesPart)
		}
	}

	// DEFAULT DATABASE
	if stmt.DefaultDatabase != nil {
		dbPart := f.formatDefaultDatabase(stmt.DefaultDatabase)
		if dbPart != "" {
			parts = append(parts, dbPart)
		}
	}

	// GRANTEES
	if stmt.Grantees != nil {
		granteesPart := f.formatGrantees(stmt.Grantees)
		if granteesPart != "" {
			parts = append(parts, granteesPart)
		}
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}

// formatUserIdentification formats the identification clause
func (f *Formatter) formatUserIdentification(id *parser.UserIdentification) string {
	if id == nil {
		return ""
	}

	if id.NotIdentified {
		return f.keyword("NOT IDENTIFIED")
	}

	if id.IdentifiedWithHttp != nil {
		return f.keyword("IDENTIFIED WITH") + " http " + f.keyword("SERVER") + " " + *id.IdentifiedWithHttp
	}

	if id.IdentifiedWithLdap != nil {
		return f.keyword("IDENTIFIED WITH") + " ldap " + f.keyword("SERVER") + " " + *id.IdentifiedWithLdap
	}

	if id.IdentifiedWithKerberos != nil {
		parts := []string{f.keyword("IDENTIFIED WITH") + " kerberos"}
		if id.IdentifiedWithKerberos.Realm != nil {
			parts = append(parts, f.keyword("REALM"), *id.IdentifiedWithKerberos.Realm)
		}
		return strings.Join(parts, " ")
	}

	if id.IdentifiedWithSslCertificate != nil {
		return f.keyword("IDENTIFIED WITH") + " ssl_certificate " + f.keyword("CN") + " " + *id.IdentifiedWithSslCertificate
	}

	if id.IdentifiedWithSha256Password != nil {
		parts := []string{f.keyword("IDENTIFIED WITH") + " sha256_password"}
		if id.IdentifiedWithSha256Password.By != nil {
			parts = append(parts, f.keyword("BY"), *id.IdentifiedWithSha256Password.By)
		}
		return strings.Join(parts, " ")
	}

	if id.IdentifiedWithOther != nil {
		parts := []string{f.keyword("IDENTIFIED")}

		if id.IdentifiedWithOther.With != nil {
			parts = append(parts, f.keyword("WITH"), *id.IdentifiedWithOther.With)
		}

		parts = append(parts, f.keyword("BY"), id.IdentifiedWithOther.By)

		if id.IdentifiedWithOther.Salt != nil {
			parts = append(parts, f.keyword("SALT"), *id.IdentifiedWithOther.Salt)
		}

		if id.IdentifiedWithOther.ValidUntil != nil {
			parts = append(parts, f.keyword("VALID UNTIL"), *id.IdentifiedWithOther.ValidUntil)
		}

		return strings.Join(parts, " ")
	}

	return ""
}

// formatUserHost formats the host restriction clause
func (f *Formatter) formatUserHost(host *parser.UserHost) string {
	if host == nil {
		return ""
	}

	if host.IP != nil {
		return f.keyword("HOST IP") + " " + *host.IP
	}

	if host.Any {
		return f.keyword("HOST ANY")
	}

	if host.Local {
		return f.keyword("HOST LOCAL")
	}

	if host.Name != nil {
		return f.keyword("HOST NAME") + " " + *host.Name
	}

	if host.Regexp != nil {
		return f.keyword("HOST REGEXP") + " " + *host.Regexp
	}

	if host.Like != nil {
		return f.keyword("HOST LIKE") + " " + *host.Like
	}

	return ""
}

// formatRoles formats the DEFAULT ROLE clause
func (f *Formatter) formatRoles(roles *parser.Roles) string {
	if roles == nil || len(roles.Roles) == 0 {
		return ""
	}

	parts := []string{f.keyword("DEFAULT ROLE")}

	// Format the role list
	roleNames := make([]string, len(roles.Roles))
	for i, role := range roles.Roles {
		cleaned := removeQuotes(role)
		roleNames[i] = f.identifier(cleaned)
	}
	parts = append(parts, strings.Join(roleNames, ", "))

	// Add EXCEPT clause if present
	if len(roles.Except) > 0 {
		exceptNames := make([]string, len(roles.Except))
		for i, role := range roles.Except {
			cleaned := removeQuotes(role)
			exceptNames[i] = f.identifier(cleaned)
		}
		parts = append(parts, f.keyword("EXCEPT"), strings.Join(exceptNames, ", "))
	}

	return strings.Join(parts, " ")
}

// formatDefaultDatabase formats the DEFAULT DATABASE clause
func (f *Formatter) formatDefaultDatabase(db *parser.DefaultDatabase) string {
	if db == nil {
		return ""
	}

	if db.None {
		return f.keyword("DEFAULT DATABASE NONE")
	}

	if db.Database != nil {
		cleaned := removeQuotes(*db.Database)

		// Check if it's NONE keyword (parser may store this as a string)
		if strings.ToUpper(cleaned) == "NONE" {
			return f.keyword("DEFAULT DATABASE NONE")
		}

		return f.keyword("DEFAULT DATABASE") + " " + f.identifier(cleaned)
	}

	return ""
}

// formatGrantees formats the GRANTEES clause
func (f *Formatter) formatGrantees(grantees *parser.UserGrantees) string {
	if grantees == nil || len(grantees.Items) == 0 {
		return ""
	}

	parts := []string{f.keyword("GRANTEES")}

	// Format grantee items
	items := make([]string, len(grantees.Items))
	for i, item := range grantees.Items {
		items[i] = f.formatGranteeItem(&item)
	}
	parts = append(parts, strings.Join(items, ", "))

	// Add EXCEPT clause if present
	if grantees.Except != nil && len(grantees.Except.Items) > 0 {
		exceptItems := make([]string, len(grantees.Except.Items))
		for i, item := range grantees.Except.Items {
			exceptItems[i] = f.formatGranteeItem(&item)
		}
		parts = append(parts, f.keyword("EXCEPT"), strings.Join(exceptItems, ", "))
	}

	return strings.Join(parts, " ")
}

// formatGranteeItem formats a single grantee item
func (f *Formatter) formatGranteeItem(item *parser.GranteeItem) string {
	if item == nil {
		return ""
	}

	if item.Any {
		return f.keyword("ANY")
	}

	if item.None {
		return f.keyword("NONE")
	}

	if item.UserOrRole != nil {
		cleaned := removeQuotes(*item.UserOrRole)

		// Check if it's ANY or NONE keywords (parser may store these as strings)
		upper := strings.ToUpper(cleaned)
		if upper == "ANY" || upper == "NONE" {
			return f.keyword(upper)
		}

		return f.identifier(cleaned)
	}

	return ""
}

// dropUser formats a DROP USER statement
func (f *Formatter) dropUser(w io.Writer, stmt *parser.DropUserStmt) error {
	var parts []string

	// DROP USER
	parts = append(parts, f.keyword("DROP USER"))

	// IF EXISTS
	if stmt.IfExists {
		parts = append(parts, f.keyword("IF EXISTS"))
	}

	// User name
	parts = append(parts, f.identifier(stmt.Name))

	// ON CLUSTER
	if stmt.OnCluster != nil {
		parts = append(parts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	_, err := w.Write([]byte(strings.Join(parts, " ") + ";"))
	return err
}
