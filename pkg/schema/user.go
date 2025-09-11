package schema

import (
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// UserDiffCreate indicates a user needs to be created
	UserDiffCreate UserDiffType = "CREATE"
	// UserDiffDrop indicates a user needs to be dropped
	UserDiffDrop UserDiffType = "DROP"
	// UserDiffAlter indicates a user needs to be altered (via OR REPLACE)
	UserDiffAlter UserDiffType = "ALTER"
)

type (
	// UserDiff represents a difference between current and target user states.
	UserDiff struct {
		Type        UserDiffType // Type of operation (CREATE, ALTER, DROP)
		UserName    string       // Name of the user being modified
		Description string       // Human-readable description of the change
		UpSQL       string       // SQL to apply the change (forward migration)
		DownSQL     string       // SQL to rollback the change (reverse migration)
		Current     *UserInfo    // Current state (nil if user doesn't exist)
		Target      *UserInfo    // Target state (nil if user should be dropped)
	}

	// UserDiffType represents the type of user difference
	UserDiffType string

	// UserInfo represents parsed user information extracted from CREATE USER statements.
	UserInfo struct {
		Name              string // User name
		Cluster           string // Cluster name for distributed users
		IfNotExists       bool   // Whether IF NOT EXISTS was used
		OrReplace         bool   // Whether OR REPLACE was used
		Identification    string // Identification method (formatted)
		Host              string // Host restriction (formatted)
		ValidUntil        string // Valid until timestamp
		AccessStorageType string // Access storage type
		Roles             string // Default roles (formatted)
		DefaultDatabase   string // Default database
		Grantees          string // Grantees (formatted)
	}
)

// compareUsers compares current and target parsed schemas to find user differences.
func compareUsers(current, target *parser.SQL) ([]*UserDiff, error) {
	currentUsers := extractUsersFromSQL(current)
	targetUsers := extractUsersFromSQL(target)

	return generateUserDiffs(currentUsers, targetUsers)
}

// extractUsersFromSQL extracts user information from parsed SQL statements
func extractUsersFromSQL(sql *parser.SQL) map[string]*UserInfo {
	users := make(map[string]*UserInfo)

	for _, stmt := range sql.Statements {
		if stmt.CreateUser != nil {
			info := extractUsersInfo(stmt.CreateUser)
			users[info.Name] = info
		}
	}

	return users
}

// extractUsersInfo extracts UserInfo from a CREATE USER statement
func extractUsersInfo(user *parser.CreateUserStmt) *UserInfo {
	userName := normalizeIdentifier(user.Name)

	userInfo := &UserInfo{
		Name:        userName,
		IfNotExists: user.Modifier != nil && user.Modifier.IfNotExists,
		OrReplace:   user.Modifier != nil && user.Modifier.OrReplace,
	}

	if user.OnCluster != nil {
		userInfo.Cluster = *user.OnCluster
	}
	if user.Identification != nil {
		userInfo.Identification = formatUserIdentificationInfo(user.Identification)
	}
	if user.Host != nil {
		userInfo.Host = formatUserHostInfo(user.Host)
	}
	if user.ValidUntil != nil {
		userInfo.ValidUntil = *user.ValidUntil
	}
	if user.AccessStorageType != nil {
		userInfo.AccessStorageType = *user.AccessStorageType
	}
	if user.Roles != nil {
		userInfo.Roles = formatRolesInfo(user.Roles)
	}
	if user.DefaultDatabase != nil {
		userInfo.DefaultDatabase = formatDefaultDatabaseInfo(user.DefaultDatabase)
	}
	if user.Grantees != nil {
		userInfo.Grantees = formatGranteesInfo(user.Grantees)
	}

	return userInfo
}

// generateUserDiffs compares current and target named collections and generates differences.
// This function identifies what changes need to be made to transform the current state
// into the target state, generating appropriate SQL migration statements.
func generateUserDiffs(current, target map[string]*UserInfo) ([]*UserDiff, error) {
	diffs := make([]*UserDiff, 0, len(current)+len(target))

	// Find users to create or modify (exist in target) - sorted for deterministic order
	targetUserNames := make([]string, 0, len(target))
	for userName := range target {
		targetUserNames = append(targetUserNames, userName)
	}
	sort.Strings(targetUserNames)

	for _, userName := range targetUserNames {
		targetInfo := target[userName]
		currentInfo := current[userName]

		switch {
		case currentInfo == nil && targetInfo != nil:
			// User needs to be created
			diff := generateUserCreateDiff(targetInfo)
			diffs = append(diffs, diff)
		case currentInfo != nil && targetInfo != nil:
			// User exists in both - check if needs modification
			if !areUsersEqual(currentInfo, targetInfo) {
				// Generate replace diff (ALTER operations are complex, so we use CREATE OR REPLACE)
				diff := generateUserReplaceDiff(currentInfo, targetInfo)
				diffs = append(diffs, diff)
			}
		}
	}

	// Find users to drop (exist in current but not in target) - sorted for deterministic order
	currentUserNames := make([]string, 0, len(current))
	for userName := range current {
		if _, exists := target[userName]; !exists {
			currentUserNames = append(currentUserNames, userName)
		}
	}
	sort.Strings(currentUserNames)

	for _, userName := range currentUserNames {
		currentUser := current[userName]
		diff := &UserDiff{
			Type:        UserDiffDrop,
			UserName:    userName,
			Description: "Drop user " + userName,
			Current:     currentUser,
		}
		diff.UpSQL = generateDropUserSQL(currentUser)
		diff.DownSQL = generateCreateUserSQL(currentUser)
		diffs = append(diffs, diff)
	}

	return diffs, nil
}

// areUsersEqual compares two users for equality
func areUsersEqual(a, b *UserInfo) bool {
	clusterMatch := a.Cluster == b.Cluster

	return clusterMatch &&
		a.Identification == b.Identification &&
		a.Host == b.Host &&
		a.ValidUntil == b.ValidUntil &&
		a.AccessStorageType == b.AccessStorageType &&
		a.Roles == b.Roles &&
		a.DefaultDatabase == b.DefaultDatabase &&
		a.Grantees == b.Grantees
}

// SQL generation functions

func generateUserCreateDiff(targetInfo *UserInfo) *UserDiff {
	diff := &UserDiff{
		Type:        UserDiffCreate,
		UserName:    targetInfo.Name,
		Description: "Create user " + targetInfo.Name,
		Target:      targetInfo,
	}
	diff.UpSQL = generateCreateUserSQL(targetInfo)
	diff.DownSQL = generateDropUserSQL(targetInfo)

	return diff
}

func generateUserDropDiff(currentInfo *UserInfo) *UserDiff {
	diff := &UserDiff{
		Type:        UserDiffDrop,
		UserName:    currentInfo.Name,
		Description: "Drop user " + currentInfo.Name,
		Current:     currentInfo,
	}
	diff.UpSQL = generateDropUserSQL(currentInfo)
	diff.DownSQL = generateCreateUserSQL(currentInfo)

	return diff
}

func generateUserReplaceDiff(currentInfo, targetInfo *UserInfo) *UserDiff {
	diff := &UserDiff{
		Type:        UserDiffAlter,
		UserName:    currentInfo.Name,
		Description: "Alter user " + currentInfo.Name + " (using CREATE OR REPLACE)",
		Current:     currentInfo,
		Target:      targetInfo,
	}
	diff.UpSQL = generateCreateOrReplaceUserSQL(targetInfo)
	diff.DownSQL = generateCreateOrReplaceUserSQL(currentInfo)

	return diff
}

func generateCreateUserSQL(user *UserInfo) string {
	return generateUserSQL(user, false)
}

func generateCreateOrReplaceUserSQL(user *UserInfo) string {
	return generateUserSQL(user, true)
}

func generateUserSQL(user *UserInfo, orReplace bool) string {
	var sql strings.Builder

	sql.WriteString("CREATE ")
	sql.WriteString("USER ")

	if orReplace {
		sql.WriteString("OR REPLACE ")
	}

	if !orReplace && user.IfNotExists {
		sql.WriteString("IF NOT EXISTS ")
	}
	sql.WriteString(user.Name)

	if user.Cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(user.Cluster)
	}

	if user.Identification != "" {
		sql.WriteString(" ")
		sql.WriteString(user.Identification)
	}

	if user.Host != "" {
		sql.WriteString(" ")
		sql.WriteString(user.Host)
	}

	if user.ValidUntil != "" {
		sql.WriteString(" VALID UNTIL ")
		sql.WriteString(user.ValidUntil)
	}

	if user.AccessStorageType != "" {
		sql.WriteString(" IN ")
		sql.WriteString(user.AccessStorageType)
	}

	if user.Roles != "" {
		sql.WriteString(" ")
		sql.WriteString(user.Roles)
	}

	if user.DefaultDatabase != "" {
		sql.WriteString(" ")
		sql.WriteString(user.DefaultDatabase)
	}

	if user.Grantees != "" {
		sql.WriteString(" ")
		sql.WriteString(user.Grantees)
	}

	return sql.String()
}

func generateDropUserSQL(user *UserInfo) string {
	var sql strings.Builder
	sql.WriteString("DROP USER IF EXISTS ")
	sql.WriteString(user.Name)
	if user.Cluster != "" {
		sql.WriteString(" ON CLUSTER ")
		sql.WriteString(user.Cluster)
	}
	return sql.String()
}

// Helper functions to format user properties

func formatUserIdentificationInfo(id *parser.UserIdentification) string {
	if id == nil {
		return ""
	}

	if id.NotIdentified {
		return "NOT IDENTIFIED"
	}

	if id.IdentifiedWithHttp != nil {
		return "IDENTIFIED WITH http SERVER " + *id.IdentifiedWithHttp
	}

	if id.IdentifiedWithLdap != nil {
		return "IDENTIFIED WITH ldap SERVER " + *id.IdentifiedWithLdap
	}

	if id.IdentifiedWithKerberos != nil {
		parts := []string{"IDENTIFIED WITH kerberos"}
		if id.IdentifiedWithKerberos.Realm != nil {
			parts = append(parts, "REALM", *id.IdentifiedWithKerberos.Realm)
		}
		return strings.Join(parts, " ")
	}

	if id.IdentifiedWithSslCertificate != nil {
		return "IDENTIFIED WITH ssl_certificate CN " + *id.IdentifiedWithSslCertificate
	}

	if id.IdentifiedWithOther != nil {
		parts := []string{"IDENTIFIED"}

		if id.IdentifiedWithOther.With != nil {
			parts = append(parts, "WITH", *id.IdentifiedWithOther.With)
		}

		parts = append(parts, "BY", id.IdentifiedWithOther.By)

		if id.IdentifiedWithOther.Salt != nil {
			parts = append(parts, "SALT", *id.IdentifiedWithOther.Salt)
		}

		if id.IdentifiedWithOther.ValidUntil != nil {
			parts = append(parts, "VALID UNTIL", *id.IdentifiedWithOther.ValidUntil)
		}

		return strings.Join(parts, " ")
	}

	return ""
}

func formatUserHostInfo(host *parser.UserHost) string {
	if host == nil {
		return ""
	}

	if host.IP != nil {
		return "HOST IP " + *host.IP
	}

	if host.Any {
		return "HOST ANY"
	}

	if host.Local {
		return "HOST LOCAL"
	}

	if host.Name != nil {
		return "HOST NAME " + *host.Name
	}

	if host.Regexp != nil {
		return "HOST REGEXP " + *host.Regexp
	}

	if host.Like != nil {
		return "HOST LIKE " + *host.Like
	}

	return ""
}

func formatRolesInfo(roles *parser.Roles) string {
	if roles == nil || len(roles.Roles) == 0 {
		return ""
	}

	parts := []string{"DEFAULT ROLE"}

	// Format the role list
	roleNames := make([]string, len(roles.Roles))
	for i, role := range roles.Roles {
		roleNames[i] = removeQuotes(role)
	}
	parts = append(parts, strings.Join(roleNames, ", "))

	// Add EXCEPT clause if present
	if len(roles.Except) > 0 {
		exceptNames := make([]string, len(roles.Except))
		for i, role := range roles.Except {
			exceptNames[i] = removeQuotes(role)
		}
		parts = append(parts, "EXCEPT", strings.Join(exceptNames, ", "))
	}

	return strings.Join(parts, " ")
}

func formatDefaultDatabaseInfo(db *parser.DefaultDatabase) string {
	if db == nil {
		return ""
	}

	if db.None {
		return "DEFAULT DATABASE NONE"
	}

	if db.Database != nil {
		cleaned := removeQuotes(*db.Database)

		// Check if it's NONE keyword (parser may store this as a string)
		if strings.ToUpper(cleaned) == "NONE" {
			return "DEFAULT DATABASE NONE"
		}

		return "DEFAULT DATABASE " + cleaned
	}

	return ""
}

func formatGranteesInfo(grantees *parser.UserGrantees) string {
	if grantees == nil || len(grantees.Items) == 0 {
		return ""
	}

	parts := []string{"GRANTEES"}

	// Format grantee items
	items := make([]string, len(grantees.Items))
	for i, item := range grantees.Items {
		items[i] = formatGranteeItemInfo(&item)
	}
	parts = append(parts, strings.Join(items, ", "))

	// Add EXCEPT clause if present
	if grantees.Except != nil && len(grantees.Except.Items) > 0 {
		exceptItems := make([]string, len(grantees.Except.Items))
		for i, item := range grantees.Except.Items {
			exceptItems[i] = formatGranteeItemInfo(&item)
		}
		parts = append(parts, "EXCEPT", strings.Join(exceptItems, ", "))
	}

	return strings.Join(parts, " ")
}

func formatGranteeItemInfo(item *parser.GranteeItem) string {
	if item == nil {
		return ""
	}

	if item.Any {
		return "ANY"
	}

	if item.None {
		return "NONE"
	}

	if item.UserOrRole != nil {
		cleaned := removeQuotes(*item.UserOrRole)

		// Check if it's ANY or NONE keywords (parser may store these as strings)
		upper := strings.ToUpper(cleaned)
		if upper == "ANY" || upper == "NONE" {
			return upper
		}

		return cleaned
	}

	return ""
}
