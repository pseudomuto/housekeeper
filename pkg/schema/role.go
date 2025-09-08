package schema

import (
	"fmt"
	"sort"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

const (
	// RoleDiffCreate indicates a role needs to be created
	RoleDiffCreate RoleDiffType = "CREATE"
	// RoleDiffDrop indicates a role needs to be dropped
	RoleDiffDrop RoleDiffType = "DROP"
	// RoleDiffAlter indicates a role needs to be altered
	RoleDiffAlter RoleDiffType = "ALTER"
	// RoleDiffRename indicates a role needs to be renamed
	RoleDiffRename RoleDiffType = "RENAME"
	// RoleDiffGrant indicates privileges need to be granted
	RoleDiffGrant RoleDiffType = "GRANT"
	// RoleDiffRevoke indicates privileges need to be revoked
	RoleDiffRevoke RoleDiffType = "REVOKE"
)

type (
	// RoleDiff represents a difference between current and target role states.
	// It contains all information needed to generate migration SQL statements for
	// role operations including CREATE, ALTER, DROP, RENAME, GRANT, and REVOKE.
	RoleDiff struct {
		Type        RoleDiffType // Type of operation (CREATE, ALTER, DROP, RENAME, GRANT, REVOKE)
		RoleName    string       // Name of the role being modified
		Description string       // Human-readable description of the change
		UpSQL       string       // SQL to apply the change (forward migration)
		DownSQL     string       // SQL to rollback the change (reverse migration)
		Current     *RoleInfo    // Current state (nil if role doesn't exist)
		Target      *RoleInfo    // Target state (nil if role should be dropped)
		NewRoleName string       // For rename operations - the new name
	}

	// RoleDiffType represents the type of role difference
	RoleDiffType string

	// RoleInfo represents parsed role information extracted from DDL statements.
	// This structure contains all the properties needed for role comparison and
	// migration generation, including settings and cluster configuration.
	RoleInfo struct {
		Name     string            // Role name
		Settings map[string]string // Role settings (key-value pairs)
		Cluster  string            // Cluster name if specified (empty if not clustered)
	}

	// GrantInfo represents parsed grant/privilege information
	GrantInfo struct {
		Grantee    string   // Role or user receiving the grant
		Privileges []string // List of privileges or roles granted
		OnTarget   string   // Target object (database.table or *.*)
		WithGrant  bool     // WITH GRANT OPTION
		WithAdmin  bool     // WITH ADMIN OPTION
		Cluster    string   // Cluster name if specified
	}
)

// compareRoles compares current and target role schemas and returns migration diffs.
// It analyzes both schemas to identify differences and generates appropriate migration operations.
//
// The function identifies:
//   - Roles that need to be created (exist in target but not current)
//   - Roles that need to be dropped (exist in current but not target)
//   - Roles that need to be altered (exist in both but have differences)
//   - Roles that need to be renamed (same properties but different names)
//   - Privilege changes (GRANT and REVOKE operations)
//
// Rename Detection:
// The function intelligently detects rename operations by comparing role properties
// (settings, cluster) excluding the name. If two roles have identical properties
// but different names, it generates a RENAME operation instead of DROP+CREATE.
func compareRoles(current, target *parser.SQL) []*RoleDiff {
	// Extract role information from both SQL structures
	currentRoles := extractRoleInfo(current)
	targetRoles := extractRoleInfo(target)

	// Extract grant information
	currentGrants := extractGrantInfo(current)
	targetGrants := extractGrantInfo(target)

	// Pre-allocate diffs slice with estimated capacity
	diffs := make([]*RoleDiff, 0, len(currentRoles)+len(targetRoles)+len(currentGrants)+len(targetGrants))

	// Detect renames first to avoid treating them as drop+create
	renameDiffs, processedCurrent, processedTarget := detectRoleRenames(currentRoles, targetRoles)
	diffs = append(diffs, renameDiffs...)

	// Find roles to create or modify (sorted for deterministic order)
	targetNames := make([]string, 0, len(processedTarget))
	for name := range processedTarget {
		targetNames = append(targetNames, name)
	}
	sort.Strings(targetNames)

	for _, name := range targetNames {
		targetRole := processedTarget[name]
		currentRole, exists := processedCurrent[name]

		if !exists {
			// Role doesn't exist - create it
			diff := &RoleDiff{
				Type:        RoleDiffCreate,
				RoleName:    name,
				Description: fmt.Sprintf("Create role '%s'", name),
				Target:      targetRole,
			}
			diff.UpSQL = generateCreateRoleSQL(targetRole)
			diff.DownSQL = generateDropRoleSQL(targetRole)
			diffs = append(diffs, diff)
		} else if needsAlter := compareRoleSettings(currentRole, targetRole); needsAlter {
			// Role exists but needs modification
			diff := &RoleDiff{
				Type:        RoleDiffAlter,
				RoleName:    name,
				Description: fmt.Sprintf("Alter role '%s'", name),
				Current:     currentRole,
				Target:      targetRole,
			}
			diff.UpSQL = generateAlterRoleSQL(currentRole, targetRole)
			diff.DownSQL = generateAlterRoleSQL(targetRole, currentRole)
			diffs = append(diffs, diff)
		}
	}

	// Find roles to drop (sorted for deterministic order)
	currentNames := make([]string, 0, len(processedCurrent))
	for name := range processedCurrent {
		currentNames = append(currentNames, name)
	}
	sort.Strings(currentNames)

	for _, name := range currentNames {
		if _, exists := processedTarget[name]; !exists {
			currentRole := processedCurrent[name]
			diff := &RoleDiff{
				Type:        RoleDiffDrop,
				RoleName:    name,
				Description: fmt.Sprintf("Drop role '%s'", name),
				Current:     currentRole,
			}
			diff.UpSQL = generateDropRoleSQL(currentRole)
			diff.DownSQL = generateCreateRoleSQL(currentRole)
			diffs = append(diffs, diff)
		}
	}

	// Compare grants and generate GRANT/REVOKE operations
	grantDiffs := compareGrants(currentGrants, targetGrants)
	diffs = append(diffs, grantDiffs...)

	return diffs
}

// extractRoleInfo extracts role information from parsed SQL
func extractRoleInfo(sql *parser.SQL) map[string]*RoleInfo {
	if sql == nil {
		return make(map[string]*RoleInfo)
	}

	roles := make(map[string]*RoleInfo)
	for _, stmt := range sql.Statements {
		if stmt.CreateRole != nil {
			role := &RoleInfo{
				Name:     stmt.CreateRole.Name,
				Settings: extractRoleSettings(stmt.CreateRole.Settings),
			}
			if stmt.CreateRole.OnCluster != nil {
				role.Cluster = *stmt.CreateRole.OnCluster
			}
			roles[role.Name] = role
		}
	}
	return roles
}

// extractRoleSettings converts parser.RoleSettings to a map
func extractRoleSettings(settings *parser.RoleSettings) map[string]string {
	result := make(map[string]string)
	if settings == nil {
		return result
	}

	for _, setting := range settings.Settings {
		if setting.Value != nil {
			result[setting.Name] = *setting.Value
		} else {
			result[setting.Name] = ""
		}
	}
	return result
}

// extractGrantInfo extracts grant information from parsed SQL
func extractGrantInfo(sql *parser.SQL) []*GrantInfo {
	if sql == nil {
		return nil
	}

	var grants []*GrantInfo
	for _, stmt := range sql.Statements {
		if stmt.Grant != nil {
			for _, grantee := range stmt.Grant.To.Items {
				grant := &GrantInfo{
					Grantee:    getGranteeName(grantee),
					Privileges: extractPrivileges(stmt.Grant.Privileges),
					WithGrant:  stmt.Grant.WithGrant,
					WithAdmin:  stmt.Grant.WithAdmin,
				}
				if stmt.Grant.On != nil {
					grant.OnTarget = formatGrantTarget(stmt.Grant.On)
				}
				if stmt.Grant.OnCluster != nil {
					grant.Cluster = *stmt.Grant.OnCluster
				}
				grants = append(grants, grant)
			}
		}
	}
	return grants
}

// getGranteeName returns the name of a grantee
func getGranteeName(grantee *parser.Grantee) string {
	if grantee.IsCurrent {
		return "CURRENT_USER"
	}
	return grantee.Name
}

// extractPrivileges extracts privilege names from a PrivilegeList
func extractPrivileges(list *parser.PrivilegeList) []string {
	if list == nil {
		return nil
	}

	var privs []string
	for _, item := range list.Items {
		if len(item.Columns) > 0 {
			privs = append(privs, fmt.Sprintf("%s(%s)", item.Name, strings.Join(item.Columns, ", ")))
		} else {
			privs = append(privs, item.Name)
		}
	}
	return privs
}

// formatGrantTarget formats a GrantTarget as a string
func formatGrantTarget(target *parser.GrantTarget) string {
	if target.Star1 != nil && target.Star2 != nil {
		return "*.*"
	}
	if target.Database != nil && target.Table != nil {
		return fmt.Sprintf("%s.%s", *target.Database, *target.Table)
	}
	return ""
}

// detectRoleRenames detects roles that have been renamed
func detectRoleRenames(current, target map[string]*RoleInfo) ([]*RoleDiff, map[string]*RoleInfo, map[string]*RoleInfo) {
	var diffs []*RoleDiff
	processedCurrent := make(map[string]*RoleInfo)
	processedTarget := make(map[string]*RoleInfo)

	// Copy all to processed maps initially
	for k, v := range current {
		processedCurrent[k] = v
	}
	for k, v := range target {
		processedTarget[k] = v
	}

	// Look for potential renames
	for currentName, currentRole := range current {
		if _, exists := target[currentName]; exists {
			continue // Not a rename
		}

		// Look for a matching role with different name
		for targetName, targetRole := range target {
			if _, exists := current[targetName]; exists {
				continue // Not a rename
			}

			// Check if roles are identical except for name
			if rolesMatchExceptName(currentRole, targetRole) {
				diff := &RoleDiff{
					Type:        RoleDiffRename,
					RoleName:    currentName,
					NewRoleName: targetName,
					Description: fmt.Sprintf("Rename role '%s' to '%s'", currentName, targetName),
					Current:     currentRole,
					Target:      targetRole,
				}
				diff.UpSQL = generateRenameRoleSQL(currentRole, targetName)
				diff.DownSQL = generateRenameRoleSQL(targetRole, currentName)
				diffs = append(diffs, diff)

				// Remove from processed maps
				delete(processedCurrent, currentName)
				delete(processedTarget, targetName)
				break
			}
		}
	}

	return diffs, processedCurrent, processedTarget
}

// rolesMatchExceptName checks if two roles are identical except for their names
func rolesMatchExceptName(r1, r2 *RoleInfo) bool {
	if r1.Cluster != r2.Cluster {
		return false
	}

	// Compare settings
	if len(r1.Settings) != len(r2.Settings) {
		return false
	}

	for k, v := range r1.Settings {
		if r2.Settings[k] != v {
			return false
		}
	}

	return true
}

// compareRoleSettings checks if role settings need to be updated
func compareRoleSettings(current, target *RoleInfo) bool {
	// Compare settings count
	if len(current.Settings) != len(target.Settings) {
		return true
	}

	// Compare each setting
	for k, v := range target.Settings {
		if currentVal, exists := current.Settings[k]; !exists || currentVal != v {
			return true
		}
	}

	return false
}

// generateGrantKey creates a unique key for grant comparison.
// The key format is "grantee:privileges:target" which uniquely identifies a grant.
func generateGrantKey(grant *GrantInfo) string {
	return fmt.Sprintf("%s:%s:%s", grant.Grantee, strings.Join(grant.Privileges, ","), grant.OnTarget)
}

// compareGrants compares grant information and generates GRANT/REVOKE diffs
func compareGrants(current, target []*GrantInfo) []*RoleDiff {
	var diffs []*RoleDiff

	// Build maps for easier comparison
	currentMap := make(map[string]*GrantInfo)
	for _, grant := range current {
		key := generateGrantKey(grant)
		currentMap[key] = grant
	}

	targetMap := make(map[string]*GrantInfo)
	for _, grant := range target {
		key := generateGrantKey(grant)
		targetMap[key] = grant
	}

	// Find grants to add
	for key, targetGrant := range targetMap {
		if _, exists := currentMap[key]; !exists {
			diff := &RoleDiff{
				Type:        RoleDiffGrant,
				Description: fmt.Sprintf("Grant %s to %s", strings.Join(targetGrant.Privileges, ", "), targetGrant.Grantee),
			}
			diff.UpSQL = generateGrantSQL(targetGrant)
			diff.DownSQL = generateRevokeSQL(targetGrant)
			diffs = append(diffs, diff)
		}
	}

	// Find grants to revoke
	for key, currentGrant := range currentMap {
		if _, exists := targetMap[key]; !exists {
			diff := &RoleDiff{
				Type:        RoleDiffRevoke,
				Description: fmt.Sprintf("Revoke %s from %s", strings.Join(currentGrant.Privileges, ", "), currentGrant.Grantee),
			}
			diff.UpSQL = generateRevokeSQL(currentGrant)
			diff.DownSQL = generateGrantSQL(currentGrant)
			diffs = append(diffs, diff)
		}
	}

	return diffs
}

// SQL generation functions

func generateCreateRoleSQL(role *RoleInfo) string {
	var parts []string
	parts = append(parts, "CREATE ROLE IF NOT EXISTS", fmt.Sprintf("`%s`", role.Name))

	if role.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", role.Cluster))
	}

	if len(role.Settings) > 0 {
		parts = append(parts, "SETTINGS", formatSettings(role.Settings))
	}

	return strings.Join(parts, " ") + ";"
}

func generateDropRoleSQL(role *RoleInfo) string {
	var parts []string
	parts = append(parts, "DROP ROLE IF EXISTS", fmt.Sprintf("`%s`", role.Name))

	if role.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", role.Cluster))
	}

	return strings.Join(parts, " ") + ";"
}

func generateAlterRoleSQL(current, target *RoleInfo) string {
	var parts []string
	parts = append(parts, "ALTER ROLE", fmt.Sprintf("`%s`", current.Name))

	if current.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", current.Cluster))
	}

	if len(target.Settings) > 0 {
		parts = append(parts, "SETTINGS", formatSettings(target.Settings))
	}

	return strings.Join(parts, " ") + ";"
}

func generateRenameRoleSQL(role *RoleInfo, newName string) string {
	var parts []string
	parts = append(parts, "ALTER ROLE", fmt.Sprintf("`%s`", role.Name))

	if role.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", role.Cluster))
	}

	parts = append(parts, "RENAME TO", fmt.Sprintf("`%s`", newName))

	return strings.Join(parts, " ") + ";"
}

func generateGrantSQL(grant *GrantInfo) string {
	var parts []string
	parts = append(parts, "GRANT", strings.Join(grant.Privileges, ", "))

	if grant.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", grant.Cluster))
	}

	if grant.OnTarget != "" {
		parts = append(parts, "ON", grant.OnTarget)
	}

	parts = append(parts, "TO", grant.Grantee)

	if grant.WithGrant {
		parts = append(parts, "WITH GRANT OPTION")
	}
	if grant.WithAdmin {
		parts = append(parts, "WITH ADMIN OPTION")
	}

	return strings.Join(parts, " ") + ";"
}

func generateRevokeSQL(grant *GrantInfo) string {
	var parts []string
	parts = append(parts, "REVOKE", strings.Join(grant.Privileges, ", "))

	if grant.Cluster != "" {
		parts = append(parts, "ON CLUSTER", fmt.Sprintf("`%s`", grant.Cluster))
	}

	if grant.OnTarget != "" {
		parts = append(parts, "ON", grant.OnTarget)
	}

	parts = append(parts, "FROM", grant.Grantee)

	return strings.Join(parts, " ") + ";"
}

func formatSettings(settings map[string]string) string {
	var parts []string
	for k, v := range settings {
		if v != "" {
			parts = append(parts, fmt.Sprintf("%s = %s", k, v))
		} else {
			parts = append(parts, k)
		}
	}
	sort.Strings(parts) // For deterministic output
	return strings.Join(parts, ", ")
}
