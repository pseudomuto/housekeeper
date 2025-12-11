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
		DiffBase           // Embeds Type, Name, NewName, Description, UpSQL, DownSQL
		Current  *RoleInfo // Current state (nil if role doesn't exist)
		Target   *RoleInfo // Target state (nil if role should be dropped)
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

// GetName implements SchemaObject interface
func (r *RoleInfo) GetName() string {
	return r.Name
}

// GetCluster implements SchemaObject interface
func (r *RoleInfo) GetCluster() string {
	return r.Cluster
}

// PropertiesMatch implements SchemaObject interface.
// Returns true if the two roles have identical properties (excluding name).
func (r *RoleInfo) PropertiesMatch(other SchemaObject) bool {
	otherRole, ok := other.(*RoleInfo)
	if !ok {
		return false
	}

	if r.Cluster != otherRole.Cluster {
		return false
	}

	// Compare settings
	if len(r.Settings) != len(otherRole.Settings) {
		return false
	}

	for k, v := range r.Settings {
		if otherRole.Settings[k] != v {
			return false
		}
	}

	return true
}

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

	// Detect renames using generic algorithm
	renames, processedCurrent, processedTarget := DetectRenames(currentRoles, targetRoles)

	// Create rename diffs
	for _, rename := range renames {
		currentRole := currentRoles[rename.OldName]
		targetRole := targetRoles[rename.NewName]
		diff := &RoleDiff{
			DiffBase: DiffBase{
				Type:        string(RoleDiffRename),
				Name:        rename.OldName,
				NewName:     rename.NewName,
				Description: fmt.Sprintf("Rename role '%s' to '%s'", rename.OldName, rename.NewName),
				UpSQL:       generateRenameRoleSQL(currentRole, rename.NewName),
				DownSQL:     generateRenameRoleSQL(targetRole, rename.OldName),
			},
			Current: currentRole,
			Target:  targetRole,
		}
		diffs = append(diffs, diff)
	}

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
				DiffBase: DiffBase{
					Type:        string(RoleDiffCreate),
					Name:        name,
					Description: fmt.Sprintf("Create role '%s'", name),
					UpSQL:       generateCreateRoleSQL(targetRole),
					DownSQL:     generateDropRoleSQL(targetRole),
				},
				Target: targetRole,
			}
			diffs = append(diffs, diff)
		} else if needsAlter := compareRoleSettings(currentRole, targetRole); needsAlter {
			// Role exists but needs modification
			diff := &RoleDiff{
				DiffBase: DiffBase{
					Type:        string(RoleDiffAlter),
					Name:        name,
					Description: fmt.Sprintf("Alter role '%s'", name),
					UpSQL:       generateAlterRoleSQL(currentRole, targetRole),
					DownSQL:     generateAlterRoleSQL(targetRole, currentRole),
				},
				Current: currentRole,
				Target:  targetRole,
			}
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
				DiffBase: DiffBase{
					Type:        string(RoleDiffDrop),
					Name:        name,
					Description: fmt.Sprintf("Drop role '%s'", name),
					UpSQL:       generateDropRoleSQL(currentRole),
					DownSQL:     generateCreateRoleSQL(currentRole),
				},
				Current: currentRole,
			}
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
				DiffBase: DiffBase{
					Type:        string(RoleDiffGrant),
					Description: fmt.Sprintf("Grant %s to %s", strings.Join(targetGrant.Privileges, ", "), targetGrant.Grantee),
					UpSQL:       generateGrantSQL(targetGrant),
					DownSQL:     generateRevokeSQL(targetGrant),
				},
			}
			diffs = append(diffs, diff)
		}
	}

	// Find grants to revoke
	for key, currentGrant := range currentMap {
		if _, exists := targetMap[key]; !exists {
			diff := &RoleDiff{
				DiffBase: DiffBase{
					Type:        string(RoleDiffRevoke),
					Description: fmt.Sprintf("Revoke %s from %s", strings.Join(currentGrant.Privileges, ", "), currentGrant.Grantee),
					UpSQL:       generateRevokeSQL(currentGrant),
					DownSQL:     generateGrantSQL(currentGrant),
				},
			}
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
