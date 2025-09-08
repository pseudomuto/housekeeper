package clickhouse

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// tableNotFoundPatterns contains error patterns that indicate a table doesn't exist.
// These errors are safely ignored for backward compatibility with older ClickHouse versions.
var tableNotFoundPatterns = []string{
	"doesn't exist",
	"does not exist",
	"UNKNOWN_TABLE",
	"no such table",
	"Unknown table",
	"Table doesn't exist",
}

// GetRoles retrieves all role definitions from the ClickHouse instance.
// It queries the system.roles table and reconstructs CREATE ROLE statements
// with their settings and cluster configuration.
//
// Returns a *parser.SQL containing all role CREATE statements, or an error if the query fails.
func (c *Client) GetRoles(ctx context.Context) (*parser.SQL, error) {
	query := `
		SELECT
			name,
			storage
		FROM system.roles
		WHERE storage != 'local directory'
		ORDER BY name
	`

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query system.roles")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var name, storage string
		if err := rows.Scan(&name, &storage); err != nil {
			return nil, errors.Wrap(err, "failed to scan role row")
		}

		// Build CREATE ROLE statement
		stmt := fmt.Sprintf("CREATE ROLE IF NOT EXISTS `%s`", name)

		// Add ON CLUSTER if configured
		if c.options.Cluster != "" {
			stmt = fmt.Sprintf("%s ON CLUSTER `%s`", stmt, c.options.Cluster)
		}

		// Get role settings
		settings, err := c.getRoleSettings(ctx, name)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get settings for role %s", name)
		}

		if len(settings) > 0 {
			stmt = fmt.Sprintf("%s SETTINGS %s", stmt, strings.Join(settings, ", "))
		}

		statements = append(statements, stmt+";")
	}

	// Get grants for roles
	grants, err := c.getRoleGrants(ctx)
	if err != nil {
		return nil, errors.Wrap(err, "failed to get role grants")
	}
	statements = append(statements, grants...)

	if len(statements) == 0 {
		return &parser.SQL{}, nil
	}

	// Parse the generated SQL
	sql := strings.Join(statements, "\n\n")
	parsedSQL, err := parser.ParseString(sql)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse generated role SQL")
	}

	return parsedSQL, nil
}

// getRoleSettings retrieves settings for a specific role
func (c *Client) getRoleSettings(ctx context.Context, roleName string) ([]string, error) {
	// Query role settings from system.role_settings if available
	// This is a simplified implementation - in reality, role settings might be stored differently
	query := `
		SELECT
			setting_name,
			value
		FROM system.settings_profile_elements
		WHERE role_name = ?
		ORDER BY setting_name
	`

	rows, err := c.conn.Query(ctx, query, roleName)
	if err != nil {
		// Only ignore "table doesn't exist" errors for backward compatibility with older ClickHouse versions
		// All other errors (connection, permissions, etc.) should be returned
		if isTableNotFoundError(err, "system.settings_profile_elements") {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to query role settings")
	}
	defer rows.Close()

	var settings []string
	for rows.Next() {
		var settingName, value string
		if err := rows.Scan(&settingName, &value); err != nil {
			continue // Skip invalid settings
		}
		settings = append(settings, fmt.Sprintf("%s = %s", settingName, value))
	}

	return settings, nil
}

// getRoleGrants retrieves GRANT statements for all roles
func (c *Client) getRoleGrants(ctx context.Context) ([]string, error) { // nolint: funlen
	// Query grants from system.grants
	query := `
		SELECT
			user_name,
			role_name,
			access_type,
			database,
			table,
			column,
			is_partial_revoke,
			grant_option
		FROM system.grants
		WHERE role_name IS NOT NULL
		ORDER BY user_name, access_type
	`

	rows, err := c.conn.Query(ctx, query)
	if err != nil {
		// Only ignore "table doesn't exist" errors for backward compatibility with older ClickHouse versions
		// All other errors (connection, permissions, etc.) should be returned
		if isTableNotFoundError(err, "system.grants") {
			return nil, nil
		}
		return nil, errors.Wrap(err, "failed to query role grants")
	}
	defer rows.Close()

	// Group grants by grantee and privilege type for efficient statement generation
	type grantKey struct {
		grantee     string
		privilege   string
		target      string
		grantOption bool
		adminOption bool
	}
	grants := make(map[grantKey]bool)

	for rows.Next() {
		var (
			userName        *string
			roleName        *string
			accessType      string
			database        *string
			table           *string
			column          *string
			isPartialRevoke bool
			grantOption     bool
		)

		if err := rows.Scan(&userName, &roleName, &accessType, &database, &table, &column, &isPartialRevoke, &grantOption); err != nil {
			continue // Skip invalid grants
		}

		if isPartialRevoke {
			continue // Skip partial revokes for now
		}

		// Determine the grantee (could be user_name or role_name)
		var granteeName string
		if userName != nil {
			granteeName = *userName
		} else if roleName != nil {
			granteeName = *roleName
		} else {
			continue // Skip if neither user_name nor role_name is set
		}

		// Build target specification
		var target string
		if database != nil && table != nil {
			if *database == "*" && *table == "*" {
				target = "*.*"
			} else {
				target = fmt.Sprintf("`%s`.`%s`", *database, *table)
			}
		}

		// Add column specification if present
		privilege := accessType
		if column != nil && *column != "" {
			privilege = fmt.Sprintf("%s(`%s`)", accessType, *column)
		}

		key := grantKey{
			grantee:     granteeName,
			privilege:   privilege,
			target:      target,
			grantOption: grantOption,
			adminOption: false, // system.grants doesn't have admin_option column
		}
		grants[key] = true
	}

	// Convert grants map to statements
	statements := make([]string, 0, len(grants))
	for key := range grants {
		stmt := "GRANT " + key.privilege

		// Add ON CLUSTER if configured
		if c.options.Cluster != "" {
			stmt = fmt.Sprintf("%s ON CLUSTER `%s`", stmt, c.options.Cluster)
		}

		if key.target != "" {
			stmt = fmt.Sprintf("%s ON %s", stmt, key.target)
		}

		stmt = fmt.Sprintf("%s TO `%s`", stmt, key.grantee)

		if key.grantOption {
			stmt = stmt + " WITH GRANT OPTION"
		}
		// Note: system.grants doesn't provide admin_option information

		statements = append(statements, stmt+";")
	}

	return statements, nil
}

// isTableNotFoundError checks if an error is a "table not found" error that can be safely ignored.
func isTableNotFoundError(err error, tableName string) bool {
	if err == nil {
		return false
	}

	errStr := err.Error()
	// First check if the error mentions the specific table
	if !strings.Contains(errStr, tableName) {
		return false
	}

	// Then check if it matches any of the "table not found" patterns
	for _, pattern := range tableNotFoundPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}
