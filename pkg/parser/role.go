package parser

type (
	// CreateRoleStmt represents CREATE ROLE statements
	// Syntax: CREATE ROLE [IF NOT EXISTS | OR REPLACE] name [ON CLUSTER cluster] [SETTINGS ...];
	CreateRoleStmt struct {
		OrReplace   bool          `parser:"'CREATE' (@'OR' 'REPLACE')? 'ROLE'"`
		IfNotExists bool          `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name        string        `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string       `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Settings    *RoleSettings `parser:"@@?"`
		Semicolon   bool          `parser:"@';'"`
	}

	// AlterRoleStmt represents ALTER ROLE statements
	// Syntax: ALTER ROLE [IF EXISTS] name [ON CLUSTER cluster] [RENAME TO new_name] [SETTINGS ...];
	AlterRoleStmt struct {
		IfExists  bool          `parser:"'ALTER' 'ROLE' @('IF' 'EXISTS')?"`
		Name      string        `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string       `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		RenameTo  *string       `parser:"('RENAME' 'TO' @(Ident | BacktickIdent))?"`
		Settings  *RoleSettings `parser:"@@?"`
		Semicolon bool          `parser:"@';'"`
	}

	// DropRoleStmt represents DROP ROLE statements
	// Syntax: DROP ROLE [IF EXISTS] name [,...] [ON CLUSTER cluster];
	DropRoleStmt struct {
		IfExists  bool     `parser:"'DROP' 'ROLE' @('IF' 'EXISTS')?"`
		Names     []string `parser:"@(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))*"`
		OnCluster *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon bool     `parser:"';'"`
	}

	// SetRoleStmt represents SET ROLE statements for session management
	// Syntax: SET ROLE {DEFAULT | NONE | ALL | ALL EXCEPT name [,...] | name [,...]};
	SetRoleStmt struct {
		Default   bool      `parser:"'SET' 'ROLE' (@'DEFAULT'"`
		None      bool      `parser:"| @'NONE'"`
		All       bool      `parser:"| @'ALL'"`
		AllExcept *RoleList `parser:"| 'ALL' 'EXCEPT' @@"`
		Roles     *RoleList `parser:"| @@)"`
		Semicolon bool      `parser:"';'"`
	}

	// SetDefaultRoleStmt represents SET DEFAULT ROLE statements
	// Syntax: SET DEFAULT ROLE {NONE | ALL | name [,...] | ALL EXCEPT name [,...]} TO user [,...];
	SetDefaultRoleStmt struct {
		None      bool      `parser:"'SET' 'DEFAULT' 'ROLE' (@'NONE'"`
		All       bool      `parser:"| @'ALL'"`
		Roles     *RoleList `parser:"| @@"`
		AllExcept *RoleList `parser:"| 'ALL' 'EXCEPT' @@)"`
		ToUsers   []string  `parser:"'TO' @(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))*"`
		Semicolon bool      `parser:"';'"`
	}

	// GrantStmt represents GRANT statements
	// Syntax: GRANT {privilege | role} [,...] [ON {database.table | *.*}] TO {user | role} [,...] [WITH GRANT OPTION];
	GrantStmt struct {
		Privileges  *PrivilegeList `parser:"'GRANT' @@"`
		OnCluster   *string        `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		On          *GrantTarget   `parser:"('ON' @@)?"`
		To          *GranteeList   `parser:"'TO' @@"`
		WithGrant   bool           `parser:"@('WITH' 'GRANT' 'OPTION')?"`
		WithReplace bool           `parser:"@('WITH' 'REPLACE' 'OPTION')?"`
		WithAdmin   bool           `parser:"@('WITH' 'ADMIN' 'OPTION')?"`
		Semicolon   bool           `parser:"';'"`
	}

	// RevokeStmt represents REVOKE statements
	// Syntax: REVOKE [GRANT OPTION FOR | ADMIN OPTION FOR] {privilege | role} [,...] [ON {database.table | *.*}] FROM {user | role} [,...];
	RevokeStmt struct {
		GrantOption bool           `parser:"'REVOKE' (@'GRANT' 'OPTION' 'FOR'"`
		AdminOption bool           `parser:"| @'ADMIN' 'OPTION' 'FOR')?"`
		Privileges  *PrivilegeList `parser:"@@"`
		OnCluster   *string        `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		On          *GrantTarget   `parser:"('ON' @@)?"`
		From        *GranteeList   `parser:"'FROM' @@"`
		Semicolon   bool           `parser:"@';'"`
	}

	// RoleSettings represents SETTINGS clause for roles
	RoleSettings struct {
		Settings []*RoleSetting `parser:"'SETTINGS' @@ (',' @@)*"`
	}

	// RoleSetting represents a single role setting
	RoleSetting struct {
		Name  string  `parser:"@(Ident | BacktickIdent)"`
		Value *string `parser:"('=' @(Number | String | Ident | BacktickIdent))?"`
	}

	// RoleList represents a list of role names
	RoleList struct {
		Names []string `parser:"@(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))*"`
	}

	// PrivilegeList represents a list of privileges or roles in GRANT/REVOKE
	PrivilegeList struct {
		Items []*PrivilegeItem `parser:"@@ (',' @@)*"`
	}

	// PrivilegeItem represents a single privilege or role
	PrivilegeItem struct {
		// This is simplified - in reality, privileges can be complex expressions
		// We'll parse them as identifiers and let ClickHouse validate
		All     bool     `parser:"@'ALL'"`
		Name    string   `parser:"| @(Ident | BacktickIdent)"`
		Columns []string `parser:"('(' @(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))* ')')?"`
	}

	// GrantTarget represents the target of a GRANT/REVOKE (database.table or *.*)
	GrantTarget struct {
		Star1    *string `parser:"( @'*'"`
		Star2    *string `parser:"  '.' @'*'"`
		Database *string `parser:"| @(Ident | BacktickIdent)"`
		Table    *string `parser:"  '.' @(Ident | BacktickIdent | '*'))"`
	}

	// GranteeList represents the list of users/roles receiving privileges
	GranteeList struct {
		Items []*Grantee `parser:"@@ (',' @@)*"`
	}

	// Grantee represents a user or role receiving privileges
	Grantee struct {
		Name      string `parser:"@(Ident | BacktickIdent)"`
		IsCurrent bool   `parser:"| @'CURRENT_USER'"`
	}
)
