package parser

type (
	// CreateDatabaseStmt represents CREATE DATABASE statements
	// Syntax: CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'Comment'];
	CreateDatabaseStmt struct {
		IfNotExists bool            `parser:"'CREATE' 'DATABASE' @('IF' 'NOT' 'EXISTS')?"`
		Name        string          `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string         `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Engine      *DatabaseEngine `parser:"@@?"`
		Comment     *string         `parser:"('COMMENT' @String)?"`
		Semicolon   bool            `parser:"';'"`
	}

	// DatabaseEngine represents ENGINE = clause for databases
	DatabaseEngine struct {
		Name       string                 `parser:"'ENGINE' '=' @(Ident | BacktickIdent)"`
		Parameters []*DatabaseEngineParam `parser:"('(' @@ (',' @@)* ')')?"`
	}

	// DatabaseEngineParam represents parameters in ENGINE clause - can be strings, numbers, or identifiers
	DatabaseEngineParam struct {
		Value string `parser:"@(String | Number | Ident | BacktickIdent)"`
	}

	// AlterDatabaseStmt represents ALTER DATABASE statements
	// Syntax: ALTER DATABASE [db].name [ON CLUSTER cluster] MODIFY COMMENT 'Comment';
	AlterDatabaseStmt struct {
		Name      string               `parser:"'ALTER' 'DATABASE' @(Ident | BacktickIdent)"`
		OnCluster *string              `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Action    *AlterDatabaseAction `parser:"@@"`
		Semicolon bool                 `parser:"';'"`
	}

	// AlterDatabaseAction represents the action to perform on the database
	AlterDatabaseAction struct {
		ModifyComment *string `parser:"'MODIFY' 'COMMENT' @String"`
	}

	// AttachDatabaseStmt represents ATTACH DATABASE statements
	// Syntax: ATTACH DATABASE [IF NOT EXISTS] name [ENGINE = engine(...)] [ON CLUSTER cluster];
	AttachDatabaseStmt struct {
		IfNotExists bool            `parser:"'ATTACH' 'DATABASE' @('IF' 'NOT' 'EXISTS')?"`
		Name        string          `parser:"@(Ident | BacktickIdent)"`
		Engine      *DatabaseEngine `parser:"@@?"`
		OnCluster   *string         `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon   bool            `parser:"';'"`
	}

	// DetachDatabaseStmt represents DETACH DATABASE statements
	// Syntax: DETACH DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC];
	DetachDatabaseStmt struct {
		IfExists    bool    `parser:"'DETACH' 'DATABASE' @('IF' 'EXISTS')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently bool    `parser:"@'PERMANENTLY'?"`
		Sync        bool    `parser:"@'SYNC'?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DropDatabaseStmt represents DROP DATABASE statements
	// Syntax: DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC];
	DropDatabaseStmt struct {
		IfExists  bool    `parser:"'DROP' 'DATABASE' @('IF' 'EXISTS')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync      bool    `parser:"@'SYNC'?"`
		Semicolon bool    `parser:"';'"`
	}

	// RenameDatabaseStmt represents RENAME DATABASE statements
	// Syntax: RENAME DATABASE name1 TO new_name1 [, name2 TO new_name2, ...] [ON CLUSTER cluster];
	RenameDatabaseStmt struct {
		Renames   []*DatabaseRename `parser:"'RENAME' 'DATABASE' @@ (',' @@)*"`
		OnCluster *string           `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon bool              `parser:"';'"`
	}

	// DatabaseRename represents a single database rename operation
	DatabaseRename struct {
		From string `parser:"@(Ident | BacktickIdent)"`
		To   string `parser:"'TO' @(Ident | BacktickIdent)"`
	}
)
