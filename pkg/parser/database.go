package parser

type (
	// CreateDatabaseStmt represents CREATE DATABASE statements
	// Syntax: CREATE DATABASE [IF NOT EXISTS] db_name [ON CLUSTER cluster] [ENGINE = engine(...)] [COMMENT 'Comment'];
	CreateDatabaseStmt struct {
		LeadingComments  []string        `parser:"@(Comment | MultilineComment)*"`
		Create           string          `parser:"'CREATE'"`
		Database         string          `parser:"'DATABASE'"`
		IfNotExists      bool            `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name             string          `parser:"@(Ident | BacktickIdent)"`
		OnCluster        *string         `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Engine           *DatabaseEngine `parser:"@@?"`
		Comment          *string         `parser:"('COMMENT' @String)?"`
		TrailingComments []string        `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool            `parser:"';'"`
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
		LeadingComments  []string             `parser:"@(Comment | MultilineComment)*"`
		Alter            string               `parser:"'ALTER'"`
		Database         string               `parser:"'DATABASE'"`
		Name             string               `parser:"@(Ident | BacktickIdent)"`
		OnCluster        *string              `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Action           *AlterDatabaseAction `parser:"@@"`
		TrailingComments []string             `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool                 `parser:"';'"`
	}

	// AlterDatabaseAction represents the action to perform on the database
	AlterDatabaseAction struct {
		ModifyComment *string `parser:"'MODIFY' 'COMMENT' @String"`
	}

	// AttachDatabaseStmt represents ATTACH DATABASE statements
	// Syntax: ATTACH DATABASE [IF NOT EXISTS] name [ENGINE = engine(...)] [ON CLUSTER cluster];
	AttachDatabaseStmt struct {
		LeadingComments  []string        `parser:"@(Comment | MultilineComment)*"`
		Attach           string          `parser:"'ATTACH'"`
		Database         string          `parser:"'DATABASE'"`
		IfNotExists      bool            `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name             string          `parser:"@(Ident | BacktickIdent)"`
		Engine           *DatabaseEngine `parser:"@@?"`
		OnCluster        *string         `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingComments []string        `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool            `parser:"';'"`
	}

	// DetachDatabaseStmt represents DETACH DATABASE statements
	// Syntax: DETACH DATABASE [IF EXISTS] [db.]name [ON CLUSTER cluster] [PERMANENTLY] [SYNC];
	DetachDatabaseStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		Detach           string   `parser:"'DETACH'"`
		Database         string   `parser:"'DATABASE'"`
		IfExists         bool     `parser:"@('IF' 'EXISTS')?"`
		Name             string   `parser:"@(Ident | BacktickIdent)"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently      bool     `parser:"@'PERMANENTLY'?"`
		Sync             bool     `parser:"@'SYNC'?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool     `parser:"';'"`
	}

	// DropDatabaseStmt represents DROP DATABASE statements
	// Syntax: DROP DATABASE [IF EXISTS] db [ON CLUSTER cluster] [SYNC];
	DropDatabaseStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		Drop             string   `parser:"'DROP'"`
		Database         string   `parser:"'DATABASE'"`
		IfExists         bool     `parser:"@('IF' 'EXISTS')?"`
		Name             string   `parser:"@(Ident | BacktickIdent)"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync             bool     `parser:"@'SYNC'?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool     `parser:"';'"`
	}

	// RenameDatabaseStmt represents RENAME DATABASE statements
	// Syntax: RENAME DATABASE name1 TO new_name1 [, name2 TO new_name2, ...] [ON CLUSTER cluster];
	RenameDatabaseStmt struct {
		LeadingComments  []string          `parser:"@(Comment | MultilineComment)*"`
		Rename           string            `parser:"'RENAME'"`
		Database         string            `parser:"'DATABASE'"`
		Renames          []*DatabaseRename `parser:"@@ (',' @@)*"`
		OnCluster        *string           `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingComments []string          `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool              `parser:"';'"`
	}

	// DatabaseRename represents a single database rename operation
	DatabaseRename struct {
		From string `parser:"@(Ident | BacktickIdent)"`
		To   string `parser:"'TO' @(Ident | BacktickIdent)"`
	}
)

// GetLeadingComments returns the leading comments for CreateDatabaseStmt
func (s *CreateDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for CreateDatabaseStmt
func (s *CreateDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}

// GetLeadingComments returns the leading comments for AlterDatabaseStmt
func (s *AlterDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for AlterDatabaseStmt
func (s *AlterDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}

// GetLeadingComments returns the leading comments for AttachDatabaseStmt
func (s *AttachDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for AttachDatabaseStmt
func (s *AttachDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}

// GetLeadingComments returns the leading comments for DetachDatabaseStmt
func (s *DetachDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for DetachDatabaseStmt
func (s *DetachDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}

// GetLeadingComments returns the leading comments for DropDatabaseStmt
func (s *DropDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for DropDatabaseStmt
func (s *DropDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}

// GetLeadingComments returns the leading comments for RenameDatabaseStmt
func (s *RenameDatabaseStmt) GetLeadingComments() []string {
	return s.LeadingComments
}

// GetTrailingComments returns the trailing comments for RenameDatabaseStmt
func (s *RenameDatabaseStmt) GetTrailingComments() []string {
	return s.TrailingComments
}
