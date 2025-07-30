package parser

type (
	// AttachTableStmt represents an ATTACH TABLE statement.
	// Used for materialized views: ATTACH TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   ATTACH TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
	AttachTableStmt struct {
		Attach      string  `parser:"'ATTACH' 'TABLE'"`
		IfNotExists bool    `parser:"('IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"(@Ident '.')?"`
		Name        string  `parser:"@Ident"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DetachTableStmt represents a DETACH TABLE statement.
	// Used for materialized views: DETACH TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   DETACH TABLE [IF EXISTS] [db.]table_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachTableStmt struct {
		Detach      string  `parser:"'DETACH' 'TABLE'"`
		IfExists    bool    `parser:"('IF' 'EXISTS')?"`
		Database    *string `parser:"(@Ident '.')?"`
		Name        string  `parser:"@Ident"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Permanently bool    `parser:"@'PERMANENTLY'?"`
		Sync        bool    `parser:"@'SYNC'?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DropTableStmt represents a DROP TABLE statement.
	// Used for materialized views: DROP TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   DROP TABLE [IF EXISTS] [db.]table_name [ON CLUSTER cluster] [SYNC]
	DropTableStmt struct {
		Drop      string  `parser:"'DROP' 'TABLE'"`
		IfExists  bool    `parser:"('IF' 'EXISTS')?"`
		Database  *string `parser:"(@Ident '.')?"`
		Name      string  `parser:"@Ident"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Sync      bool    `parser:"@'SYNC'?"`
		Semicolon bool    `parser:"';'"`
	}

	// RenameTableStmt represents a RENAME TABLE statement.
	// Used for both regular views and materialized views.
	// ClickHouse syntax:
	//   RENAME TABLE [db.]table1 TO [db.]table2, [db.]table3 TO [db.]table4, ... [ON CLUSTER cluster]
	RenameTableStmt struct {
		Rename    string         `parser:"'RENAME' 'TABLE'"`
		Renames   []TableRename  `parser:"@@ (',' @@)*"`
		OnCluster *string        `parser:"('ON' 'CLUSTER' @Ident)?"`
		Semicolon bool           `parser:"';'"`
	}

	// TableRename represents a single table rename operation
	TableRename struct {
		FromDatabase *string `parser:"(@Ident '.')?"`
		FromName     string  `parser:"@Ident"`
		To           string  `parser:"'TO'"`
		ToDatabase   *string `parser:"(@Ident '.')?"`
		ToName       string  `parser:"@Ident"`
	}
)