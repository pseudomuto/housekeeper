package parser

type (
	// CreateViewStmt represents a CREATE VIEW statement.
	// Supports both regular views and materialized views.
	// ClickHouse syntax:
	//   CREATE [OR REPLACE] [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]view_name [ON CLUSTER cluster]
	//   [TO [db.]table_name] [ENGINE = engine] [POPULATE]
	//   AS SELECT ...
	CreateViewStmt struct {
		Create       string        `parser:"'CREATE'"`
		OrReplace    bool          `parser:"@('OR' 'REPLACE')?"`
		Materialized bool          `parser:"@'MATERIALIZED'?"`
		View         string        `parser:"'VIEW'"`
		IfNotExists  bool          `parser:"('IF' 'NOT' 'EXISTS')?"`
		Database     *string       `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name         string        `parser:"@(Ident | BacktickIdent)"`
		OnCluster    *string       `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		To           *string       `parser:"('TO' @((Ident | BacktickIdent) ('.' (Ident | BacktickIdent))?))?"`
		Engine       *ViewEngine   `parser:"@@?"`
		Populate     bool          `parser:"@'POPULATE'?"`
		AsSelect     *SelectStatement `parser:"'AS' @@"`
		Semicolon    bool          `parser:"';'"`
	}

	// AttachViewStmt represents an ATTACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   ATTACH VIEW [IF NOT EXISTS] [db.]view_name [ON CLUSTER cluster]
	AttachViewStmt struct {
		Attach      string  `parser:"'ATTACH'"`
		View        string  `parser:"'VIEW'"`
		IfNotExists bool    `parser:"('IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DetachViewStmt represents a DETACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   DETACH VIEW [IF EXISTS] [db.]view_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachViewStmt struct {
		Detach      string  `parser:"'DETACH'"`
		View        string  `parser:"'VIEW'"`
		IfExists    bool    `parser:"('IF' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently bool    `parser:"'PERMANENTLY'?"`
		Sync        bool    `parser:"'SYNC'?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DropViewStmt represents a DROP VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   DROP VIEW [IF EXISTS] [db.]view_name [ON CLUSTER cluster] [SYNC]
	DropViewStmt struct {
		Drop      string  `parser:"'DROP'"`
		View      string  `parser:"'VIEW'"`
		IfExists  bool    `parser:"('IF' 'EXISTS')?"`
		Database  *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync      bool    `parser:"'SYNC'?"`
		Semicolon bool    `parser:"';'"`
	}


	// ViewEngine represents ENGINE = clause for materialized views.
	// This captures everything from ENGINE = until the next major clause (POPULATE, AS, or ;).
	// We use raw string capture because materialized view ENGINE clauses can be complex
	// and may include additional DDL like ORDER BY, PARTITION BY, etc.
	ViewEngine struct {
		Raw string `parser:"'ENGINE' '=' @(~('POPULATE' | 'AS' | ';'))+"`
	}
)
