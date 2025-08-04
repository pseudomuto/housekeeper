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
		Database     *string       `parser:"(@Ident '.')?"`
		Name         string        `parser:"@Ident"`
		OnCluster    *string       `parser:"('ON' 'CLUSTER' @Ident)?"`
		To           *string       `parser:"('TO' @(Ident ('.' Ident)?))?"`
		Engine       *ViewEngine   `parser:"@@?"`
		Populate     bool          `parser:"@'POPULATE'?"`
		AsSelect     *SelectClause `parser:"'AS' @@"`
		Semicolon    bool          `parser:"';'"`
	}

	// AttachViewStmt represents an ATTACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   ATTACH VIEW [IF NOT EXISTS] [db.]view_name [ON CLUSTER cluster]
	AttachViewStmt struct {
		Attach      string  `parser:"'ATTACH'"`
		View        string  `parser:"'VIEW'"`
		IfNotExists bool    `parser:"('IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"(@Ident '.')?"`
		Name        string  `parser:"@Ident"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DetachViewStmt represents a DETACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   DETACH VIEW [IF EXISTS] [db.]view_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachViewStmt struct {
		Detach      string  `parser:"'DETACH'"`
		View        string  `parser:"'VIEW'"`
		IfExists    bool    `parser:"('IF' 'EXISTS')?"`
		Database    *string `parser:"(@Ident '.')?"`
		Name        string  `parser:"@Ident"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)?"`
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
		Database  *string `parser:"(@Ident '.')?"`
		Name      string  `parser:"@Ident"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Sync      bool    `parser:"'SYNC'?"`
		Semicolon bool    `parser:"';'"`
	}

	// SelectClause represents a SELECT statement in a view definition.
	// This captures the entire SELECT query as a raw string.
	// We use a simple approach of capturing everything that's not a semicolon.
	SelectClause struct {
		Raw string `parser:"@(~';')+"`
	}

	// ViewEngine represents ENGINE = clause for materialized views.
	// This captures everything from ENGINE = until the next major clause (POPULATE, AS, or ;).
	// We use raw string capture because materialized view ENGINE clauses can be complex
	// and may include additional DDL like ORDER BY, PARTITION BY, etc.
	ViewEngine struct {
		Raw string `parser:"'ENGINE' '=' @(~('POPULATE' | 'AS' | ';'))+"`
	}
)
