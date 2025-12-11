package parser

type (
	// ViewTableTarget represents a table target in TO clause of materialized view
	// Can be either:
	//   - Simple table reference: [db.]table_name
	//   - Table function: functionName(args...)
	ViewTableTarget struct {
		// Try table function first (has parentheses to distinguish it)
		Function *TableFunction `parser:"@@"`
		// Fall back to table reference if no function call syntax found
		Database *string `parser:"| ((@(Ident | BacktickIdent) '.')?"`
		Table    *string `parser:"@(Ident | BacktickIdent))"`
	}

	// CreateViewStmt represents a CREATE VIEW statement.
	// Supports both regular views and materialized views.
	// ClickHouse syntax:
	//   CREATE [OR REPLACE] [MATERIALIZED] VIEW [IF NOT EXISTS] [db.]view_name [ON CLUSTER cluster]
	//   [TO [db.]table_name] [ENGINE = engine] [POPULATE]
	//   AS SELECT ...
	CreateViewStmt struct {
		LeadingCommentField
		Create       string           `parser:"'CREATE'"`
		OrReplace    bool             `parser:"@('OR' 'REPLACE')?"`
		Materialized bool             `parser:"@'MATERIALIZED'?"`
		View         string           `parser:"'VIEW'"`
		IfNotExists  bool             `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Database     *string          `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name         string           `parser:"@(Ident | BacktickIdent)"`
		OnCluster    *string          `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		To           *ViewTableTarget `parser:"('TO' @@)?"`
		Engine       *ViewEngine      `parser:"@@?"`
		Populate     bool             `parser:"@'POPULATE'?"`
		AsSelect     *SelectStatement `parser:"'AS' @@"`
		TrailingCommentField
		Semicolon bool `parser:"';'"`
	}

	// AttachViewStmt represents an ATTACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   ATTACH VIEW [IF NOT EXISTS] [db.]view_name [ON CLUSTER cluster]
	AttachViewStmt struct {
		LeadingCommentField
		Attach      string  `parser:"'ATTACH'"`
		View        string  `parser:"'VIEW'"`
		IfNotExists bool    `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingCommentField
		Semicolon bool `parser:"';'"`
	}

	// DetachViewStmt represents a DETACH VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   DETACH VIEW [IF EXISTS] [db.]view_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachViewStmt struct {
		LeadingCommentField
		Detach      string  `parser:"'DETACH'"`
		View        string  `parser:"'VIEW'"`
		IfExists    bool    `parser:"@('IF' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently bool    `parser:"@'PERMANENTLY'?"`
		Sync        bool    `parser:"@'SYNC'?"`
		TrailingCommentField
		Semicolon bool `parser:"';'"`
	}

	// DropViewStmt represents a DROP VIEW statement (for regular views only).
	// ClickHouse syntax:
	//   DROP VIEW [IF EXISTS] [db.]view_name [ON CLUSTER cluster] [SYNC]
	DropViewStmt struct {
		LeadingCommentField
		Drop      string  `parser:"'DROP'"`
		View      string  `parser:"'VIEW'"`
		IfExists  bool    `parser:"@('IF' 'EXISTS')?"`
		Database  *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync      bool    `parser:"@'SYNC'?"`
		TrailingCommentField
		Semicolon bool `parser:"';'"`
	}

	// ViewEngine represents ENGINE = clause for materialized views.
	// Materialized views can have ENGINE clauses with additional DDL like ORDER BY.
	// We structure this similar to table engines but with optional materialized view specific clauses.
	ViewEngine struct {
		Engine      string            `parser:"'ENGINE' '='"`
		Name        string            `parser:"@(Ident | BacktickIdent)"`
		Parameters  []EngineParameter `parser:"('(' (@@ (',' @@)*)? ')')?"`
		OrderBy     *ViewOrderBy      `parser:"@@?"`
		PartitionBy *ViewPartitionBy  `parser:"@@?"`
		PrimaryKey  *ViewPrimaryKey   `parser:"@@?"`
		SampleBy    *ViewSampleBy     `parser:"@@?"`
	}

	// ViewOrderBy represents ORDER BY in materialized view ENGINE clause
	ViewOrderBy struct {
		OrderBy    string     `parser:"'ORDER' 'BY'"`
		Expression Expression `parser:"@@"`
	}

	// ViewPartitionBy represents PARTITION BY in materialized view ENGINE clause
	ViewPartitionBy struct {
		PartitionBy string     `parser:"'PARTITION' 'BY'"`
		Expression  Expression `parser:"@@"`
	}

	// ViewPrimaryKey represents PRIMARY KEY in materialized view ENGINE clause
	ViewPrimaryKey struct {
		PrimaryKey string     `parser:"'PRIMARY' 'KEY'"`
		Expression Expression `parser:"@@"`
	}

	// ViewSampleBy represents SAMPLE BY in materialized view ENGINE clause
	ViewSampleBy struct {
		SampleBy   string     `parser:"'SAMPLE' 'BY'"`
		Expression Expression `parser:"@@"`
	}
)
