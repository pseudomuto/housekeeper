package parser

// This file contains comprehensive ClickHouse query parsing structures including SELECT statements

type (
	// SelectStatement represents a SELECT statement (for subqueries, no semicolon)
	SelectStatement struct {
		With     *WithClause          `parser:"@@?"`
		Select   string               `parser:"'SELECT'"`
		Distinct bool                 `parser:"@'DISTINCT'?"`
		Columns  []SelectColumn       `parser:"@@ (',' @@)*"`
		From     *FromClause          `parser:"@@?"`
		Where    *WhereClause         `parser:"@@?"`
		GroupBy  *GroupByClause       `parser:"@@?"`
		Having   *HavingClause        `parser:"@@?"`
		OrderBy  *SelectOrderByClause `parser:"@@?"`
		Limit    *LimitClause         `parser:"@@?"`
		Settings *SettingsClause      `parser:"@@?"`
	}

	// TopLevelSelectStatement represents a top-level SELECT statement (requires semicolon)
	TopLevelSelectStatement struct {
		SelectStatement
		Semicolon bool `parser:"';'"`
	}

	// WithClause represents WITH clause for CTEs
	WithClause struct {
		With string                  `parser:"'WITH'"`
		CTEs []CommonTableExpression `parser:"@@ (',' @@)*"`
	}

	// CommonTableExpression represents a single CTE
	CommonTableExpression struct {
		Name  string           `parser:"@(Ident | BacktickIdent)"`
		As    string           `parser:"'AS'"`
		Query *SelectStatement `parser:"'(' @@ ')'"`
	}

	// SelectColumn represents a column in SELECT clause
	SelectColumn struct {
		Star       *string     `parser:"@'*'"`
		Expression *Expression `parser:"| @@"`
		Alias      *string     `parser:"('AS' @(Ident | BacktickIdent))?"`
	}

	// FromClause represents FROM clause with joins
	FromClause struct {
		From  string       `parser:"'FROM'"`
		Table TableRef     `parser:"@@"`
		Joins []JoinClause `parser:"@@*"`
	}

	// TableRef represents a table reference (table, subquery, or function)
	TableRef struct {
		// Table reference with optional alias
		TableName *TableNameWithAlias `parser:"@@"`
		// OR subquery with optional alias
		Subquery *SubqueryWithAlias `parser:"| @@"`
		// OR table function with optional alias
		Function *FunctionWithAlias `parser:"| @@"`
	}

	// TableNameWithAlias represents a table name with optional alias
	TableNameWithAlias struct {
		Database      *string     `parser:"(@(Ident | BacktickIdent) '.')?"`
		Table         string      `parser:"@(Ident | BacktickIdent)"`
		ExplicitAlias *TableAlias `parser:"@@?"`
		ImplicitAlias *string     `parser:"| @(Ident | BacktickIdent)"`
	}

	// TableAlias represents a table alias - requires explicit AS keyword
	TableAlias struct {
		Name *string `parser:"'AS' @(Ident | BacktickIdent)"`
	}

	// SubqueryWithAlias represents a subquery with optional alias
	SubqueryWithAlias struct {
		Subquery SelectStatement `parser:"'(' @@ ')'"`
		Alias    *string         `parser:"('AS' @(Ident | BacktickIdent))?"`
	}

	// FunctionWithAlias represents a table function with optional alias
	FunctionWithAlias struct {
		Function TableFunction `parser:"@@"`
		Alias    *string       `parser:"('AS' @(Ident | BacktickIdent))?"`
	}

	// TableFunction represents table functions like numbers(), remote(), etc.
	TableFunction struct {
		Name      string        `parser:"@(Ident | BacktickIdent)"`
		Arguments []FunctionArg `parser:"'(' (@@ (',' @@)*)? ')'"`
	}

	// JoinClause represents JOIN operations
	JoinClause struct {
		Type      string         `parser:"@('INNER' | 'LEFT' | 'RIGHT' | 'FULL' | 'CROSS')?"`
		Join      string         `parser:"@('JOIN' | 'ARRAY' 'JOIN' | 'GLOBAL' 'JOIN' | 'ASOF' 'JOIN')"`
		Table     TableRef       `parser:"@@"`
		Condition *JoinCondition `parser:"@@?"`
	}

	// JoinCondition represents ON or USING clause in joins
	JoinCondition struct {
		On    *Expression `parser:"'ON' @@"`
		Using []string    `parser:"| 'USING' '(' @(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))* ')'"`
	}

	// WhereClause represents WHERE clause
	WhereClause struct {
		Where     string     `parser:"'WHERE'"`
		Condition Expression `parser:"@@"`
	}

	// GroupByClause represents GROUP BY clause
	GroupByClause struct {
		GroupBy    string       `parser:"'GROUP' 'BY'"`
		All        bool         `parser:"(@'ALL'"`
		Columns    []Expression `parser:"| @@ (',' @@)*)"`
		WithClause *string      `parser:"('WITH' @('CUBE' | 'ROLLUP' | 'TOTALS'))?"`
	}

	// HavingClause represents HAVING clause
	HavingClause struct {
		Having    string     `parser:"'HAVING'"`
		Condition Expression `parser:"@@"`
	}

	// SelectOrderByClause represents ORDER BY clause in SELECT statements
	SelectOrderByClause struct {
		OrderBy     string             `parser:"'ORDER' 'BY'"`
		Columns     []OrderByColumn    `parser:"@@ (',' @@)*"`
		Interpolate *InterpolateClause `parser:"@@?"`
	}

	// InterpolateClause represents INTERPOLATE clause for WITH FILL
	InterpolateClause struct {
		Interpolate string              `parser:"'INTERPOLATE'"`
		Columns     []InterpolateColumn `parser:"('(' (@@ (',' @@)*)? ')')?"`
	}

	// InterpolateColumn represents a column in INTERPOLATE clause
	InterpolateColumn struct {
		Name       string      `parser:"@(Ident | BacktickIdent)"`
		Expression *Expression `parser:"('AS' @@)?"`
	}

	// OrderByColumn represents a single column in ORDER BY
	OrderByColumn struct {
		Expression    Expression  `parser:"@@"`
		Direction     *string     `parser:"@('ASC' | 'DESC')?"`
		Nulls         *string     `parser:"('NULLS' @('FIRST' | 'LAST'))?"`
		Collate       *string     `parser:"('COLLATE' @String)?"`
		WithFill      bool        `parser:"@('WITH' 'FILL')?"`
		FillFrom      *Expression `parser:"('FROM' @@)?"`
		FillTo        *Expression `parser:"('TO' @@)?"`
		FillStep      *Expression `parser:"('STEP' @@)?"`
		FillStaleness *Expression `parser:"('STALENESS' @@)?"`
	}

	// LimitClause represents LIMIT clause
	LimitClause struct {
		Limit  string        `parser:"'LIMIT'"`
		Count  Expression    `parser:"@@"`
		Offset *OffsetClause `parser:"@@?"`
		By     *LimitBy      `parser:"@@?"`
	}

	// OffsetClause represents OFFSET clause
	OffsetClause struct {
		Offset string     `parser:"'OFFSET'"`
		Value  Expression `parser:"@@"`
	}

	// LimitBy represents LIMIT ... BY clause
	LimitBy struct {
		By      string       `parser:"'BY'"`
		Columns []Expression `parser:"@@ (',' @@)*"`
	}

	// SettingsClause represents SETTINGS clause
	SettingsClause struct {
		Settings string               `parser:"'SETTINGS'"`
		Values   []SettingsAssignment `parser:"@@ (',' @@)*"`
	}

	// SettingsAssignment represents key=value in SETTINGS
	SettingsAssignment struct {
		Key   string     `parser:"@(Ident | BacktickIdent)"`
		Value Expression `parser:"'=' @@"`
	}
)
