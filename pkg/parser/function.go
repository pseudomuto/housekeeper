package parser

type (
	// CreateFunctionStmt represents CREATE FUNCTION statements
	// Syntax: CREATE FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression;
	//     or: CREATE FUNCTION name [ON CLUSTER cluster] AS parameter -> expression;
	CreateFunctionStmt struct {
		LeadingComments  []string         `parser:"@(Comment | MultilineComment)*"`
		Name             string           `parser:"'CREATE' 'FUNCTION' @(Ident | BacktickIdent)"`
		OnCluster        *string          `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Parameters       []*FunctionParam `parser:"'AS' ( '(' (@@ (',' @@)*)? ')' | @@ )"`
		Expression       *Expression      `parser:"'->' @@"`
		TrailingComments []string         `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool             `parser:"';'?"`
	}

	// DropFunctionStmt represents DROP FUNCTION statements
	// Syntax: DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster];
	DropFunctionStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		IfExists         bool     `parser:"'DROP' 'FUNCTION' @('IF' 'EXISTS')?"`
		Name             string   `parser:"@(Ident | BacktickIdent)"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
		Semicolon        bool     `parser:"';'"`
	}

	// FunctionParam represents a function parameter
	// Syntax: parameter_name
	FunctionParam struct {
		Name string `parser:"@(Ident | BacktickIdent)"`
	}
)

// GetLeadingComments returns the leading comments for CreateFunctionStmt
func (c *CreateFunctionStmt) GetLeadingComments() []string {
	return c.LeadingComments
}

// GetTrailingComments returns the trailing comments for CreateFunctionStmt
func (c *CreateFunctionStmt) GetTrailingComments() []string {
	return c.TrailingComments
}

// GetLeadingComments returns the leading comments for DropFunctionStmt
func (d *DropFunctionStmt) GetLeadingComments() []string {
	return d.LeadingComments
}

// GetTrailingComments returns the trailing comments for DropFunctionStmt
func (d *DropFunctionStmt) GetTrailingComments() []string {
	return d.TrailingComments
}
