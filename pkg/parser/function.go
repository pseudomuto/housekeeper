package parser

type (
	// CreateFunctionStmt represents CREATE FUNCTION statements
	// Syntax: CREATE FUNCTION name [ON CLUSTER cluster] AS (parameter0, ...) -> expression;
	CreateFunctionStmt struct {
		Name       string           `parser:"'CREATE' 'FUNCTION' @(Ident | BacktickIdent)"`
		OnCluster  *string          `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Parameters []*FunctionParam `parser:"'AS' '(' (@@ (',' @@)*)? ')'"`
		Expression *Expression      `parser:"'->' @@"`
		Semicolon  bool             `parser:"';'"`
	}

	// DropFunctionStmt represents DROP FUNCTION statements
	// Syntax: DROP FUNCTION [IF EXISTS] name [ON CLUSTER cluster];
	DropFunctionStmt struct {
		IfExists  bool    `parser:"'DROP' 'FUNCTION' @('IF' 'EXISTS')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon bool    `parser:"';'"`
	}

	// FunctionParam represents a function parameter
	// Syntax: parameter_name
	FunctionParam struct {
		Name string `parser:"@(Ident | BacktickIdent)"`
	}
)
