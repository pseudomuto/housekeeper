package schema

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// removeQuotes removes surrounding single quotes from a string
func removeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}

	return s
}

// escapeSQL escapes single quotes in SQL strings
func escapeSQL(s string) string {
	return strings.ReplaceAll(s, "'", "\\'")
}

// formatEngine formats a database engine with its parameters
func formatEngine(engine *parser.DatabaseEngine) string {
	if len(engine.Parameters) == 0 {
		return engine.Name
	}

	params := make([]string, len(engine.Parameters))
	for i, param := range engine.Parameters {
		params[i] = param.Value
	}

	return engine.Name + "(" + strings.Join(params, ", ") + ")"
}

// getStringValue safely gets a string value from a string pointer
func getStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// normalizeIdentifier removes surrounding backticks from ClickHouse identifiers
// for consistent comparison between parsed DDL and ClickHouse system table output
func normalizeIdentifier(s string) string {
	if len(s) >= 2 && s[0] == '`' && s[len(s)-1] == '`' {
		return s[1 : len(s)-1]
	}
	return s
}

// AST-based expression comparison functions

// expressionsAreEqual compares expressions using AST-based structural comparison
func expressionsAreEqual(expr1, expr2 *parser.Expression) bool {
	if expr1 == nil && expr2 == nil {
		return true
	}
	if expr1 == nil || expr2 == nil {
		return false
	}

	// Compare AST structure directly
	if expr1.Case != nil && expr2.Case != nil {
		return caseExpressionsEqual(expr1.Case, expr2.Case)
	}
	if expr1.Case != nil || expr2.Case != nil {
		return false
	}

	if expr1.Or != nil && expr2.Or != nil {
		return orExpressionsEqual(expr1.Or, expr2.Or)
	}
	if expr1.Or != nil || expr2.Or != nil {
		return false
	}

	return true
}

// caseExpressionsEqual compares CASE expressions structurally
func caseExpressionsEqual(case1, case2 *parser.CaseExpression) bool {
	if case1 == nil && case2 == nil {
		return true
	}
	if case1 == nil || case2 == nil {
		return false
	}

	// Compare WHEN clauses
	if len(case1.WhenClauses) != len(case2.WhenClauses) {
		return false
	}
	for i, when1 := range case1.WhenClauses {
		when2 := case2.WhenClauses[i]
		if !whenClausesEqual(&when1, &when2) {
			return false
		}
	}

	// Compare ELSE clause
	if case1.ElseClause == nil && case2.ElseClause == nil {
		return true
	}
	if case1.ElseClause == nil || case2.ElseClause == nil {
		return false
	}
	return elseClausesEqual(case1.ElseClause, case2.ElseClause)
}

// whenClausesEqual compares WHEN clauses (using string comparison for now)
func whenClausesEqual(when1, when2 *parser.WhenClause) bool {
	if when1 == nil && when2 == nil {
		return true
	}
	if when1 == nil || when2 == nil {
		return false
	}

	// For now, use string comparison on the parsed string values
	// This could be enhanced to parse the condition and result as expressions
	return strings.TrimSpace(when1.Condition) == strings.TrimSpace(when2.Condition) &&
		strings.TrimSpace(when1.Result) == strings.TrimSpace(when2.Result)
}

// elseClausesEqual compares ELSE clauses (using string comparison for now)
func elseClausesEqual(else1, else2 *parser.ElseClause) bool {
	if else1 == nil && else2 == nil {
		return true
	}
	if else1 == nil || else2 == nil {
		return false
	}

	// For now, use string comparison on the parsed string value
	// This could be enhanced to parse the result as an expression
	return strings.TrimSpace(else1.Result) == strings.TrimSpace(else2.Result)
}

// orExpressionsEqual compares OR expressions structurally
func orExpressionsEqual(or1, or2 *parser.OrExpression) bool {
	if or1 == nil && or2 == nil {
		return true
	}
	if or1 == nil || or2 == nil {
		return false
	}

	// Compare base AND expression
	if !andExpressionsEqual(or1.And, or2.And) {
		return false
	}

	// Compare OR rest clauses
	if len(or1.Rest) != len(or2.Rest) {
		return false
	}
	for i, rest1 := range or1.Rest {
		rest2 := or2.Rest[i]
		if rest1.Op != rest2.Op || !andExpressionsEqual(rest1.And, rest2.And) {
			return false
		}
	}

	return true
}

// andExpressionsEqual compares AND expressions structurally
func andExpressionsEqual(and1, and2 *parser.AndExpression) bool {
	if and1 == nil && and2 == nil {
		return true
	}
	if and1 == nil || and2 == nil {
		return false
	}

	// Compare base NOT expression
	if !notExpressionsEqual(and1.Not, and2.Not) {
		return false
	}

	// Compare AND rest clauses
	if len(and1.Rest) != len(and2.Rest) {
		return false
	}
	for i, rest1 := range and1.Rest {
		rest2 := and2.Rest[i]
		if rest1.Op != rest2.Op || !notExpressionsEqual(rest1.Not, rest2.Not) {
			return false
		}
	}

	return true
}

// notExpressionsEqual compares NOT expressions structurally
func notExpressionsEqual(not1, not2 *parser.NotExpression) bool {
	if not1 == nil && not2 == nil {
		return true
	}
	if not1 == nil || not2 == nil {
		return false
	}

	// Compare NOT flag and comparison
	return not1.Not == not2.Not && comparisonExpressionsEqual(not1.Comparison, not2.Comparison)
}

// comparisonExpressionsEqual compares comparison expressions structurally
func comparisonExpressionsEqual(comp1, comp2 *parser.ComparisonExpression) bool {
	if comp1 == nil && comp2 == nil {
		return true
	}
	if comp1 == nil || comp2 == nil {
		return false
	}

	// Compare base addition expression
	if !additionExpressionsEqual(comp1.Addition, comp2.Addition) {
		return false
	}

	// Compare comparison rest (operations)
	if comp1.Rest == nil && comp2.Rest == nil {
		return true
	}
	if comp1.Rest == nil || comp2.Rest == nil {
		return false
	}

	// Compare simple operations
	if comp1.Rest.SimpleOp != nil && comp2.Rest.SimpleOp != nil {
		return simpleComparisonsEqual(comp1.Rest.SimpleOp, comp2.Rest.SimpleOp)
	}
	if comp1.Rest.SimpleOp != nil || comp2.Rest.SimpleOp != nil {
		return false
	}

	// Compare IN operations
	if comp1.Rest.InOp != nil && comp2.Rest.InOp != nil {
		return inComparisonsEqual(comp1.Rest.InOp, comp2.Rest.InOp)
	}
	if comp1.Rest.InOp != nil || comp2.Rest.InOp != nil {
		return false
	}

	// Compare BETWEEN operations
	if comp1.Rest.BetweenOp != nil && comp2.Rest.BetweenOp != nil {
		return betweenComparisonsEqual(comp1.Rest.BetweenOp, comp2.Rest.BetweenOp)
	}

	return comp1.Rest.BetweenOp == nil && comp2.Rest.BetweenOp == nil
}

// simpleComparisonsEqual compares simple comparison operations
func simpleComparisonsEqual(comp1, comp2 *parser.SimpleComparison) bool {
	if comp1 == nil && comp2 == nil {
		return true
	}
	if comp1 == nil || comp2 == nil {
		return false
	}

	// Compare operators
	if !simpleComparisonOpsEqual(comp1.Op, comp2.Op) {
		return false
	}

	// Compare addition expressions
	return additionExpressionsEqual(comp1.Addition, comp2.Addition)
}

// simpleComparisonOpsEqual compares simple comparison operators
func simpleComparisonOpsEqual(op1, op2 *parser.SimpleComparisonOp) bool {
	if op1 == nil && op2 == nil {
		return true
	}
	if op1 == nil || op2 == nil {
		return false
	}

	return op1.Eq == op2.Eq &&
		op1.NotEq == op2.NotEq &&
		op1.LtEq == op2.LtEq &&
		op1.GtEq == op2.GtEq &&
		op1.Lt == op2.Lt &&
		op1.Gt == op2.Gt &&
		op1.Like == op2.Like &&
		op1.NotLike == op2.NotLike
}

// inComparisonsEqual compares IN operations
func inComparisonsEqual(in1, in2 *parser.InComparison) bool {
	if in1 == nil && in2 == nil {
		return true
	}
	if in1 == nil || in2 == nil {
		return false
	}

	return in1.Not == in2.Not && in1.In == in2.In
	// Note: InExpression comparison would need to be implemented based on the actual structure
}

// betweenComparisonsEqual compares BETWEEN operations
func betweenComparisonsEqual(between1, between2 *parser.BetweenComparison) bool {
	if between1 == nil && between2 == nil {
		return true
	}
	if between1 == nil || between2 == nil {
		return false
	}

	return between1.Not == between2.Not && between1.Between == between2.Between
	// Note: BetweenExpression comparison would need to be implemented based on the actual structure
}

// additionExpressionsEqual compares addition/subtraction expressions
func additionExpressionsEqual(add1, add2 *parser.AdditionExpression) bool {
	if add1 == nil && add2 == nil {
		return true
	}
	if add1 == nil || add2 == nil {
		return false
	}

	// Compare base multiplication expression
	if !multiplicationExpressionsEqual(add1.Multiplication, add2.Multiplication) {
		return false
	}

	// Compare addition rest clauses
	if len(add1.Rest) != len(add2.Rest) {
		return false
	}
	for i, rest1 := range add1.Rest {
		rest2 := add2.Rest[i]
		if rest1.Op != rest2.Op || !multiplicationExpressionsEqual(rest1.Multiplication, rest2.Multiplication) {
			return false
		}
	}

	return true
}

// multiplicationExpressionsEqual compares multiplication/division expressions
func multiplicationExpressionsEqual(mul1, mul2 *parser.MultiplicationExpression) bool {
	if mul1 == nil && mul2 == nil {
		return true
	}
	if mul1 == nil || mul2 == nil {
		return false
	}

	// Compare base unary expression
	if !unaryExpressionsEqual(mul1.Unary, mul2.Unary) {
		return false
	}

	// Compare multiplication rest clauses
	if len(mul1.Rest) != len(mul2.Rest) {
		return false
	}
	for i, rest1 := range mul1.Rest {
		rest2 := mul2.Rest[i]
		if rest1.Op != rest2.Op || !unaryExpressionsEqual(rest1.Unary, rest2.Unary) {
			return false
		}
	}

	return true
}

// unaryExpressionsEqual compares unary expressions
func unaryExpressionsEqual(unary1, unary2 *parser.UnaryExpression) bool {
	if unary1 == nil && unary2 == nil {
		return true
	}
	if unary1 == nil || unary2 == nil {
		return false
	}

	return unary1.Op == unary2.Op && primaryExpressionsEqual(unary1.Primary, unary2.Primary)
}

// primaryExpressionsEqual compares primary expressions
func primaryExpressionsEqual(prim1, prim2 *parser.PrimaryExpression) bool {
	if prim1 == nil && prim2 == nil {
		return true
	}
	if prim1 == nil || prim2 == nil {
		return false
	}

	// Compare literals
	if prim1.Literal != nil && prim2.Literal != nil {
		return literalsEqual(prim1.Literal, prim2.Literal)
	}
	if prim1.Literal != nil || prim2.Literal != nil {
		return false
	}

	// Compare identifiers
	if prim1.Identifier != nil && prim2.Identifier != nil {
		return identifiersEqual(prim1.Identifier, prim2.Identifier)
	}
	if prim1.Identifier != nil || prim2.Identifier != nil {
		return false
	}

	// Compare function calls
	if prim1.Function != nil && prim2.Function != nil {
		return functionCallsEqual(prim1.Function, prim2.Function)
	}
	if prim1.Function != nil || prim2.Function != nil {
		return false
	}

	// Compare parentheses expressions
	if prim1.Parentheses != nil && prim2.Parentheses != nil {
		return expressionsAreEqual(&prim1.Parentheses.Expression, &prim2.Parentheses.Expression)
	}
	if prim1.Parentheses != nil || prim2.Parentheses != nil {
		return false
	}

	// For other expression types (Interval, Extract, Cast, Tuple, Array),
	// we can add more detailed comparisons as needed
	return true
}

// literalsEqual compares literal values
func literalsEqual(lit1, lit2 *parser.Literal) bool {
	if lit1 == nil && lit2 == nil {
		return true
	}
	if lit1 == nil || lit2 == nil {
		return false
	}

	// Compare string values
	if lit1.StringValue != nil && lit2.StringValue != nil {
		return *lit1.StringValue == *lit2.StringValue
	}
	if lit1.StringValue != nil || lit2.StringValue != nil {
		return false
	}

	// Compare numbers
	if lit1.Number != nil && lit2.Number != nil {
		return *lit1.Number == *lit2.Number
	}
	if lit1.Number != nil || lit2.Number != nil {
		return false
	}

	// Compare booleans
	if lit1.Boolean != nil && lit2.Boolean != nil {
		return *lit1.Boolean == *lit2.Boolean
	}
	if lit1.Boolean != nil || lit2.Boolean != nil {
		return false
	}

	// Compare NULL
	return lit1.Null == lit2.Null
}

// identifiersEqual compares identifier expressions
func identifiersEqual(id1, id2 *parser.IdentifierExpr) bool {
	if id1 == nil && id2 == nil {
		return true
	}
	if id1 == nil || id2 == nil {
		return false
	}

	// Compare database qualifier
	if id1.Database != nil && id2.Database != nil {
		if *id1.Database != *id2.Database {
			return false
		}
	} else if id1.Database != nil || id2.Database != nil {
		return false
	}

	// Compare table qualifier
	if id1.Table != nil && id2.Table != nil {
		if *id1.Table != *id2.Table {
			return false
		}
	} else if id1.Table != nil || id2.Table != nil {
		return false
	}

	// Compare column name (case-insensitive for ClickHouse)
	return strings.EqualFold(id1.Name, id2.Name)
}

// functionCallsEqual compares function calls including arguments
func functionCallsEqual(func1, func2 *parser.FunctionCall) bool {
	if func1 == nil && func2 == nil {
		return true
	}
	if func1 == nil || func2 == nil {
		return false
	}

	// Compare function names (case-insensitive for ClickHouse)
	if !strings.EqualFold(func1.Name, func2.Name) {
		return false
	}

	// Compare first parentheses argument lists
	if len(func1.FirstParentheses) != len(func2.FirstParentheses) {
		return false
	}

	for i, arg1 := range func1.FirstParentheses {
		arg2 := func2.FirstParentheses[i]
		if !functionArgsEqual(&arg1, &arg2) {
			return false
		}
	}

	// Compare second parentheses argument lists (for parameterized functions)
	if len(func1.SecondParentheses) != len(func2.SecondParentheses) {
		return false
	}

	for i, arg1 := range func1.SecondParentheses {
		arg2 := func2.SecondParentheses[i]
		if !functionArgsEqual(&arg1, &arg2) {
			return false
		}
	}

	return true
}

// engineParametersEqual compares engine parameters
func engineParametersEqual(param1, param2 *parser.EngineParameter) bool {
	if param1 == nil && param2 == nil {
		return true
	}
	if param1 == nil || param2 == nil {
		return false
	}

	// Compare expressions
	if param1.Expression != nil && param2.Expression != nil {
		return expressionsAreEqual(param1.Expression, param2.Expression)
	}
	if param1.Expression != nil || param2.Expression != nil {
		return false
	}

	// Compare string values
	if param1.String != nil && param2.String != nil {
		return *param1.String == *param2.String
	}
	if param1.String != nil || param2.String != nil {
		return false
	}

	// Compare number values
	if param1.Number != nil && param2.Number != nil {
		return *param1.Number == *param2.Number
	}
	if param1.Number != nil || param2.Number != nil {
		return false
	}

	// Compare identifier values
	if param1.Ident != nil && param2.Ident != nil {
		return strings.EqualFold(*param1.Ident, *param2.Ident)
	}
	if param1.Ident != nil || param2.Ident != nil {
		return false
	}

	return true
}

// functionArgsEqual compares function arguments (can be * or expressions)
func functionArgsEqual(arg1, arg2 *parser.FunctionArg) bool {
	if arg1 == nil && arg2 == nil {
		return true
	}
	if arg1 == nil || arg2 == nil {
		return false
	}

	// Compare star arguments
	if arg1.Star != nil && arg2.Star != nil {
		return *arg1.Star == *arg2.Star
	}
	if arg1.Star != nil || arg2.Star != nil {
		return false
	}

	// Compare expression arguments
	if arg1.Expression != nil && arg2.Expression != nil {
		return expressionsAreEqual(arg1.Expression, arg2.Expression)
	}
	if arg1.Expression != nil || arg2.Expression != nil {
		return false
	}

	return true
}
