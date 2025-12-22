package schema

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// removeQuotes removes surrounding single quotes from a string
func removeQuotes(s string) string {
	if len(s) >= 2 && s[0] == '\'' && s[len(s)-1] == '\'' {
		return s[1 : len(s)-1]
	}

	return s
}

// getStringValue safely gets a string value from a string pointer
func getStringValue(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// getViewTableTargetValue converts ViewTableTarget to string representation
// with normalized identifiers (backticks removed) for consistent comparison
func getViewTableTargetValue(target *parser.ViewTableTarget) string {
	if target == nil {
		return ""
	}

	if target.Function != nil {
		// Format table function as string
		result := normalizeIdentifier(target.Function.Name) + "("
		var args []string
		for _, arg := range target.Function.Arguments {
			if arg.Star != nil {
				args = append(args, "*")
			} else if arg.Expression != nil {
				args = append(args, arg.Expression.String())
			}
		}
		result += strings.Join(args, ", ") + ")"
		return result
	} else if target.Table != nil {
		if target.Database != nil {
			return normalizeIdentifier(*target.Database) + "." + normalizeIdentifier(*target.Table)
		}
		return normalizeIdentifier(*target.Table)
	}

	return ""
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
	if eq, needsMoreChecks := compare.NilCheck(expr1, expr2); !needsMoreChecks {
		return eq
	}

	// Compare AST structure directly
	return compare.PointersWithEqual(expr1.Case, expr2.Case, caseExpressionsEqual) &&
		compare.PointersWithEqual(expr1.Or, expr2.Or, orExpressionsEqual)
}

// caseExpressionsEqual compares CASE expressions structurally
func caseExpressionsEqual(case1, case2 *parser.CaseExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(case1, case2); !needsMoreChecks {
		return eq
	}

	// Compare WHEN clauses
	return compare.Slices(case1.WhenClauses, case2.WhenClauses, func(a, b parser.WhenClause) bool {
		return whenClausesEqual(&a, &b)
	}) && compare.PointersWithEqual(case1.ElseClause, case2.ElseClause, elseClausesEqual)
}

// whenClausesEqual compares WHEN clauses (using string comparison for now)
func whenClausesEqual(when1, when2 *parser.WhenClause) bool {
	if eq, needsMoreChecks := compare.NilCheck(when1, when2); !needsMoreChecks {
		return eq
	}

	// For now, use string comparison on the parsed string values
	// This could be enhanced to parse the condition and result as expressions
	return strings.TrimSpace(when1.Condition) == strings.TrimSpace(when2.Condition) &&
		strings.TrimSpace(when1.Result) == strings.TrimSpace(when2.Result)
}

// elseClausesEqual compares ELSE clauses (using string comparison for now)
func elseClausesEqual(else1, else2 *parser.ElseClause) bool {
	if eq, needsMoreChecks := compare.NilCheck(else1, else2); !needsMoreChecks {
		return eq
	}

	// For now, use string comparison on the parsed string value
	// This could be enhanced to parse the result as an expression
	return strings.TrimSpace(else1.Result) == strings.TrimSpace(else2.Result)
}

// orExpressionsEqual compares OR expressions structurally
func orExpressionsEqual(or1, or2 *parser.OrExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(or1, or2); !needsMoreChecks {
		return eq
	}

	// Compare base AND expression
	if !andExpressionsEqual(or1.And, or2.And) {
		return false
	}

	// Compare OR rest clauses
	return compare.Slices(or1.Rest, or2.Rest, func(a, b parser.OrRest) bool {
		return a.Op == b.Op && andExpressionsEqual(a.And, b.And)
	})
}

// andExpressionsEqual compares AND expressions structurally
func andExpressionsEqual(and1, and2 *parser.AndExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(and1, and2); !needsMoreChecks {
		return eq
	}

	// Compare base NOT expression
	if !notExpressionsEqual(and1.Not, and2.Not) {
		return false
	}

	// Compare AND rest clauses
	return compare.Slices(and1.Rest, and2.Rest, func(a, b parser.AndRest) bool {
		return a.Op == b.Op && notExpressionsEqual(a.Not, b.Not)
	})
}

// notExpressionsEqual compares NOT expressions structurally
func notExpressionsEqual(not1, not2 *parser.NotExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(not1, not2); !needsMoreChecks {
		return eq
	}

	// Compare NOT flag and comparison
	return not1.Not == not2.Not && comparisonExpressionsEqual(not1.Comparison, not2.Comparison)
}

// comparisonExpressionsEqual compares comparison expressions structurally
func comparisonExpressionsEqual(comp1, comp2 *parser.ComparisonExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(comp1, comp2); !needsMoreChecks {
		return eq
	}

	// Compare base addition expression
	if !additionExpressionsEqual(comp1.Addition, comp2.Addition) {
		return false
	}

	// Compare comparison rest (operations)
	if eq, needsMoreChecks := compare.NilCheck(comp1.Rest, comp2.Rest); !needsMoreChecks {
		return eq
	}

	// Compare operation types using helper function
	return compareComparisonOperations(comp1.Rest, comp2.Rest)
}

// compareComparisonOperations is a helper to compare different types of comparison operations
func compareComparisonOperations(rest1, rest2 *parser.ComparisonRest) bool {
	// Compare simple operations
	if !compare.PointersWithEqual(rest1.SimpleOp, rest2.SimpleOp, simpleComparisonsEqual) {
		return false
	}

	// Compare IN operations
	if !compare.PointersWithEqual(rest1.InOp, rest2.InOp, inComparisonsEqual) {
		return false
	}

	// Compare BETWEEN operations
	return compare.PointersWithEqual(rest1.BetweenOp, rest2.BetweenOp, betweenComparisonsEqual)
}

// simpleComparisonsEqual compares simple comparison operations
func simpleComparisonsEqual(comp1, comp2 *parser.SimpleComparison) bool {
	if eq, needsMoreChecks := compare.NilCheck(comp1, comp2); !needsMoreChecks {
		return eq
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
	if eq, needsMoreChecks := compare.NilCheck(op1, op2); !needsMoreChecks {
		return eq
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
	if eq, needsMoreChecks := compare.NilCheck(in1, in2); !needsMoreChecks {
		return eq
	}

	if in1.Not != in2.Not {
		return false
	}

	// Compare InExpression
	return inExpressionsEqual(in1.Expr, in2.Expr)
}

// inExpressionsEqual compares IN expressions (list, array, subquery, or identifier)
func inExpressionsEqual(expr1, expr2 *parser.InExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(expr1, expr2); !needsMoreChecks {
		return eq
	}

	// Extract identifier from expression if it's a single-element list with just an identifier
	// This handles the case where ClickHouse returns IN (cte_name) vs schema has IN cte_name
	getIdentFromExpr := func(expr *parser.InExpression) *string {
		if expr.Ident != nil {
			return expr.Ident
		}
		// Check if it's a single-element list containing just an identifier
		if len(expr.List) == 1 {
			e := &expr.List[0]
			if e.Or != nil && e.Or.And != nil && e.Or.And.Not != nil &&
				e.Or.And.Not.Comparison != nil && e.Or.And.Not.Comparison.Addition != nil &&
				e.Or.And.Not.Comparison.Addition.Multiplication != nil &&
				e.Or.And.Not.Comparison.Addition.Multiplication.Unary != nil &&
				e.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary != nil &&
				e.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Identifier != nil &&
				len(e.Or.Rest) == 0 && len(e.Or.And.Rest) == 0 &&
				e.Or.And.Not.Comparison.Rest == nil &&
				len(e.Or.And.Not.Comparison.Addition.Rest) == 0 &&
				len(e.Or.And.Not.Comparison.Addition.Multiplication.Rest) == 0 {
				// It's a simple identifier wrapped in parentheses
				return &e.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Identifier.Name
			}
		}
		return nil
	}

	ident1 := getIdentFromExpr(expr1)
	ident2 := getIdentFromExpr(expr2)

	// If both can be resolved to identifiers, compare them
	if ident1 != nil && ident2 != nil {
		return strings.EqualFold(normalizeIdentifier(*ident1), normalizeIdentifier(*ident2))
	}

	// Compare list of expressions (both must be lists)
	if len(expr1.List) > 0 && len(expr2.List) > 0 {
		return compare.Slices(expr1.List, expr2.List, func(a, b parser.Expression) bool {
			return expressionsAreEqual(&a, &b)
		})
	}
	if len(expr1.List) > 0 || len(expr2.List) > 0 {
		return false
	}

	// Compare array expressions
	if expr1.Array != nil && expr2.Array != nil {
		return arrayExpressionsEqual(expr1.Array, expr2.Array)
	}
	if expr1.Array != nil || expr2.Array != nil {
		return false
	}

	// Compare subqueries
	if expr1.Subquery != nil && expr2.Subquery != nil {
		return subqueriesEqual(expr1.Subquery, expr2.Subquery)
	}
	if expr1.Subquery != nil || expr2.Subquery != nil {
		return false
	}

	return true
}

// subqueriesEqual compares subquery expressions
func subqueriesEqual(sub1, sub2 *parser.Subquery) bool {
	if eq, needsMoreChecks := compare.NilCheck(sub1, sub2); !needsMoreChecks {
		return eq
	}
	// Use selectStatementsAreEqualAST from view.go (same package)
	return selectStatementsAreEqualAST(&sub1.SelectStmt, &sub2.SelectStmt)
}

// betweenComparisonsEqual compares BETWEEN operations
func betweenComparisonsEqual(between1, between2 *parser.BetweenComparison) bool {
	if eq, needsMoreChecks := compare.NilCheck(between1, between2); !needsMoreChecks {
		return eq
	}

	return between1.Not == between2.Not && between1.Between == between2.Between
	// Note: BetweenExpression comparison would need to be implemented based on the actual structure
}

// additionExpressionsEqual compares addition/subtraction expressions
func additionExpressionsEqual(add1, add2 *parser.AdditionExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(add1, add2); !needsMoreChecks {
		return eq
	}

	// Compare base multiplication expression
	if !multiplicationExpressionsEqual(add1.Multiplication, add2.Multiplication) {
		return false
	}

	// Compare addition rest clauses
	return compare.Slices(add1.Rest, add2.Rest, func(a, b parser.AdditionRest) bool {
		return a.Op == b.Op && multiplicationExpressionsEqual(a.Multiplication, b.Multiplication)
	})
}

// multiplicationExpressionsEqual compares multiplication/division expressions
func multiplicationExpressionsEqual(mul1, mul2 *parser.MultiplicationExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(mul1, mul2); !needsMoreChecks {
		return eq
	}

	// Compare base unary expression
	if !unaryExpressionsEqual(mul1.Unary, mul2.Unary) {
		return false
	}

	// Compare multiplication rest clauses
	return compare.Slices(mul1.Rest, mul2.Rest, func(a, b parser.MultiplicationRest) bool {
		return a.Op == b.Op && unaryExpressionsEqual(a.Unary, b.Unary)
	})
}

// unaryExpressionsEqual compares unary expressions
func unaryExpressionsEqual(unary1, unary2 *parser.UnaryExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(unary1, unary2); !needsMoreChecks {
		return eq
	}

	return unary1.Op == unary2.Op && primaryExpressionsEqual(unary1.Primary, unary2.Primary)
}

// primaryExpressionsEqual compares primary expressions
func primaryExpressionsEqual(prim1, prim2 *parser.PrimaryExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(prim1, prim2); !needsMoreChecks {
		return eq
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

	// Compare tuple expressions
	if prim1.Tuple != nil && prim2.Tuple != nil {
		return tupleExpressionsEqual(prim1.Tuple, prim2.Tuple)
	}
	if prim1.Tuple != nil || prim2.Tuple != nil {
		return false
	}

	// Compare array expressions
	if prim1.Array != nil && prim2.Array != nil {
		return arrayExpressionsEqual(prim1.Array, prim2.Array)
	}
	if prim1.Array != nil || prim2.Array != nil {
		return false
	}

	// Compare interval expressions
	if prim1.Interval != nil && prim2.Interval != nil {
		return intervalExpressionsEqual(prim1.Interval, prim2.Interval)
	}
	// Handle case where one is INTERVAL and the other is toIntervalXxx() function
	// ClickHouse converts "INTERVAL 3 HOUR" to "toIntervalHour(3)"
	if prim1.Interval != nil && prim2.Function != nil {
		if valStr, unit, ok := toIntervalFunctionToInterval(prim2.Function); ok {
			return prim1.Interval.Value == valStr && timeUnitsAreEqual(prim1.Interval.Unit, unit)
		}
		return false
	}
	if prim2.Interval != nil && prim1.Function != nil {
		if valStr, unit, ok := toIntervalFunctionToInterval(prim1.Function); ok {
			return prim2.Interval.Value == valStr && timeUnitsAreEqual(prim2.Interval.Unit, unit)
		}
		return false
	}
	if prim1.Interval != nil || prim2.Interval != nil {
		return false
	}

	// Compare cast expressions
	if prim1.Cast != nil && prim2.Cast != nil {
		return castExpressionsEqual(prim1.Cast, prim2.Cast)
	}
	if prim1.Cast != nil || prim2.Cast != nil {
		return false
	}

	// Compare extract expressions
	if prim1.Extract != nil && prim2.Extract != nil {
		return extractExpressionsEqual(prim1.Extract, prim2.Extract)
	}
	if prim1.Extract != nil || prim2.Extract != nil {
		return false
	}

	// All known types handled, expressions are equal if we reach here
	return true
}

// intervalExpressionsEqual compares INTERVAL expressions
func intervalExpressionsEqual(int1, int2 *parser.IntervalExpr) bool {
	if eq, needsMoreChecks := compare.NilCheck(int1, int2); !needsMoreChecks {
		return eq
	}
	return int1.Value == int2.Value && timeUnitsAreEqual(int1.Unit, int2.Unit)
}

// toIntervalFunctionToInterval extracts interval info from toIntervalXxx(n) function calls
// Returns (valueStr, unit, ok) where ok is true if this is a toInterval function
func toIntervalFunctionToInterval(funcCall *parser.FunctionCall) (string, string, bool) {
	if funcCall == nil {
		return "", "", false
	}

	name := strings.ToLower(normalizeIdentifier(funcCall.Name))
	var unit string
	switch name {
	case "tointervalsecond":
		unit = "SECOND"
	case "tointervalminute":
		unit = "MINUTE"
	case "tointervalhour":
		unit = "HOUR"
	case "tointervalday":
		unit = "DAY"
	case "tointervalweek":
		unit = "WEEK"
	case "tointervalmonth":
		unit = "MONTH"
	case "tointervalyear":
		unit = "YEAR"
	default:
		return "", "", false
	}

	// Get the argument value from FirstParentheses
	if len(funcCall.FirstParentheses) != 1 {
		return "", "", false
	}

	// Try to extract the number value from the argument
	arg := funcCall.FirstParentheses[0]
	if arg.Expression != nil &&
		arg.Expression.Or != nil && arg.Expression.Or.And != nil && arg.Expression.Or.And.Not != nil &&
		arg.Expression.Or.And.Not.Comparison != nil && arg.Expression.Or.And.Not.Comparison.Addition != nil &&
		arg.Expression.Or.And.Not.Comparison.Addition.Multiplication != nil &&
		arg.Expression.Or.And.Not.Comparison.Addition.Multiplication.Unary != nil &&
		arg.Expression.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary != nil &&
		arg.Expression.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Literal != nil &&
		arg.Expression.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Literal.Number != nil {
		val := *arg.Expression.Or.And.Not.Comparison.Addition.Multiplication.Unary.Primary.Literal.Number
		return val, unit, true
	}

	return "", "", false
}

// castExpressionsEqual compares CAST expressions
func castExpressionsEqual(cast1, cast2 *parser.CastExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(cast1, cast2); !needsMoreChecks {
		return eq
	}
	// Compare the expression being cast
	if !expressionsAreEqual(&cast1.Expression, &cast2.Expression) {
		return false
	}
	// Compare the target types using their Equal method
	return cast1.Type.Equal(&cast2.Type)
}

// extractExpressionsEqual compares EXTRACT expressions
func extractExpressionsEqual(ext1, ext2 *parser.ExtractExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(ext1, ext2); !needsMoreChecks {
		return eq
	}
	// Compare what is being extracted (e.g., YEAR, MONTH, DAY)
	if !strings.EqualFold(ext1.Part, ext2.Part) {
		return false
	}
	// Compare the expression being extracted from
	return expressionsAreEqual(&ext1.Expr, &ext2.Expr)
}

// tupleExpressionsEqual compares tuple expressions
func tupleExpressionsEqual(tuple1, tuple2 *parser.TupleExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(tuple1, tuple2); !needsMoreChecks {
		return eq
	}
	return compare.Slices(tuple1.Elements, tuple2.Elements, func(a, b parser.Expression) bool {
		return expressionsAreEqual(&a, &b)
	})
}

// arrayExpressionsEqual compares array expressions
func arrayExpressionsEqual(array1, array2 *parser.ArrayExpression) bool {
	if eq, needsMoreChecks := compare.NilCheck(array1, array2); !needsMoreChecks {
		return eq
	}
	return compare.Slices(array1.Elements, array2.Elements, func(a, b parser.Expression) bool {
		return expressionsAreEqual(&a, &b)
	})
}

// literalsEqual compares literal values
func literalsEqual(lit1, lit2 *parser.Literal) bool {
	if eq, needsMoreChecks := compare.NilCheck(lit1, lit2); !needsMoreChecks {
		return eq
	}

	// Compare string values
	if !compare.Pointers(lit1.StringValue, lit2.StringValue) {
		return false
	}

	// Compare numbers (stored as strings in parser)
	if !compare.Pointers(lit1.Number, lit2.Number) {
		return false
	}

	// Compare booleans (stored as strings in parser)
	if !compare.Pointers(lit1.Boolean, lit2.Boolean) {
		return false
	}

	// Compare NULL
	return lit1.Null == lit2.Null
}

// identifiersEqual compares identifier expressions
func identifiersEqual(id1, id2 *parser.IdentifierExpr) bool {
	if eq, needsMoreChecks := compare.NilCheck(id1, id2); !needsMoreChecks {
		return eq
	}

	// Compare database qualifier (normalize backticks)
	db1 := ""
	if id1.Database != nil {
		db1 = normalizeIdentifier(*id1.Database)
	}
	db2 := ""
	if id2.Database != nil {
		db2 = normalizeIdentifier(*id2.Database)
	}
	if !strings.EqualFold(db1, db2) {
		return false
	}

	// Compare table qualifier (normalize backticks)
	table1 := ""
	if id1.Table != nil {
		table1 = normalizeIdentifier(*id1.Table)
	}
	table2 := ""
	if id2.Table != nil {
		table2 = normalizeIdentifier(*id2.Table)
	}
	if !strings.EqualFold(table1, table2) {
		return false
	}

	// Compare column name (case-insensitive, normalize backticks for ClickHouse)
	return strings.EqualFold(normalizeIdentifier(id1.Name), normalizeIdentifier(id2.Name))
}

// functionCallsEqual compares function calls including arguments
func functionCallsEqual(func1, func2 *parser.FunctionCall) bool {
	if eq, needsMoreChecks := compare.NilCheck(func1, func2); !needsMoreChecks {
		return eq
	}

	// Compare function names (case-insensitive, normalize backticks for ClickHouse)
	if !strings.EqualFold(normalizeIdentifier(func1.Name), normalizeIdentifier(func2.Name)) {
		return false
	}

	// Compare first parentheses argument lists
	if !compare.Slices(func1.FirstParentheses, func2.FirstParentheses, func(a, b parser.FunctionArg) bool {
		return functionArgsEqual(&a, &b)
	}) {
		return false
	}

	// Compare second parentheses argument lists (for parameterized functions)
	if !compare.Slices(func1.SecondParentheses, func2.SecondParentheses, func(a, b parser.FunctionArg) bool {
		return functionArgsEqual(&a, &b)
	}) {
		return false
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
