package parser

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
)

type (
	// Expression represents any ClickHouse expression with proper precedence handling
	// Precedence levels (lowest to highest):
	// 1. CASE (lowest - can contain any sub-expressions)
	// 2. OR
	// 3. AND
	// 4. NOT
	// 5. Comparison (=, !=, <, >, <=, >=, LIKE, IN, BETWEEN)
	// 6. Addition/Subtraction (+, -)
	// 7. Multiplication/Division/Modulo (*, /, %)
	// 8. Unary (+, -, NOT)
	// 9. Primary (literals, identifiers, functions, parentheses)
	Expression struct {
		Case *CaseExpression `parser:"@@"`
		Or   *OrExpression   `parser:"| @@"`
	}

	// OrExpression handles OR operations (lowest precedence)
	OrExpression struct {
		And  *AndExpression `parser:"@@"`
		Rest []OrRest       `parser:"@@*"`
	}

	OrRest struct {
		Op  string         `parser:"@'OR'"`
		And *AndExpression `parser:"@@"`
	}

	// AndExpression handles AND operations
	AndExpression struct {
		Not  *NotExpression `parser:"@@"`
		Rest []AndRest      `parser:"@@*"`
	}

	AndRest struct {
		Op  string         `parser:"@'AND'"`
		Not *NotExpression `parser:"@@"`
	}

	// NotExpression handles NOT operations
	NotExpression struct {
		Not        bool                  `parser:"@'NOT'?"`
		Comparison *ComparisonExpression `parser:"@@"`
	}

	// ComparisonExpression handles comparison operations
	ComparisonExpression struct {
		Addition *AdditionExpression `parser:"@@"`
		Rest     *ComparisonRest     `parser:"@@?"`
		IsNull   *IsNullExpr         `parser:"@@?"`
	}

	ComparisonRest struct {
		SimpleOp  *SimpleComparison  `parser:"@@"`
		InOp      *InComparison      `parser:"| @@"`
		BetweenOp *BetweenComparison `parser:"| @@"`
	}

	// SimpleComparison handles basic comparison operations
	SimpleComparison struct {
		Op       *SimpleComparisonOp `parser:"@@"`
		Addition *AdditionExpression `parser:"@@"`
	}

	SimpleComparisonOp struct {
		Eq      bool `parser:"@'='"`
		NotEq   bool `parser:"| @'!=' | @'<>'"`
		LtEq    bool `parser:"| @'<='"`
		GtEq    bool `parser:"| @'>='"`
		Lt      bool `parser:"| @'<'"`
		Gt      bool `parser:"| @'>'"`
		Like    bool `parser:"| @'LIKE'"`
		NotLike bool `parser:"| @('NOT' 'LIKE')"`
	}

	// InComparison handles IN and NOT IN operations
	InComparison struct {
		Not  bool          `parser:"@'NOT'?"`
		In   string        `parser:"'IN'"`
		Expr *InExpression `parser:"@@"`
	}

	// BetweenComparison handles BETWEEN and NOT BETWEEN operations
	BetweenComparison struct {
		Not     bool               `parser:"@'NOT'?"`
		Between string             `parser:"'BETWEEN'"`
		Expr    *BetweenExpression `parser:"@@"`
	}

	// AdditionExpression handles addition and subtraction
	AdditionExpression struct {
		Multiplication *MultiplicationExpression `parser:"@@"`
		Rest           []AdditionRest            `parser:"@@*"`
	}

	AdditionRest struct {
		Op             string                    `parser:"@('+' | '-')"`
		Multiplication *MultiplicationExpression `parser:"@@"`
	}

	// MultiplicationExpression handles multiplication, division, and modulo
	MultiplicationExpression struct {
		Unary *UnaryExpression     `parser:"@@"`
		Rest  []MultiplicationRest `parser:"@@*"`
	}

	MultiplicationRest struct {
		Op    string           `parser:"@('*' | '/' | '%')"`
		Unary *UnaryExpression `parser:"@@"`
	}

	// UnaryExpression handles unary operators
	UnaryExpression struct {
		Op      string             `parser:"@('+' | '-')?"`
		Primary *PrimaryExpression `parser:"@@"`
	}

	// PrimaryExpression represents the highest precedence expressions
	PrimaryExpression struct {
		Literal     *Literal           `parser:"@@"`
		Interval    *IntervalExpr      `parser:"| @@"`
		Extract     *ExtractExpression `parser:"| @@"`
		Cast        *CastExpression    `parser:"| @@"`
		Function    *FunctionCall      `parser:"| @@"`
		Identifier  *IdentifierExpr    `parser:"| @@"`
		Parentheses *ParenExpression   `parser:"| @@"`
		Tuple       *TupleExpression   `parser:"| @@"`
		Array       *ArrayExpression   `parser:"| @@"`
	}

	// Literal represents literal values
	Literal struct {
		StringValue *string `parser:"@String"`
		Number      *string `parser:"| @Number"`
		Boolean     *string `parser:"| @('TRUE' | 'FALSE')"`
		Null        bool    `parser:"| @'NULL'"`
	}

	// IdentifierExpr represents column names or qualified names
	IdentifierExpr struct {
		Database *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Table    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name     string  `parser:"@(Ident | BacktickIdent)"`
	}

	// FunctionCall represents function invocations, including parameterized functions like quantilesState(0.5, 0.75)(value)
	FunctionCall struct {
		Name             string        `parser:"@(Ident | BacktickIdent)"`
		FirstParentheses []FunctionArg `parser:"'(' (@@ (',' @@)*)? ')'"`
		// Optional second set of parentheses for parameterized functions
		SecondParentheses []FunctionArg `parser:"('(' (@@ (',' @@)*)? ')')?"`
		Over              *OverClause   `parser:"@@?"`
	}

	// FunctionArg represents arguments in function calls (can be * or expression)
	FunctionArg struct {
		Star       *string     `parser:"@'*'"`
		Expression *Expression `parser:"| @@"`
	}

	// OverClause for window functions
	OverClause struct {
		Over        string        `parser:"'OVER'"`
		PartitionBy []Expression  `parser:"'(' ('PARTITION' 'BY' @@ (',' @@)*)?"`
		OrderBy     []OrderByExpr `parser:"('ORDER' 'BY' @@ (',' @@)*)?"`
		Frame       *WindowFrame  `parser:"@@? ')'"`
	}

	// OrderByExpr for ORDER BY in OVER clause
	OrderByExpr struct {
		Expression Expression `parser:"@@"`
		Desc       bool       `parser:"@'DESC'?"`
		Nulls      *string    `parser:"('NULLS' @('FIRST' | 'LAST'))?"`
	}

	// WindowFrame for window functions
	WindowFrame struct {
		Type    string      `parser:"@('ROWS' | 'RANGE')"`
		Between bool        `parser:"@'BETWEEN'?"`
		Start   FrameBound  `parser:"@@"`
		End     *FrameBound `parser:"('AND' @@)?"`
	}

	// FrameBound represents window frame boundaries
	FrameBound struct {
		Type      string `parser:"@('UNBOUNDED' | 'CURRENT' | Number)"`
		Direction string `parser:"@('PRECEDING' | 'FOLLOWING' | 'ROW')?"`
	}

	// ParenExpression represents parenthesized expressions
	ParenExpression struct {
		Expression Expression `parser:"'(' @@ ')'"`
	}

	// TupleExpression represents tuple literals
	TupleExpression struct {
		Elements []Expression `parser:"'(' (@@ (',' @@)*)? ')'"`
	}

	// ArrayExpression represents array literals
	ArrayExpression struct {
		Elements []Expression `parser:"'[' (@@ (',' @@)*)? ']'"`
	}

	// CaseExpression represents CASE expressions
	CaseExpression struct {
		Case        string       `parser:"'CASE'"`
		WhenClauses []WhenClause `parser:"@@+"`
		ElseClause  *ElseClause  `parser:"@@?"`
		End         string       `parser:"'END'"`
	}

	// WhenClause represents WHEN condition THEN result
	// Using simpler parsing to avoid recursion issues during debugging
	WhenClause struct {
		When      string `parser:"'WHEN'"`
		Condition string `parser:"@(~'THEN')+"`
		Then      string `parser:"'THEN'"`
		Result    string `parser:"@(~('WHEN' | 'ELSE' | 'END'))+"`
	}

	// ElseClause represents ELSE result
	ElseClause struct {
		Else   string `parser:"'ELSE'"`
		Result string `parser:"@(~'END')+"`
	}

	// CastExpression represents type casting
	CastExpression struct {
		Cast       string     `parser:"'CAST' '('"`
		Expression Expression `parser:"@@"`
		As         string     `parser:"'AS'"`
		Type       DataType   `parser:"@@"`
		Close      string     `parser:"')'"`
	}

	// IntervalExpr represents INTERVAL expressions
	IntervalExpr struct {
		Interval string `parser:"'INTERVAL'"`
		Value    string `parser:"@Number"`
		Unit     string `parser:"@('SECOND' | 'MINUTE' | 'HOUR' | 'DAY' | 'WEEK' | 'MONTH' | 'QUARTER' | 'YEAR')"`
	}

	// ExtractExpression represents EXTRACT expressions
	ExtractExpression struct {
		Extract string     `parser:"'EXTRACT' '('"`
		Part    string     `parser:"@('YEAR' | 'MONTH' | 'DAY' | 'HOUR' | 'MINUTE' | 'SECOND')"`
		From    string     `parser:"'FROM'"`
		Expr    Expression `parser:"@@"`
		Close   string     `parser:"')'"`
	}

	// Subquery represents a subquery expression
	Subquery struct {
		OpenParen  string          `parser:"'('"`
		SelectStmt SelectStatement `parser:"@@"`
		CloseParen string          `parser:"')'"`
	}

	// BetweenExpression handles BETWEEN operations (part of comparison)
	BetweenExpression struct {
		Low  AdditionExpression `parser:"@@"`
		And  string             `parser:"'AND'"`
		High AdditionExpression `parser:"@@"`
	}

	// InExpression handles IN operations with lists, arrays, or subqueries
	InExpression struct {
		List     []Expression     `parser:"'(' @@ (',' @@)* ')'"`
		Array    *ArrayExpression `parser:"| @@"`
		Subquery *Subquery        `parser:"| @@"`
	}

	// IsNullExpr handles IS NULL and IS NOT NULL expressions as postfix operators
	IsNullExpr struct {
		Is   string `parser:"'IS'"`
		Not  bool   `parser:"@'NOT'?"`
		Null string `parser:"'NULL'"`
	}
)

// String returns the string representation of an Expression.
func (e *Expression) String() string {
	if e.Case != nil {
		return e.Case.String()
	}
	if e.Or != nil {
		return e.Or.String()
	}
	return "expression"
}

// String returns the string representation of an OrExpression with proper OR operator placement.
func (o *OrExpression) String() string {
	if o.And != nil {
		var results strings.Builder
		results.WriteString(o.And.String())
		for _, rest := range o.Rest {
			results.WriteString(" OR " + rest.And.String())
		}

		return results.String()
	}
	return ""
}

// String returns the string representation of an AndExpression with proper AND operator placement.
func (a *AndExpression) String() string {
	if a.Not != nil {
		var results strings.Builder
		results.WriteString(a.Not.String())
		for _, rest := range a.Rest {
			results.WriteString(" AND " + rest.Not.String())
		}

		return results.String()
	}
	return ""
}

// String returns the string representation of a NotExpression with optional NOT prefix.
func (n *NotExpression) String() string {
	prefix := ""
	if n.Not {
		prefix = "NOT "
	}
	if n.Comparison != nil {
		return prefix + n.Comparison.String()
	}
	return prefix
}

// String returns the string representation of a ComparisonExpression including operators and IS NULL checks.
//
//nolint:nestif // Complex nested logic needed for expression string formatting
func (c *ComparisonExpression) String() string {
	if c.Addition != nil {
		result := c.Addition.String()
		if c.Rest != nil {
			if c.Rest.SimpleOp != nil {
				result += " " + c.Rest.SimpleOp.Op.String() + " " + c.Rest.SimpleOp.Addition.String()
			} else if c.Rest.InOp != nil {
				if c.Rest.InOp.Not {
					result += " NOT"
				}
				result += " IN " + c.Rest.InOp.Expr.String()
			} else if c.Rest.BetweenOp != nil {
				if c.Rest.BetweenOp.Not {
					result += " NOT"
				}
				result += " BETWEEN " + c.Rest.BetweenOp.Expr.String()
			}
		}
		if c.IsNull != nil {
			result += " IS"
			if c.IsNull.Not {
				result += " NOT"
			}
			result += " NULL"
		}
		return result
	}
	return ""
}

// String returns the string representation of a SimpleComparisonOp (=, !=, <, >, <=, >=, LIKE, NOT LIKE).
func (c *SimpleComparisonOp) String() string {
	if c.Eq {
		return "="
	}
	if c.NotEq {
		return "!="
	}
	if c.LtEq {
		return "<="
	}
	if c.GtEq {
		return ">="
	}
	if c.Lt {
		return "<"
	}
	if c.Gt {
		return ">"
	}
	if c.Like {
		return "LIKE"
	}
	if c.NotLike {
		return "NOT LIKE"
	}
	return ""
}

func (a *AdditionExpression) String() string {
	if a.Multiplication != nil {
		var results strings.Builder
		results.WriteString(a.Multiplication.String())
		for _, rest := range a.Rest {
			results.WriteString(" " + rest.Op + " " + rest.Multiplication.String())
		}

		return results.String()
	}
	return ""
}

func (m *MultiplicationExpression) String() string {
	if m.Unary != nil {
		var results strings.Builder
		results.WriteString(m.Unary.String())
		for _, rest := range m.Rest {
			results.WriteString(" " + rest.Op + " " + rest.Unary.String())
		}

		return results.String()
	}
	return ""
}

func (u *UnaryExpression) String() string {
	prefix := u.Op
	if u.Primary != nil {
		return prefix + u.Primary.String()
	}
	return prefix
}

func (p *PrimaryExpression) String() string {
	if p.Literal != nil {
		return p.Literal.String()
	}
	if p.Interval != nil {
		return p.Interval.String()
	}
	if p.Extract != nil {
		return p.Extract.String()
	}
	if p.Cast != nil {
		return p.Cast.String()
	}
	if p.Function != nil {
		return p.Function.String()
	}
	if p.Identifier != nil {
		return p.Identifier.String()
	}
	if p.Parentheses != nil {
		return "(" + p.Parentheses.Expression.String() + ")"
	}
	if p.Tuple != nil {
		return p.Tuple.String()
	}
	if p.Array != nil {
		return p.Array.String()
	}
	return ""
}

// String returns the string representation of a Literal value (string, number, boolean, or NULL).
func (l *Literal) String() string {
	if l.StringValue != nil {
		return *l.StringValue
	}
	if l.Number != nil {
		return *l.Number
	}
	if l.Boolean != nil {
		return *l.Boolean
	}
	if l.Null {
		return "NULL"
	}
	return ""
}

// String returns the string representation of an IdentifierExpr with optional database and table qualifiers.
func (i *IdentifierExpr) String() string {
	result := ""
	if i.Database != nil {
		result += *i.Database + "."
	}
	if i.Table != nil {
		result += *i.Table + "."
	}
	result += i.Name
	return result
}

// String returns the string representation of a FunctionCall with function name and arguments.
func (f *FunctionCall) String() string {
	var results strings.Builder
	results.WriteString(f.Name + "(")
	for i, arg := range f.FirstParentheses {
		if i > 0 {
			results.WriteString(", ")
		}
		results.WriteString(arg.String())
	}
	results.WriteString(")")

	// Second set of parentheses if present (for parameterized functions)
	if len(f.SecondParentheses) > 0 {
		results.WriteString("(")
		for i, arg := range f.SecondParentheses {
			if i > 0 {
				results.WriteString(", ")
			}
			results.WriteString(arg.String())
		}
		results.WriteString(")")
	}

	// Add OVER clause for window functions
	if f.Over != nil {
		results.WriteString(" " + f.Over.String())
	}

	return results.String()
}

func (a *FunctionArg) String() string {
	if a.Star != nil {
		return "*"
	}
	if a.Expression != nil {
		return a.Expression.String()
	}
	return ""
}

// String returns the string representation of an OverClause for window functions
func (o *OverClause) String() string {
	var results strings.Builder
	results.WriteString("OVER (")

	// Add PARTITION BY clause if present
	if len(o.PartitionBy) > 0 {
		results.WriteString("PARTITION BY ")
		for i, expr := range o.PartitionBy {
			if i > 0 {
				results.WriteString(", ")
			}
			results.WriteString(expr.String())
		}
	}

	// Add ORDER BY clause if present
	if len(o.OrderBy) > 0 {
		if len(o.PartitionBy) > 0 {
			results.WriteString(" ")
		}

		results.WriteString("ORDER BY ")
		for i, orderExpr := range o.OrderBy {
			if i > 0 {
				results.WriteString(", ")
			}
			results.WriteString(orderExpr.String())
		}
	}

	// Add frame clause if present
	if o.Frame != nil {
		if len(o.PartitionBy) > 0 || len(o.OrderBy) > 0 {
			results.WriteString(" ")
		}
		results.WriteString(o.Frame.String())
	}

	results.WriteString(")")
	return results.String()
}

// String returns the string representation of an OrderByExpr for ORDER BY in OVER clauses
func (o *OrderByExpr) String() string {
	result := o.Expression.String()
	if o.Desc {
		result += " DESC"
	}
	if o.Nulls != nil {
		result += " NULLS " + *o.Nulls
	}
	return result
}

// String returns the string representation of a WindowFrame for window functions
func (w *WindowFrame) String() string {
	result := w.Type
	if w.Between {
		result += " BETWEEN " + w.Start.String()
		if w.End != nil {
			result += " AND " + w.End.String()
		}
	} else {
		result += " " + w.Start.String()
	}
	return result
}

// String returns the string representation of a FrameBound for window frame boundaries
func (f *FrameBound) String() string {
	result := f.Type
	if f.Direction != "" {
		result += " " + f.Direction
	}
	return result
}

func (t *TupleExpression) String() string {
	var results strings.Builder
	results.WriteString("(")

	for i, elem := range t.Elements {
		if i > 0 {
			results.WriteString(",")
		}
		results.WriteString(elem.String())
	}

	results.WriteString(")")
	return results.String()
}

func (a *ArrayExpression) String() string {
	var results strings.Builder
	results.WriteString("[")

	for i, elem := range a.Elements {
		if i > 0 {
			results.WriteString(", ")
		}
		results.WriteString(elem.String())
	}

	results.WriteString("]")
	return results.String()
}

func (i *IntervalExpr) String() string {
	return "INTERVAL " + i.Value + " " + i.Unit
}

func (c *CaseExpression) String() string {
	var results strings.Builder
	results.WriteString("CASE")

	for _, when := range c.WhenClauses {
		results.WriteString(" WHEN " + when.Condition + " THEN " + when.Result)
	}

	if c.ElseClause != nil {
		results.WriteString(" ELSE " + c.ElseClause.Result)
	}

	results.WriteString(" END")
	return results.String()
}

func (c *CastExpression) String() string {
	return "CAST(" + c.Expression.String() + " AS " + formatDataTypeForExpression(c.Type) + ")"
}

func (e *ExtractExpression) String() string {
	return "EXTRACT(" + e.Part + " FROM " + e.Expr.String() + ")"
}

// Equal compares two Expression instances for structural equality
func (e *Expression) Equal(other *Expression) bool {
	if eq, done := compare.NilCheck(e, other); !done {
		return eq
	}

	// Compare CASE expressions
	if e.Case != nil && other.Case != nil {
		return e.Case.Equal(other.Case)
	}
	if e.Case != nil || other.Case != nil {
		return false
	}

	// Compare OR expressions
	if e.Or != nil && other.Or != nil {
		return e.Or.Equal(other.Or)
	}

	return e.Or == nil && other.Or == nil
}

// Equal compares two Case expressions
func (c *CaseExpression) Equal(other *CaseExpression) bool {
	if eq, done := compare.NilCheck(c, other); !done {
		return eq
	}

	whenEqual := compare.Slices(c.WhenClauses, other.WhenClauses, func(a, b WhenClause) bool {
		return a.Condition == b.Condition && a.Result == b.Result
	})

	elseEqual := (c.ElseClause == nil && other.ElseClause == nil) ||
		(c.ElseClause != nil && other.ElseClause != nil && c.ElseClause.Result == other.ElseClause.Result)

	return whenEqual && elseEqual
}

// Equal compares two OR expressions
func (o *OrExpression) Equal(other *OrExpression) bool {
	if eq, done := compare.NilCheck(o, other); !done {
		return eq
	}

	return o.And.Equal(other.And) &&
		compare.Slices(o.Rest, other.Rest, func(a, b OrRest) bool {
			return a.Op == b.Op && a.And.Equal(b.And)
		})
}

// Equal compares two AND expressions
func (a *AndExpression) Equal(other *AndExpression) bool {
	if eq, done := compare.NilCheck(a, other); !done {
		return eq
	}

	return a.Not.Equal(other.Not) &&
		compare.Slices(a.Rest, other.Rest, func(x, y AndRest) bool {
			return x.Op == y.Op && x.Not.Equal(y.Not)
		})
}

// Equal compares two NOT expressions
func (n *NotExpression) Equal(other *NotExpression) bool {
	if eq, done := compare.NilCheck(n, other); !done {
		return eq
	}

	return n.Not == other.Not && n.Comparison.Equal(other.Comparison)
}

// Equal compares two Comparison expressions
func (c *ComparisonExpression) Equal(other *ComparisonExpression) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}

	if !c.Addition.Equal(other.Addition) {
		return false
	}

	// Compare Rest
	if (c.Rest == nil) != (other.Rest == nil) {
		return false
	}
	if c.Rest != nil && !c.Rest.Equal(other.Rest) {
		return false
	}

	// Compare IsNull
	if (c.IsNull == nil) != (other.IsNull == nil) {
		return false
	}
	if c.IsNull != nil && !c.IsNull.Equal(other.IsNull) {
		return false
	}

	return true
}

// Equal compares two ComparisonRest expressions
func (c *ComparisonRest) Equal(other *ComparisonRest) bool {
	if c == nil && other == nil {
		return true
	}
	if c == nil || other == nil {
		return false
	}

	// Use pointer comparison for which operation is set
	if (c.SimpleOp != nil) != (other.SimpleOp != nil) {
		return false
	}
	if c.SimpleOp != nil && !c.SimpleOp.Equal(other.SimpleOp) {
		return false
	}

	if (c.InOp != nil) != (other.InOp != nil) {
		return false
	}
	// For now, just check if both are set - full IN comparison would require more work

	if (c.BetweenOp != nil) != (other.BetweenOp != nil) {
		return false
	}
	// For now, just check if both are set - full BETWEEN comparison would require more work

	return true
}

// Equal compares two SimpleComparison operations
func (s *SimpleComparison) Equal(other *SimpleComparison) bool {
	if s == nil && other == nil {
		return true
	}
	if s == nil || other == nil {
		return false
	}

	return s.Op.Equal(other.Op) && s.Addition.Equal(other.Addition)
}

// Equal compares two SimpleComparisonOp
func (s *SimpleComparisonOp) Equal(other *SimpleComparisonOp) bool {
	if s == nil && other == nil {
		return true
	}
	if s == nil || other == nil {
		return false
	}

	return s.Eq == other.Eq &&
		s.NotEq == other.NotEq &&
		s.LtEq == other.LtEq &&
		s.GtEq == other.GtEq &&
		s.Lt == other.Lt &&
		s.Gt == other.Gt &&
		s.Like == other.Like &&
		s.NotLike == other.NotLike
}

// Equal compares two IsNullExpr
func (i *IsNullExpr) Equal(other *IsNullExpr) bool {
	if i == nil && other == nil {
		return true
	}
	if i == nil || other == nil {
		return false
	}

	return i.Is == other.Is && i.Not == other.Not && i.Null == other.Null
}

// Equal compares two Addition expressions
func (a *AdditionExpression) Equal(other *AdditionExpression) bool {
	if a == nil && other == nil {
		return true
	}
	if a == nil || other == nil {
		return false
	}

	if !a.Multiplication.Equal(other.Multiplication) {
		return false
	}

	if len(a.Rest) != len(other.Rest) {
		return false
	}

	for i := range a.Rest {
		if a.Rest[i].Op != other.Rest[i].Op || !a.Rest[i].Multiplication.Equal(other.Rest[i].Multiplication) {
			return false
		}
	}

	return true
}

// Equal compares two Multiplication expressions
func (m *MultiplicationExpression) Equal(other *MultiplicationExpression) bool {
	if m == nil && other == nil {
		return true
	}
	if m == nil || other == nil {
		return false
	}

	if !m.Unary.Equal(other.Unary) {
		return false
	}

	if len(m.Rest) != len(other.Rest) {
		return false
	}

	for i := range m.Rest {
		if m.Rest[i].Op != other.Rest[i].Op || !m.Rest[i].Unary.Equal(other.Rest[i].Unary) {
			return false
		}
	}

	return true
}

// Equal compares two Unary expressions
func (u *UnaryExpression) Equal(other *UnaryExpression) bool {
	if u == nil && other == nil {
		return true
	}
	if u == nil || other == nil {
		return false
	}

	return u.Op == other.Op && u.Primary.Equal(other.Primary)
}

// Equal compares two Primary expressions
func (p *PrimaryExpression) Equal(other *PrimaryExpression) bool {
	if p == nil && other == nil {
		return true
	}
	if p == nil || other == nil {
		return false
	}

	// Check Literal
	if (p.Literal != nil) != (other.Literal != nil) {
		return false
	}
	if p.Literal != nil && !p.Literal.Equal(other.Literal) {
		return false
	}

	// Check Identifier
	if (p.Identifier != nil) != (other.Identifier != nil) {
		return false
	}
	if p.Identifier != nil && !p.Identifier.Equal(other.Identifier) {
		return false
	}

	// Check Function
	if (p.Function != nil) != (other.Function != nil) {
		return false
	}
	if p.Function != nil && !p.Function.Equal(other.Function) {
		return false
	}

	// Check Parentheses
	if (p.Parentheses != nil) != (other.Parentheses != nil) {
		return false
	}
	if p.Parentheses != nil && !p.Parentheses.Expression.Equal(&other.Parentheses.Expression) {
		return false
	}

	// For other types, do basic comparison
	return true
}

// Equal compares two Literal values
func (l *Literal) Equal(other *Literal) bool {
	if eq, done := compare.NilCheck(l, other); !done {
		return eq
	}

	return compare.Pointers(l.StringValue, other.StringValue) &&
		compare.Pointers(l.Number, other.Number) &&
		compare.Pointers(l.Boolean, other.Boolean) &&
		l.Null == other.Null
}

// Equal compares two IdentifierExpr
func (i *IdentifierExpr) Equal(other *IdentifierExpr) bool {
	if i == nil && other == nil {
		return true
	}
	if i == nil || other == nil {
		return false
	}

	// Compare Database
	if (i.Database != nil) != (other.Database != nil) {
		return false
	}
	if i.Database != nil && *i.Database != *other.Database {
		return false
	}

	// Compare Table
	if (i.Table != nil) != (other.Table != nil) {
		return false
	}
	if i.Table != nil && *i.Table != *other.Table {
		return false
	}

	// Compare Name
	return i.Name == other.Name
}

// Equal compares two FunctionCall
func (f *FunctionCall) Equal(other *FunctionCall) bool {
	if f == nil && other == nil {
		return true
	}
	if f == nil || other == nil {
		return false
	}

	if f.Name != other.Name {
		return false
	}

	if len(f.FirstParentheses) != len(other.FirstParentheses) {
		return false
	}
	for i := range f.FirstParentheses {
		if !f.FirstParentheses[i].Equal(&other.FirstParentheses[i]) {
			return false
		}
	}

	if len(f.SecondParentheses) != len(other.SecondParentheses) {
		return false
	}
	for i := range f.SecondParentheses {
		if !f.SecondParentheses[i].Equal(&other.SecondParentheses[i]) {
			return false
		}
	}

	return true
}

// Equal compares two FunctionArg
func (f *FunctionArg) Equal(other *FunctionArg) bool {
	if f == nil && other == nil {
		return true
	}
	if f == nil || other == nil {
		return false
	}

	// Compare Star
	if (f.Star != nil) != (other.Star != nil) {
		return false
	}
	if f.Star != nil && *f.Star != *other.Star {
		return false
	}

	// Compare Expression
	if (f.Expression != nil) != (other.Expression != nil) {
		return false
	}
	if f.Expression != nil && !f.Expression.Equal(other.Expression) {
		return false
	}

	return true
}

func (i InExpression) String() string {
	if len(i.List) > 0 {
		var results strings.Builder
		results.WriteString("(")

		for idx, expr := range i.List {
			if idx > 0 {
				results.WriteString(", ")
			}
			results.WriteString(expr.String())
		}

		results.WriteString(")")
		return results.String()
	}

	if i.Array != nil {
		return i.Array.String()
	}

	if i.Subquery != nil {
		return i.Subquery.String()
	}

	return "()"
}

func (b BetweenExpression) String() string {
	return b.Low.String() + " AND " + b.High.String()
}

func (s Subquery) String() string {
	// For now, return a simple representation
	// A full implementation would render the complete SELECT statement
	return "(SELECT ...)"
}

// formatParametricFunctionForExpression formats a function call within type parameters for expressions
func formatParametricFunctionForExpression(fn *ParametricFunction) string {
	if fn == nil {
		return ""
	}

	var results strings.Builder
	results.WriteString(fn.Name + "(")

	for i, param := range fn.Parameters {
		if i > 0 {
			results.WriteString(", ")
		}
		if param.Function != nil {
			results.WriteString(formatParametricFunctionForExpression(param.Function))
		} else if param.String != nil {
			results.WriteString(*param.String)
		} else if param.Number != nil {
			results.WriteString(*param.Number)
		} else if param.Ident != nil {
			results.WriteString(*param.Ident)
		}
	}

	results.WriteString(")")
	return results.String()
}

// formatDataTypeForExpression formats a DataType for use in expressions
func formatDataTypeForExpression(dataType DataType) string {
	if dataType.Nullable != nil {
		return "Nullable(" + formatDataTypeForExpression(*dataType.Nullable.Type) + ")"
	}

	if dataType.Array != nil {
		return "Array(" + formatDataTypeForExpression(*dataType.Array.Type) + ")"
	}

	if dataType.Tuple != nil {
		var results strings.Builder
		results.WriteString("Tuple(")

		for i, element := range dataType.Tuple.Elements {
			if i > 0 {
				results.WriteString(", ")
			}
			if element.Name != nil {
				results.WriteString(*element.Name + " " + formatDataTypeForExpression(*element.Type))
			} else {
				results.WriteString(formatDataTypeForExpression(*element.UnnamedType))
			}
		}

		results.WriteString(")")
		return results.String()
	}

	if dataType.Map != nil {
		return "Map(" + formatDataTypeForExpression(*dataType.Map.KeyType) + ", " + formatDataTypeForExpression(*dataType.Map.ValueType) + ")"
	}

	if dataType.LowCardinality != nil {
		return "LowCardinality(" + formatDataTypeForExpression(*dataType.LowCardinality.Type) + ")"
	}

	//nolint:nestif // Complex nested logic needed for data type formatting in expressions
	if dataType.Simple != nil {
		var results strings.Builder
		results.WriteString(dataType.Simple.Name)

		if len(dataType.Simple.Parameters) > 0 {
			results.WriteString("(")
			for i, param := range dataType.Simple.Parameters {
				if i > 0 {
					results.WriteString(", ")
				}
				if param.Function != nil {
					results.WriteString(formatParametricFunctionForExpression(param.Function))
				} else if param.String != nil {
					results.WriteString(*param.String)
				} else if param.Number != nil {
					results.WriteString(*param.Number)
				} else if param.Ident != nil {
					results.WriteString(*param.Ident)
				}
			}

			results.WriteString(")")
		}

		return results.String()
	}

	return ""
}
