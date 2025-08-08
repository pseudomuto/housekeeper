package parser

type (
	// Expression represents any ClickHouse expression with proper precedence handling
	// Precedence levels (lowest to highest):
	// 1. OR
	// 2. AND
	// 3. NOT
	// 4. Comparison (=, !=, <, >, <=, >=, LIKE, IN, BETWEEN)
	// 5. Addition/Subtraction (+, -)
	// 6. Multiplication/Division/Modulo (*, /, %)
	// 7. Unary (+, -, NOT)
	// 8. Primary (literals, identifiers, functions, parentheses)
	Expression struct {
		Or *OrExpression `parser:"@@"`
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
		Case        *CaseExpression    `parser:"| @@"`
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

	// FunctionCall represents function invocations
	FunctionCall struct {
		Name      string        `parser:"@(Ident | BacktickIdent)"`
		Arguments []FunctionArg `parser:"'(' (@@ (',' @@)*)? ')'"`
		Over      *OverClause   `parser:"@@?"`
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
	// For now, capture the content as raw text to avoid recursion issues
	CaseExpression struct {
		Case    string `parser:"'CASE'"`
		Content string `parser:"@(~'END')+"`
		End     string `parser:"'END'"`
	}

	// WhenClause represents a WHEN clause in CASE
	WhenClause struct {
		When      string               `parser:"'WHEN'"`
		Condition ComparisonExpression `parser:"@@"`
		Then      string               `parser:"'THEN'"`
		Result    ComparisonExpression `parser:"@@"`
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
func (e Expression) String() string {
	if e.Or != nil {
		return e.Or.String()
	}
	return "expression"
}

// String returns the string representation of an OrExpression with proper OR operator placement.
func (o OrExpression) String() string {
	if o.And != nil {
		result := o.And.String()
		for _, rest := range o.Rest {
			result += " OR " + rest.And.String()
		}
		return result
	}
	return ""
}

// String returns the string representation of an AndExpression with proper AND operator placement.
func (a AndExpression) String() string {
	if a.Not != nil {
		result := a.Not.String()
		for _, rest := range a.Rest {
			result += " AND " + rest.Not.String()
		}
		return result
	}
	return ""
}

// String returns the string representation of a NotExpression with optional NOT prefix.
func (n NotExpression) String() string {
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
func (c ComparisonExpression) String() string {
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
func (c SimpleComparisonOp) String() string {
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

func (a AdditionExpression) String() string {
	if a.Multiplication != nil {
		result := a.Multiplication.String()
		for _, rest := range a.Rest {
			result += " " + rest.Op + " " + rest.Multiplication.String()
		}
		return result
	}
	return ""
}

func (m MultiplicationExpression) String() string {
	if m.Unary != nil {
		result := m.Unary.String()
		for _, rest := range m.Rest {
			result += " " + rest.Op + " " + rest.Unary.String()
		}
		return result
	}
	return ""
}

func (u UnaryExpression) String() string {
	prefix := u.Op
	if u.Primary != nil {
		return prefix + u.Primary.String()
	}
	return prefix
}

func (p PrimaryExpression) String() string {
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
	if p.Case != nil {
		return p.Case.String()
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
func (l Literal) String() string {
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
func (i IdentifierExpr) String() string {
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
func (f FunctionCall) String() string {
	result := f.Name + "("
	for i, arg := range f.Arguments {
		if i > 0 {
			result += ", "
		}
		result += arg.String()
	}
	result += ")"
	return result
}

func (a FunctionArg) String() string {
	if a.Star != nil {
		return "*"
	}
	if a.Expression != nil {
		return a.Expression.String()
	}
	return ""
}

func (t TupleExpression) String() string {
	result := "("
	for i, elem := range t.Elements {
		if i > 0 {
			result += ","
		}
		result += elem.String()
	}
	result += ")"
	return result
}

func (a ArrayExpression) String() string {
	result := "["
	for i, elem := range a.Elements {
		if i > 0 {
			result += ", "
		}
		result += elem.String()
	}
	result += "]"
	return result
}

func (i IntervalExpr) String() string {
	return "INTERVAL " + i.Value + " " + i.Unit
}

func (c CaseExpression) String() string {
	return "CASE " + c.Content + " END"
}

func (w WhenClause) String() string {
	return "WHEN " + w.Condition.String() + " THEN " + w.Result.String()
}

func (c CastExpression) String() string {
	return "CAST(" + c.Expression.String() + " AS " + formatDataTypeForExpression(c.Type) + ")"
}

func (e ExtractExpression) String() string {
	return "EXTRACT(" + e.Part + " FROM " + e.Expr.String() + ")"
}

func (i InExpression) String() string {
	if len(i.List) > 0 {
		result := "("
		for idx, expr := range i.List {
			if idx > 0 {
				result += ", "
			}
			result += expr.String()
		}
		result += ")"
		return result
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

	result := fn.Name + "("
	for i, param := range fn.Parameters {
		if i > 0 {
			result += ", "
		}
		if param.Function != nil {
			result += formatParametricFunctionForExpression(param.Function)
		} else if param.String != nil {
			result += *param.String
		} else if param.Number != nil {
			result += *param.Number
		} else if param.Ident != nil {
			result += *param.Ident
		}
	}
	result += ")"
	return result
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
		result := "Tuple("
		for i, element := range dataType.Tuple.Elements {
			if i > 0 {
				result += ", "
			}
			if element.Name != nil {
				result += *element.Name + " " + formatDataTypeForExpression(*element.Type)
			} else {
				result += formatDataTypeForExpression(*element.UnnamedType)
			}
		}
		return result + ")"
	}
	if dataType.Map != nil {
		return "Map(" + formatDataTypeForExpression(*dataType.Map.KeyType) + ", " + formatDataTypeForExpression(*dataType.Map.ValueType) + ")"
	}
	if dataType.LowCardinality != nil {
		return "LowCardinality(" + formatDataTypeForExpression(*dataType.LowCardinality.Type) + ")"
	}
	//nolint:nestif // Complex nested logic needed for data type formatting in expressions
	if dataType.Simple != nil {
		result := dataType.Simple.Name
		if len(dataType.Simple.Parameters) > 0 {
			result += "("
			for i, param := range dataType.Simple.Parameters {
				if i > 0 {
					result += ", "
				}
				if param.Function != nil {
					result += formatParametricFunctionForExpression(param.Function)
				} else if param.String != nil {
					result += *param.String
				} else if param.Number != nil {
					result += *param.Number
				} else if param.Ident != nil {
					result += *param.Ident
				}
			}
			result += ")"
		}
		return result
	}
	return ""
}
