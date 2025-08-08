package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// formatExpression formats an expression with proper identifier backticking
func (f *Formatter) formatExpression(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}
	return f.formatOrExpression(expr.Or)
}

// formatOrExpression formats an OR expression
func (f *Formatter) formatOrExpression(or *parser.OrExpression) string {
	if or == nil {
		return ""
	}
	
	result := f.formatAndExpression(or.And)
	for _, rest := range or.Rest {
		result += " OR " + f.formatAndExpression(rest.And)
	}
	return result
}

// formatAndExpression formats an AND expression
func (f *Formatter) formatAndExpression(and *parser.AndExpression) string {
	if and == nil {
		return ""
	}
	
	result := f.formatNotExpression(and.Not)
	for _, rest := range and.Rest {
		result += " AND " + f.formatNotExpression(rest.Not)
	}
	return result
}

// formatNotExpression formats a NOT expression
func (f *Formatter) formatNotExpression(not *parser.NotExpression) string {
	if not == nil {
		return ""
	}
	if not.Not {
		return "NOT " + f.formatComparisonExpression(not.Comparison)
	}
	return f.formatComparisonExpression(not.Comparison)
}

// formatComparisonExpression formats a comparison expression
func (f *Formatter) formatComparisonExpression(comp *parser.ComparisonExpression) string {
	if comp == nil {
		return ""
	}
	
	left := f.formatAdditionExpression(comp.Addition)
	
	// Handle IS NULL/IS NOT NULL
	if comp.IsNull != nil {
		if comp.IsNull.Not {
			return left + " IS NOT NULL"
		}
		return left + " IS NULL"
	}
	
	// Handle other comparisons
	if comp.Rest != nil {
		if comp.Rest.SimpleOp != nil {
			op := f.formatSimpleComparisonOp(comp.Rest.SimpleOp.Op)
			right := f.formatAdditionExpression(comp.Rest.SimpleOp.Addition)
			return left + " " + op + " " + right
		} else if comp.Rest.InOp != nil {
			inOp := "IN"
			if comp.Rest.InOp.Not {
				inOp = "NOT IN"
			}
			inExpr := f.formatInExpression(comp.Rest.InOp.Expr)
			return left + " " + inOp + " " + inExpr
		} else if comp.Rest.BetweenOp != nil {
			betweenOp := "BETWEEN"
			if comp.Rest.BetweenOp.Not {
				betweenOp = "NOT BETWEEN"
			}
			betweenExpr := f.formatBetweenExpression(comp.Rest.BetweenOp.Expr)
			return left + " " + betweenOp + " " + betweenExpr
		}
	}
	
	return left
}

// formatSimpleComparisonOp formats a simple comparison operator
func (f *Formatter) formatSimpleComparisonOp(op *parser.SimpleComparisonOp) string {
	if op == nil {
		return ""
	}
	switch {
	case op.Eq:
		return "="
	case op.NotEq:
		return "!="
	case op.LtEq:
		return "<="
	case op.GtEq:
		return ">="
	case op.Lt:
		return "<"
	case op.Gt:
		return ">"
	case op.Like:
		return "LIKE"
	case op.NotLike:
		return "NOT LIKE"
	default:
		return ""
	}
}

// formatInExpression formats an IN expression
func (f *Formatter) formatInExpression(in *parser.InExpression) string {
	if in == nil {
		return ""
	}
	// This would need to be implemented based on the InExpression structure
	// For now, fall back to String()
	return in.String()
}

// formatBetweenExpression formats a BETWEEN expression
func (f *Formatter) formatBetweenExpression(between *parser.BetweenExpression) string {
	if between == nil {
		return ""
	}
	// This would need to be implemented based on the BetweenExpression structure
	// For now, fall back to String()
	return between.String()
}

// formatAdditionExpression formats an addition expression
func (f *Formatter) formatAdditionExpression(add *parser.AdditionExpression) string {
	if add == nil {
		return ""
	}
	
	result := f.formatMultiplicationExpression(add.Multiplication)
	
	for _, rest := range add.Rest {
		result += " " + rest.Op + " " + f.formatMultiplicationExpression(rest.Multiplication)
	}
	
	return result
}

// formatMultiplicationExpression formats a multiplication expression
func (f *Formatter) formatMultiplicationExpression(mul *parser.MultiplicationExpression) string {
	if mul == nil {
		return ""
	}
	
	result := f.formatUnaryExpression(mul.Unary)
	
	for _, rest := range mul.Rest {
		result += " " + rest.Op + " " + f.formatUnaryExpression(rest.Unary)
	}
	
	return result
}

// formatUnaryExpression formats a unary expression
func (f *Formatter) formatUnaryExpression(unary *parser.UnaryExpression) string {
	if unary == nil {
		return ""
	}
	
	if unary.Op != "" {
		return unary.Op + f.formatPrimaryExpression(unary.Primary)
	}
	return f.formatPrimaryExpression(unary.Primary)
}

// formatPrimaryExpression formats a primary expression
func (f *Formatter) formatPrimaryExpression(primary *parser.PrimaryExpression) string {
	if primary == nil {
		return ""
	}
	
	switch {
	case primary.Literal != nil:
		return f.formatLiteral(primary.Literal)
	case primary.Identifier != nil:
		return f.formatIdentifierExpr(primary.Identifier)
	case primary.Function != nil:
		return f.formatFunctionCall(primary.Function)
	case primary.Parentheses != nil:
		return "(" + f.formatExpression(&primary.Parentheses.Expression) + ")"
	case primary.Tuple != nil:
		return f.formatTupleExpression(primary.Tuple)
	case primary.Array != nil:
		return f.formatArrayExpression(primary.Array)
	case primary.Cast != nil:
		return f.formatCastExpression(primary.Cast)
	case primary.Interval != nil:
		return f.formatIntervalExpression(primary.Interval)
	case primary.Extract != nil:
		return f.formatExtractExpression(primary.Extract)
	case primary.Case != nil:
		return f.formatCaseExpression(primary.Case)
	default:
		return ""
	}
}

// formatLiteral formats a literal value
func (f *Formatter) formatLiteral(lit *parser.Literal) string {
	if lit == nil {
		return ""
	}
	
	switch {
	case lit.StringValue != nil:
		return *lit.StringValue
	case lit.Number != nil:
		return *lit.Number
	case lit.Boolean != nil:
		return *lit.Boolean
	case lit.Null:
		return "NULL"
	default:
		return ""
	}
}

// formatIdentifierExpr formats an identifier expression with proper backticking
func (f *Formatter) formatIdentifierExpr(id *parser.IdentifierExpr) string {
	if id == nil {
		return ""
	}
	
	var parts []string
	if id.Database != nil {
		parts = append(parts, f.identifier(*id.Database))
	}
	if id.Table != nil {
		parts = append(parts, f.identifier(*id.Table))
	}
	parts = append(parts, f.identifier(id.Name))
	
	return strings.Join(parts, ".")
}

// formatFunctionCall formats a function call
func (f *Formatter) formatFunctionCall(fn *parser.FunctionCall) string {
	if fn == nil {
		return ""
	}
	
	result := fn.Name + "("
	if len(fn.FirstParentheses) > 0 {
		var args []string
		for _, arg := range fn.FirstParentheses {
			args = append(args, f.formatFunctionArg(&arg))
		}
		result += strings.Join(args, ", ")
	}
	result += ")"
	
	// Handle second parentheses for parameterized functions
	if len(fn.SecondParentheses) > 0 {
		result += "("
		var args []string
		for _, arg := range fn.SecondParentheses {
			args = append(args, f.formatFunctionArg(&arg))
		}
		result += strings.Join(args, ", ")
		result += ")"
	}
	
	// Handle OVER clause for window functions - fallback to string for now
	if fn.Over != nil {
		// For now, we'll omit the OVER clause formatting to avoid complexity
		// This is a limitation that can be addressed later
	}
	
	return result
}

// formatFunctionArg formats a function argument
func (f *Formatter) formatFunctionArg(arg *parser.FunctionArg) string {
	if arg == nil {
		return ""
	}
	if arg.Star != nil {
		return "*"
	}
	if arg.Expression != nil {
		return f.formatExpression(arg.Expression)
	}
	return arg.String()
}


// formatArrayExpression formats an array expression
func (f *Formatter) formatArrayExpression(arr *parser.ArrayExpression) string {
	if arr == nil || len(arr.Elements) == 0 {
		return "[]"
	}
	
	var elements []string
	for _, elem := range arr.Elements {
		elements = append(elements, f.formatExpression(&elem))
	}
	return "[" + strings.Join(elements, ", ") + "]"
}

// formatTupleExpression formats a tuple expression
func (f *Formatter) formatTupleExpression(tuple *parser.TupleExpression) string {
	if tuple == nil || len(tuple.Elements) == 0 {
		return "()"
	}
	
	var elements []string
	for _, elem := range tuple.Elements {
		elements = append(elements, f.formatExpression(&elem))
	}
	return "(" + strings.Join(elements, ", ") + ")"
}

// Placeholder formatters for unsupported expression types (fall back to String())
func (f *Formatter) formatCastExpression(cast *parser.CastExpression) string {
	if cast == nil {
		return ""
	}
	return cast.String()
}

func (f *Formatter) formatIntervalExpression(interval *parser.IntervalExpr) string {
	if interval == nil {
		return ""
	}
	return interval.String()
}

func (f *Formatter) formatExtractExpression(extract *parser.ExtractExpression) string {
	if extract == nil {
		return ""
	}
	return extract.String()
}

func (f *Formatter) formatCaseExpression(caseExpr *parser.CaseExpression) string {
	if caseExpr == nil {
		return ""
	}
	return caseExpr.String()
}

// formatDataType formats a data type specification
func (f *Formatter) formatDataType(dataType *parser.DataType) string {
	if dataType == nil {
		return ""
	}

	if dataType.Nullable != nil {
		return "Nullable(" + f.formatDataType(dataType.Nullable.Type) + ")"
	}
	if dataType.Array != nil {
		return "Array(" + f.formatDataType(dataType.Array.Type) + ")"
	}
	if dataType.Tuple != nil {
		return f.formatTupleDataType(dataType.Tuple)
	}
	if dataType.Nested != nil {
		return f.formatNestedDataType(dataType.Nested)
	}
	if dataType.Map != nil {
		keyType := f.formatDataType(dataType.Map.KeyType)
		valueType := f.formatDataType(dataType.Map.ValueType)
		return "Map(" + keyType + ", " + valueType + ")"
	}
	if dataType.LowCardinality != nil {
		return "LowCardinality(" + f.formatDataType(dataType.LowCardinality.Type) + ")"
	}
	if dataType.Simple != nil {
		return f.formatSimpleDataType(dataType.Simple)
	}
	return ""
}

// formatSimpleDataType formats a simple data type
func (f *Formatter) formatSimpleDataType(simple *parser.SimpleType) string {
	if simple == nil {
		return ""
	}

	result := simple.Name
	if len(simple.Parameters) > 0 {
		result += "("
		var params []string
		for _, param := range simple.Parameters {
			if param.Function != nil {
				params = append(params, f.formatParametricFunction(param.Function))
			} else if param.String != nil {
				params = append(params, *param.String)
			} else if param.Number != nil {
				params = append(params, *param.Number)
			} else if param.Ident != nil {
				params = append(params, *param.Ident)
			}
		}
		result += strings.Join(params, ", ")
		result += ")"
	}
	return result
}

// formatParametricFunction formats a function call within type parameters
func (f *Formatter) formatParametricFunction(fn *parser.ParametricFunction) string {
	if fn == nil {
		return ""
	}

	result := fn.Name + "("
	var params []string
	for _, param := range fn.Parameters {
		if param.Function != nil {
			params = append(params, f.formatParametricFunction(param.Function))
		} else if param.String != nil {
			params = append(params, *param.String)
		} else if param.Number != nil {
			params = append(params, *param.Number)
		} else if param.Ident != nil {
			params = append(params, *param.Ident)
		}
	}
	result += strings.Join(params, ", ")
	result += ")"
	return result
}

// formatTupleDataType formats a tuple data type
func (f *Formatter) formatTupleDataType(tuple *parser.TupleType) string {
	if tuple == nil || len(tuple.Elements) == 0 {
		return "Tuple()"
	}

	var elements []string
	for _, element := range tuple.Elements {
		if element.Name != nil {
			// Named tuple element
			elementStr := f.identifier(*element.Name) + " " + f.formatDataType(element.Type)
			elements = append(elements, elementStr)
		} else {
			// Unnamed tuple element
			elements = append(elements, f.formatDataType(element.UnnamedType))
		}
	}

	return "Tuple(" + strings.Join(elements, ", ") + ")"
}

// formatNestedDataType formats a nested data type
func (f *Formatter) formatNestedDataType(nested *parser.NestedType) string {
	if nested == nil || len(nested.Columns) == 0 {
		return "Nested()"
	}

	columns := make([]string, 0, len(nested.Columns))
	for _, col := range nested.Columns {
		columnStr := f.identifier(col.Name) + " " + f.formatDataType(col.Type)
		columns = append(columns, columnStr)
	}

	return "Nested(" + strings.Join(columns, ", ") + ")"
}

// formatCodec formats a codec specification
func (f *Formatter) formatCodec(codec *parser.CodecClause) string {
	if codec == nil || len(codec.Codecs) == 0 {
		return ""
	}

	codecSpecs := make([]string, 0, len(codec.Codecs))
	for _, codecSpec := range codec.Codecs {
		spec := codecSpec.Name
		// Always add parentheses for consistency
		spec += "("
		if len(codecSpec.Parameters) > 0 {
			var params []string
			for _, param := range codecSpec.Parameters {
				if param.String != nil {
					params = append(params, *param.String)
				} else if param.Number != nil {
					params = append(params, *param.Number)
				} else if param.Ident != nil {
					params = append(params, f.identifier(*param.Ident))
				}
			}
			spec += strings.Join(params, ", ")
		}
		spec += ")"
		codecSpecs = append(codecSpecs, spec)
	}

	return f.keyword("CODEC") + "(" + strings.Join(codecSpecs, ", ") + ")"
}
