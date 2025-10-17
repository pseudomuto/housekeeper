package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// formatExpression formats an expression with proper identifier backticking
func (f *Formatter) formatExpression(expr *parser.Expression) string {
	return f.formatExpressionWithContext(expr, false, 0)
}

// formatExpressionWithContext formats an expression with multi-line context
func (f *Formatter) formatExpressionWithContext(expr *parser.Expression, multilineContext bool, baseIndent int) string {
	if expr == nil {
		return ""
	}
	if expr.Case != nil {
		return f.formatCaseExpression(expr.Case)
	}
	return f.formatOrExpressionWithContext(expr.Or, multilineContext, baseIndent)
}

// formatOrExpressionWithContext formats an OR expression with multi-line context
func (f *Formatter) formatOrExpressionWithContext(or *parser.OrExpression, multilineContext bool, baseIndent int) string {
	if or == nil {
		return ""
	}

	result := f.formatAndExpressionWithContext(or.And, multilineContext, baseIndent)
	for _, rest := range or.Rest {
		result += " OR " + f.formatAndExpressionWithContext(rest.And, multilineContext, baseIndent)
	}
	return result
}

// formatAndExpressionWithContext formats an AND expression with multi-line context
func (f *Formatter) formatAndExpressionWithContext(and *parser.AndExpression, multilineContext bool, baseIndent int) string {
	if and == nil {
		return ""
	}

	result := f.formatNotExpressionWithContext(and.Not, multilineContext, baseIndent)
	for _, rest := range and.Rest {
		result += " AND " + f.formatNotExpressionWithContext(rest.Not, multilineContext, baseIndent)
	}
	return result
}

// formatNotExpressionWithContext formats a NOT expression with multi-line context
func (f *Formatter) formatNotExpressionWithContext(not *parser.NotExpression, multilineContext bool, baseIndent int) string {
	if not == nil {
		return ""
	}
	if not.Not {
		return "NOT " + f.formatComparisonExpressionWithContext(not.Comparison, multilineContext, baseIndent)
	}
	return f.formatComparisonExpressionWithContext(not.Comparison, multilineContext, baseIndent)
}

// formatComparisonExpressionWithContext formats a comparison expression with multi-line context
func (f *Formatter) formatComparisonExpressionWithContext(comp *parser.ComparisonExpression, multilineContext bool, baseIndent int) string {
	if comp == nil {
		return ""
	}

	left := f.formatAdditionExpressionWithContext(comp.Addition, multilineContext, baseIndent)

	// Handle IS NULL/IS NOT NULL
	if comp.IsNull != nil {
		if comp.IsNull.Not {
			return left + " IS NOT NULL"
		}
		return left + " IS NULL"
	}

	// Handle other comparisons
	if comp.Rest != nil { // nolint nestif
		if comp.Rest.SimpleOp != nil {
			op := f.formatSimpleComparisonOp(comp.Rest.SimpleOp.Op)
			right := f.formatAdditionExpressionWithContext(comp.Rest.SimpleOp.Addition, multilineContext, baseIndent)
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

// formatAdditionExpressionWithContext formats an addition expression with multi-line context
func (f *Formatter) formatAdditionExpressionWithContext(add *parser.AdditionExpression, multilineContext bool, baseIndent int) string {
	if add == nil {
		return ""
	}

	result := f.formatMultiplicationExpressionWithContext(add.Multiplication, multilineContext, baseIndent)

	for _, rest := range add.Rest {
		result += " " + rest.Op + " " + f.formatMultiplicationExpressionWithContext(rest.Multiplication, multilineContext, baseIndent)
	}

	return result
}

// formatMultiplicationExpressionWithContext formats a multiplication expression with multi-line context
func (f *Formatter) formatMultiplicationExpressionWithContext(mul *parser.MultiplicationExpression, multilineContext bool, baseIndent int) string {
	if mul == nil {
		return ""
	}

	result := f.formatUnaryExpressionWithContext(mul.Unary, multilineContext, baseIndent)

	for _, rest := range mul.Rest {
		result += " " + rest.Op + " " + f.formatUnaryExpressionWithContext(rest.Unary, multilineContext, baseIndent)
	}

	return result
}

// formatUnaryExpressionWithContext formats a unary expression with multi-line context
func (f *Formatter) formatUnaryExpressionWithContext(unary *parser.UnaryExpression, multilineContext bool, baseIndent int) string {
	if unary == nil {
		return ""
	}

	if unary.Op != "" {
		return unary.Op + f.formatPrimaryExpressionWithContext(unary.Primary, multilineContext, baseIndent)
	}
	return f.formatPrimaryExpressionWithContext(unary.Primary, multilineContext, baseIndent)
}

// formatPrimaryExpressionWithContext formats a primary expression with multi-line context
func (f *Formatter) formatPrimaryExpressionWithContext(primary *parser.PrimaryExpression, multilineContext bool, baseIndent int) string {
	if primary == nil {
		return ""
	}

	switch {
	case primary.Literal != nil:
		return f.formatLiteral(primary.Literal)
	case primary.Identifier != nil:
		return f.formatIdentifierExpr(primary.Identifier)
	case primary.Function != nil:
		return f.formatFunctionCallWithContext(primary.Function, multilineContext)
	case primary.Parentheses != nil:
		return "(" + f.formatExpressionWithContext(&primary.Parentheses.Expression, multilineContext, baseIndent) + ")"
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

	// Special case: don't backtick boolean literals that are parsed as identifiers
	if id.Database == nil && id.Table == nil && isBooleanLiteral(id.Name) {
		parts = append(parts, id.Name)
	} else {
		parts = append(parts, f.identifier(id.Name))
	}

	return strings.Join(parts, ".")
}

// isBooleanLiteral checks if a name represents a boolean literal
func isBooleanLiteral(name string) bool {
	return name == "true" || name == "false"
}

// formatFunctionCallWithContext formats a function call with multi-line context
func (f *Formatter) formatFunctionCallWithContext(fn *parser.FunctionCall, multilineContext bool) string {
	if fn == nil {
		return ""
	}

	// Check if this function should be formatted multi-line
	shouldUseMultiline := f.shouldFormatFunctionMultiline(fn, multilineContext)

	if shouldUseMultiline {
		return f.formatFunctionCallMultiline(fn)
	}

	// Use original single-line formatting
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

	// Handle OVER clause for window functions
	if fn.Over != nil {
		result += " " + fn.Over.String()
	}

	return result
}

// shouldFormatFunctionMultiline determines if a function should be formatted multi-line
func (f *Formatter) shouldFormatFunctionMultiline(fn *parser.FunctionCall, multilineContext bool) bool {
	if !f.options.MultilineFunctions {
		return false
	}

	// Always format multi-line if in multiline context
	if multilineContext {
		return true
	}

	// Check if function name is in the always-multiline list
	for _, name := range f.options.MultilineFunctionNames {
		if strings.EqualFold(fn.Name, name) {
			return true
		}
	}

	// Check if function has enough arguments to trigger multi-line
	argCount := len(fn.FirstParentheses) + len(fn.SecondParentheses)
	return argCount >= f.options.FunctionArgThreshold
}

// formatFunctionCallMultiline formats a function call across multiple lines
func (f *Formatter) formatFunctionCallMultiline(fn *parser.FunctionCall) string {
	// Use a reasonable indentation for readability
	indentStr := strings.Repeat(" ", f.options.FunctionIndentSize)

	result := fn.Name + "(\n"

	// Format first parentheses arguments
	if len(fn.FirstParentheses) > 0 {
		if f.shouldUsePairedFormatting(fn) {
			result += f.formatArgumentsPaired(fn.FirstParentheses, indentStr)
		} else {
			result += f.formatArgumentsLineByLine(fn.FirstParentheses, indentStr)
		}
	}

	result += ")"

	// Handle second parentheses for parameterized functions
	if len(fn.SecondParentheses) > 0 {
		result += "(\n"
		// Always use line-by-line for second parentheses (less common, simpler approach)
		result += f.formatArgumentsLineByLine(fn.SecondParentheses, indentStr)
		result += ")"
	}

	// Handle OVER clause for window functions
	if fn.Over != nil {
		result += " " + fn.Over.String()
	}

	return result
}

// shouldUsePairedFormatting determines if a function should use smart argument pairing
func (f *Formatter) shouldUsePairedFormatting(fn *parser.FunctionCall) bool {
	if !f.options.SmartFunctionPairing {
		return false
	}

	// Check if function name is in the paired functions list
	for _, name := range f.options.PairedFunctionNames {
		if strings.EqualFold(fn.Name, name) {
			return true
		}
	}

	return false
}

// formatArgumentsPaired formats arguments in pairs (e.g., condition-value pairs for multiIf)
func (f *Formatter) formatArgumentsPaired(args []parser.FunctionArg, indentStr string) string {
	result := ""
	pairSize := f.options.PairSize

	for i := 0; i < len(args); i += pairSize {
		result += indentStr

		// Add arguments in pairs
		for j := 0; j < pairSize && i+j < len(args); j++ {
			if j > 0 {
				result += ", "
			}
			result += f.formatFunctionArg(&args[i+j])
		}

		// Add comma unless it's the last group
		if i+pairSize < len(args) {
			result += ","
		}
		result += "\n"
	}

	return result
}

// formatArgumentsLineByLine formats arguments with each argument on its own line
func (f *Formatter) formatArgumentsLineByLine(args []parser.FunctionArg, indentStr string) string {
	result := ""

	for i, arg := range args {
		result += indentStr + f.formatFunctionArg(&arg)
		if i < len(args)-1 {
			result += ","
		}
		result += "\n"
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

	elements := make([]string, len(arr.Elements))
	for i, elem := range arr.Elements {
		elements[i] = f.formatExpression(&elem)
	}

	return "[" + strings.Join(elements, ", ") + "]"
}

// formatTupleExpression formats a tuple expression
func (f *Formatter) formatTupleExpression(tuple *parser.TupleExpression) string {
	if tuple == nil || len(tuple.Elements) == 0 {
		return "()"
	}

	elements := make([]string, len(tuple.Elements))
	for i, elem := range tuple.Elements {
		elements[i] = f.formatExpression(&elem)
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
		// Only add parentheses when there are parameters
		if len(codecSpec.Parameters) > 0 {
			spec += "("
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
			spec += strings.Join(params, ", ") + ")"
		}
		codecSpecs = append(codecSpecs, spec)
	}

	return f.keyword("CODEC") + "(" + strings.Join(codecSpecs, ", ") + ")"
}
