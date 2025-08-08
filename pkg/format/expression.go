package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// formatExpression formats an expression
func (f *Formatter) formatExpression(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}
	return expr.String()
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
