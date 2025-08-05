package format

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// formatExpression formats an expression
func (f *formatter) formatExpression(expr *parser.Expression) string {
	if expr == nil {
		return ""
	}
	return expr.String()
}

// formatDataType formats a data type specification
func (f *formatter) formatDataType(dataType *parser.DataType) string {
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
func (f *formatter) formatSimpleDataType(simple *parser.SimpleType) string {
	if simple == nil {
		return ""
	}

	result := simple.Name
	if len(simple.Parameters) > 0 {
		result += "("
		var params []string
		for _, param := range simple.Parameters {
			if param.String != nil {
				params = append(params, *param.String)
			} else if param.Number != nil {
				params = append(params, *param.Number)
			} else if param.Ident != nil {
				params = append(params, f.identifier(*param.Ident))
			}
		}
		result += strings.Join(params, ", ")
		result += ")"
	}
	return result
}

// formatTupleDataType formats a tuple data type
func (f *formatter) formatTupleDataType(tuple *parser.TupleType) string {
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
func (f *formatter) formatNestedDataType(nested *parser.NestedType) string {
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
func (f *formatter) formatCodec(codec *parser.CodecClause) string {
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
