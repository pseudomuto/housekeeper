package format

import (
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// CreateNamedCollection formats a CREATE NAMED COLLECTION statement
func (f *Formatter) createNamedCollection(w io.Writer, stmt *parser.CreateNamedCollectionStmt) error {
	var lines []string

	// Build the header line
	var headerParts []string
	headerParts = append(headerParts, f.keyword("CREATE"))

	if stmt.OrReplace {
		headerParts = append(headerParts, f.keyword("OR REPLACE"))
	}

	headerParts = append(headerParts, f.keyword("NAMED COLLECTION"))

	if stmt.IfNotExists != nil {
		headerParts = append(headerParts, f.keyword("IF NOT EXISTS"))
	}

	headerParts = append(headerParts, f.identifier(stmt.Name))

	if stmt.OnCluster != nil {
		headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	headerParts = append(headerParts, f.keyword("AS"))

	lines = append(lines, strings.Join(headerParts, " "))

	// Format parameters
	f.formatCreateNamedCollectionParameters(&lines, stmt)

	// Add global override if present on its own line
	if stmt.GlobalOverride != nil {
		if stmt.GlobalOverride.NotOverridable {
			lines = append(lines, f.keyword("NOT OVERRIDABLE"))
		} else if stmt.GlobalOverride.Overridable {
			lines = append(lines, f.keyword("OVERRIDABLE"))
		}
	}

	// Add comment if present
	if stmt.Comment != nil {
		lines = append(lines, f.keyword("COMMENT")+" "+*stmt.Comment)
	}

	// Join lines and add semicolon
	result := strings.Join(lines, "\n") + ";"
	_, err := w.Write([]byte(result))
	return err
}

// AlterNamedCollection formats an ALTER NAMED COLLECTION statement
func (f *Formatter) alterNamedCollection(w io.Writer, stmt *parser.AlterNamedCollectionStmt) error {
	var lines []string

	// Build the header line
	var headerParts []string
	headerParts = append(headerParts, f.keyword("ALTER"))
	headerParts = append(headerParts, f.keyword("NAMED COLLECTION"))

	if stmt.IfExists != nil {
		headerParts = append(headerParts, f.keyword("IF EXISTS"))
	}

	headerParts = append(headerParts, f.identifier(stmt.Name))

	if stmt.OnCluster != nil {
		headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	lines = append(lines, strings.Join(headerParts, " "))

	// Format operations
	if stmt.Operations != nil {
		f.formatAlterNamedCollectionOperations(&lines, stmt.Operations)
	}

	// Join lines and add semicolon
	result := strings.Join(lines, "\n") + ";"
	_, err := w.Write([]byte(result))
	return err
}

// DropNamedCollection formats a DROP NAMED COLLECTION statement
func (f *Formatter) dropNamedCollection(w io.Writer, stmt *parser.DropNamedCollectionStmt) error {
	var headerParts []string
	headerParts = append(headerParts, f.keyword("DROP"))
	headerParts = append(headerParts, f.keyword("NAMED COLLECTION"))

	if stmt.IfExists != nil {
		headerParts = append(headerParts, f.keyword("IF EXISTS"))
	}

	headerParts = append(headerParts, f.identifier(stmt.Name))

	if stmt.OnCluster != nil {
		headerParts = append(headerParts, f.keyword("ON CLUSTER"), f.identifier(*stmt.OnCluster))
	}

	line := strings.Join(headerParts, " ") + ";"
	_, err := w.Write([]byte(line))
	return err
}

// formatCreateNamedCollectionParameters formats the parameters section of a CREATE NAMED COLLECTION statement
func (f *Formatter) formatCreateNamedCollectionParameters(lines *[]string, stmt *parser.CreateNamedCollectionStmt) {
	if len(stmt.Parameters) == 0 {
		return
	}

	for i, param := range stmt.Parameters {
		paramLine := f.formatSingleParameter(param, stmt.GlobalOverride == nil)
		if f.shouldAddComma(i, len(stmt.Parameters), stmt.GlobalOverride != nil) {
			paramLine += ","
		}
		*lines = append(*lines, paramLine)
	}
}

// formatAlterNamedCollectionOperations formats the operations section of an ALTER NAMED COLLECTION statement
func (f *Formatter) formatAlterNamedCollectionOperations(lines *[]string, ops *parser.AlterNamedCollectionOperations) {
	if len(ops.SetParams) > 0 {
		f.addSetOperation(lines, ops)
	} else if len(ops.DeleteParams) > 0 {
		f.addDeleteOperation(lines, ops.DeleteParams)
	}
}

// formatSingleParameter formats a single parameter with its override clause
func (f *Formatter) formatSingleParameter(param *parser.NamedCollectionParameter, allowOverride bool) string {
	paramLine := f.indent(1) + f.identifier(param.Key) + " = " + f.formatNamedCollectionValue(param.Value)

	if allowOverride && param.Override != nil {
		if param.Override.NotOverridable {
			paramLine += " " + f.keyword("NOT OVERRIDABLE")
		} else if param.Override.Overridable {
			paramLine += " " + f.keyword("OVERRIDABLE")
		}
	}

	return paramLine
}

// shouldAddComma determines if a comma should be added after a parameter
func (f *Formatter) shouldAddComma(index, total int, hasGlobalOverride bool) bool {
	return index < total-1 || hasGlobalOverride
}

// addSetOperation adds a SET operation to the lines
func (f *Formatter) addSetOperation(lines *[]string, ops *parser.AlterNamedCollectionOperations) {
	setLine := f.indent(1) + f.keyword("SET") + " "

	paramParts := make([]string, 0, len(ops.SetParams))
	for _, param := range ops.SetParams {
		paramLine := f.identifier(param.Key) + " = " + f.formatNamedCollectionValue(param.Value)

		if param.Override != nil {
			if param.Override.NotOverridable {
				paramLine += " " + f.keyword("NOT OVERRIDABLE")
			} else if param.Override.Overridable {
				paramLine += " " + f.keyword("OVERRIDABLE")
			}
		}

		paramParts = append(paramParts, paramLine)
	}

	setLine += strings.Join(paramParts, ", ")

	if len(ops.DeleteParams) > 0 {
		setLine += "\n" + f.indent(1) + f.keyword("DELETE") + " " + f.formatDeleteKeys(ops.DeleteParams)
	}

	*lines = append(*lines, setLine)
}

// addDeleteOperation adds a DELETE-only operation to the lines
func (f *Formatter) addDeleteOperation(lines *[]string, deleteParams []*string) {
	deleteKeys := f.formatDeleteKeys(deleteParams)
	*lines = append(*lines, f.indent(1)+f.keyword("DELETE")+" "+deleteKeys)
}

// formatDeleteKeys formats a list of delete parameter keys
func (f *Formatter) formatDeleteKeys(deleteParams []*string) string {
	deleteKeys := make([]string, len(deleteParams))
	for i, key := range deleteParams {
		deleteKeys[i] = f.identifier(*key)
	}
	return strings.Join(deleteKeys, ", ")
}

// formatNamedCollectionValue formats a named collection parameter value
func (f *Formatter) formatNamedCollectionValue(value *parser.NamedCollectionValue) string {
	if value.String != nil {
		return *value.String
	}
	if value.Number != nil {
		// Format the number appropriately
		if float64(int64(*value.Number)) == *value.Number {
			// It's an integer
			return strconv.FormatInt(int64(*value.Number), 10)
		}
		// It's a float
		return fmt.Sprintf("%g", *value.Number)
	}
	if value.Bool != nil {
		return *value.Bool
	}
	if value.Null != nil {
		return "NULL"
	}
	return ""
}
