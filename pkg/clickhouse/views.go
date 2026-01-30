package clickhouse

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/pkg/errors"
	"github.com/pseudomuto/housekeeper/pkg/parser"
)

// cleanViewStatement cleans up a CREATE VIEW/MATERIALIZED VIEW statement from ClickHouse
// to make it parseable by our parser. This handles:
// - Column definitions that ClickHouse adds after TO/APPEND TO table_name
// - DEFINER clause that ClickHouse adds
// - Ensuring proper semicolon termination
func cleanViewStatement(createQuery string) string {
	cleaned := strings.TrimSpace(createQuery)
	if !strings.HasSuffix(cleaned, ";") {
		cleaned += ";"
	}

	// Remove DEFINER clause: "DEFINER = username SQL SECURITY DEFINER"
	// This appears before AS in ClickHouse output
	definerPattern := regexp.MustCompile(`\s+DEFINER\s*=\s*\S+\s+SQL\s+SECURITY\s+DEFINER`)
	cleaned = definerPattern.ReplaceAllString(cleaned, "")

	// ClickHouse may return CREATE VIEW name (col1 Type1, ...) AS SELECT
	// or CREATE MATERIALIZED VIEW name ... APPEND TO table_name (col1 Type1, ...) AS SELECT/WITH
	// We need to remove the column definitions if they exist
	//
	// Find the main "AS" that introduces the SELECT query (not CTEs like "cte_name AS (SELECT...)")
	// The main AS is followed by either SELECT or WITH (for CTEs)
	// Use flexible pattern to handle various whitespace (spaces, newlines, tabs)
	mainAsPattern := regexp.MustCompile(`\)\s*AS\s+(SELECT|WITH)(?:\s|;|$)`)
	mainAsMatch := mainAsPattern.FindStringIndex(cleaned)

	if mainAsMatch != nil {
		// Found pattern like ") AS SELECT" or ") AS WITH"
		// The column definitions are between the opening ( and this closing )
		closingParenPos := mainAsMatch[0] // Position of the closing )
		prefixPart := cleaned[:closingParenPos+1]

		// Find the opening ( of column definitions by working backwards and counting parens
		// This handles nested parentheses in column types like AggregateFunction(argMin, String, DateTime)
		parenPos := findColumnDefOpenParenByCounting(prefixPart)
		if parenPos < 0 {
			// Fallback: look for "TO table_name (" directly (handles local instances without ON CLUSTER)
			parenPos = findColumnDefOpenParenFallback(prefixPart)
		}
		if parenPos >= 0 {
			// Remove column definitions: take before ( and from AS onwards
			asPos := mainAsMatch[0] + 1 // Position right after the closing )
			cleaned = cleaned[:parenPos] + cleaned[asPos:]
		}
	}

	// Normalize whitespace after removal (multiple spaces/newlines to single space)
	cleaned = regexp.MustCompile(`\s+`).ReplaceAllString(cleaned, " ")

	// ClickHouse can return CAST(expr, 'Type') but our parser expects CAST(expr AS Type)
	cleaned = normalizeCastTwoArgToAs(cleaned)

	return strings.TrimSpace(cleaned)
}

// normalizeCastTwoArgToAs converts ClickHouse's CAST(expr, 'Type') to CAST(expr AS Type)
// so the parser (which only supports CAST(expr AS type)) can parse it.
// Handles nested parens in expr and type strings like 'Nullable(Decimal64(2))'.
func normalizeCastTwoArgToAs(s string) string {
	const castKeyword = "CAST("
	from := 0
	for {
		idx := strings.Index(strings.ToUpper(s[from:]), castKeyword)
		if idx < 0 {
			return s
		}
		start := from + idx
		openParen := start + len(castKeyword) - 1
		pos := openParen + 1
		depth := 1
		exprEnd := -1
		for pos < len(s) && depth > 0 {
			c := s[pos]
			switch c {
			case '(':
				depth++
			case ')':
				depth--
			case ',':
				if depth == 1 {
					p := pos + 1
					for p < len(s) && (s[p] == ' ' || s[p] == '\t' || s[p] == '\n' || s[p] == '\r') {
						p++
					}
					if p < len(s) && s[p] == '\'' {
						exprEnd = pos
						break
					}
				}
			}
			pos++
		}
		if exprEnd < 0 {
			from = start + 1
			continue
		}
		expr := strings.TrimSpace(s[openParen+1 : exprEnd])
		typeStart := exprEnd + 1
		for typeStart < len(s) && (s[typeStart] == ' ' || s[typeStart] == '\t' || s[typeStart] == '\n' || s[typeStart] == '\r') {
			typeStart++
		}
		if typeStart >= len(s) || s[typeStart] != '\'' {
			from = start + 1
			continue
		}
		typeContentStart := typeStart + 1
		typeContentEnd := typeContentStart
		for typeContentEnd < len(s) {
			if s[typeContentEnd] == '\'' {
				if typeContentEnd+1 < len(s) && s[typeContentEnd+1] == '\'' {
					typeContentEnd += 2
					continue
				}
				break
			}
			typeContentEnd++
		}
		typeStr := s[typeContentStart:typeContentEnd]
		typeStr = strings.ReplaceAll(typeStr, "''", "'")
		closeParen := typeContentEnd + 1
		for closeParen < len(s) && (s[closeParen] == ' ' || s[closeParen] == '\t' || s[closeParen] == '\n' || s[closeParen] == '\r') {
			closeParen++
		}
		if closeParen >= len(s) || s[closeParen] != ')' {
			from = start + 1
			continue
		}
		closeParen++
		replacement := "CAST(" + expr + " AS " + typeStr + ")"
		s = s[:start] + replacement + s[closeParen:]
		from = start + len(replacement)
	}
}

// findColumnDefOpenParenByCounting finds the opening parenthesis of column definitions
// by working backwards from the closing paren and counting parentheses.
// This is more robust than regex matching and handles nested parentheses in column types.
func findColumnDefOpenParenByCounting(prefix string) int {
	if len(prefix) == 0 {
		return -1
	}

	// Start from the last character (the closing paren) and work backwards
	// Count parentheses to find the matching opening paren
	depth := 0
	closingParenPos := len(prefix) - 1

	for i := closingParenPos; i >= 0; i-- {
		char := prefix[i]
		if char == ')' {
			depth++
		} else if char == '(' {
			depth--
			if depth == 0 {
				// Found the matching opening paren
				// Now check if this looks like column definitions (after TO/APPEND TO or after VIEW name)
				beforeParen := prefix[:i]
				// Check if this paren is after TO/APPEND TO or after VIEW name
				// (not part of a function call in the middle of the statement)
				if isColumnDefParen(beforeParen) {
					return i
				}
			}
		}
	}

	return -1
}

// findColumnDefOpenParenFallback finds the opening paren of column definitions by looking
// for "TO table_name (" or "ON CLUSTER x TO table_name (" when the counting method fails.
// Used when isColumnDefParen is too strict (e.g. local instances without ON CLUSTER).
func findColumnDefOpenParenFallback(prefix string) int {
	// Look for "TO table_name (" - the ( that immediately follows the TO clause table name
	toParenPattern := regexp.MustCompile(`TO\s+[\w.]+\s*\(`)
	matches := toParenPattern.FindAllStringIndex(prefix, -1)
	if len(matches) == 0 {
		return -1
	}
	// Use the last match (closest to the closing paren)
	lastMatch := matches[len(matches)-1]
	openParenPos := lastMatch[1] - 1 // Position of the (
	return openParenPos
}

// isColumnDefParen checks if the opening paren at the end of the given string
// is likely a column definition paren (after TO/APPEND TO or after VIEW name)
// Since we already matched ") AS SELECT" or ") AS WITH", this is very likely column definitions.
// We just need to verify it's not part of a nested expression.
func isColumnDefParen(beforeParen string) bool {
	// Look backwards for TO, APPEND TO, or VIEW keywords
	// Use a generous window so we find TO/VIEW even with long view names or ON CLUSTER
	checkLen := 600
	if len(beforeParen) < checkLen {
		checkLen = len(beforeParen)
	}
	recent := strings.TrimSpace(beforeParen[len(beforeParen)-checkLen:])

	// Use (?s) to make . match newlines, and be more flexible with whitespace
	// Look for TO or APPEND TO (works with or without ON CLUSTER - local instances omit ON CLUSTER)
	toPattern := regexp.MustCompile(`(?is)(?:APPEND\s+)?TO\s+[\w.]+\s*$`)
	if toPattern.MatchString(recent) {
		return true
	}

	// Also check for ON CLUSTER ... TO pattern (ON CLUSTER comes before TO)
	onClusterToPattern := regexp.MustCompile(`(?is)ON\s+CLUSTER\s+[\w.]+\s+TO\s+[\w.]+\s*$`)
	if onClusterToPattern.MatchString(recent) {
		return true
	}

	// Check for VIEW name pattern (possibly with ON CLUSTER)
	viewPattern := regexp.MustCompile(`(?is)VIEW\s+[\w.]+(?:\s+ON\s+CLUSTER\s+[\w.]+)?\s*$`)
	if viewPattern.MatchString(recent) {
		return true
	}

	// If we can't find TO or VIEW, it's probably not column definitions
	// (could be a nested expression or function call)
	return false
}

// extractViews retrieves all view definitions (both regular and materialized) from the ClickHouse instance.
// This function queries the system.tables table to get complete view information and returns them
// as parsed DDL statements, handling both regular views and materialized views.
//
// System views are automatically excluded. All DDL statements are validated
// using the parser before being returned.
//
// Example:
//
//	client, err := clickhouse.NewClient(ctx, "localhost:9000")
//	if err != nil {
//		log.Fatal(err)
//	}
//	defer client.Close()
//
//	views, err := client.GetViews(ctx)
//	if err != nil {
//		log.Fatalf("Failed to extract views: %v", err)
//	}
//
//	// Process the parsed view statements
//	for _, stmt := range views.Statements {
//		if stmt.CreateView != nil {
//			viewType := "VIEW"
//			if stmt.CreateView.Materialized {
//				viewType = "MATERIALIZED VIEW"
//			}
//			name := stmt.CreateView.Name
//			if stmt.CreateView.Database != nil {
//				name = *stmt.CreateView.Database + "." + name
//			}
//			fmt.Printf("%s: %s\n", viewType, name)
//		}
//	}
//
// Returns a *parser.SQL containing view CREATE statements or an error if extraction fails.
func extractViews(ctx context.Context, client *Client) (*parser.SQL, error) {
	condition, params := buildDatabaseExclusion("database", client.options.IgnoreDatabases)
	query := fmt.Sprintf(`
		SELECT 
			create_table_query
		FROM system.tables
		WHERE %s
		  AND engine IN ('View', 'MaterializedView')
		ORDER BY database, name
	`, condition)

	rows, err := client.conn.Query(ctx, query, params...)
	if err != nil {
		return nil, errors.Wrap(err, "failed to query views")
	}
	defer rows.Close()

	var statements []string
	for rows.Next() {
		var createQuery string
		if err := rows.Scan(&createQuery); err != nil {
			return nil, errors.Wrap(err, "failed to scan view row")
		}

		// Clean up the CREATE statement - first remove ClickHouse-specific clauses
		cleanedQuery := cleanViewStatement(createQuery)

		// Validate the statement using our parser
		if err := validateDDLStatement(cleanedQuery); err != nil {
			// Include the problematic query in the error for debugging
			return nil, errors.Wrapf(err, "generated invalid DDL for view (query: %s)", cleanedQuery)
		}

		statements = append(statements, cleanedQuery)
	}

	if err := rows.Err(); err != nil {
		return nil, errors.Wrap(err, "error iterating view rows")
	}

	// Parse all statements into a SQL structure
	combinedSQL := strings.Join(statements, "\n")

	sqlResult, err := parser.ParseString(combinedSQL)
	if err != nil {
		return nil, errors.Wrap(err, "failed to parse combined view DDL")
	}

	return sqlResult, nil
}
