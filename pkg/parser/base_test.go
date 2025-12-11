package parser_test

import (
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	. "github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

// testLexer is a minimal lexer for validation testing
var testLexer = lexer.MustSimple([]lexer.SimpleRule{
	{Name: "Comment", Pattern: `--[^\n]*`},
	{Name: "MultilineComment", Pattern: `/\*[\s\S]*?\*/`},
	{Name: "Whitespace", Pattern: `\s+`},
	{Name: "Ident", Pattern: `[a-zA-Z_][a-zA-Z0-9_]*`},
	{Name: "Punct", Pattern: `[;]`},
})

// TestStatement is a minimal test struct that embeds comment fields
// to validate Participle handles embedded parser tags correctly.
type TestStatement struct {
	LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
	Create           string   `parser:"'CREATE'"`
	Test             string   `parser:"'TEST'"`
	Name             string   `parser:"@Ident"`
	TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
	Semicolon        bool     `parser:"';'"`
}

// EmbeddedLeading provides leading comment support
type EmbeddedLeading struct {
	LeadingComments []string `parser:"@(Comment | MultilineComment)*"`
}

func (e *EmbeddedLeading) GetLeadingComments() []string {
	return e.LeadingComments
}

// EmbeddedTrailing provides trailing comment support
type EmbeddedTrailing struct {
	TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
}

func (e *EmbeddedTrailing) GetTrailingComments() []string {
	return e.TrailingComments
}

// TestStatementEmbedded uses embedded structs for comment fields
type TestStatementEmbedded struct {
	EmbeddedLeading
	Create string `parser:"'CREATE'"`
	Test   string `parser:"'TEST'"`
	Name   string `parser:"@Ident"`
	EmbeddedTrailing
	Semicolon bool `parser:"';'"`
}

// Verify TestStatementEmbedded implements parser.CommentAccessor
var _ CommentAccessor = (*TestStatementEmbedded)(nil)

func TestParticipleEmbeddingValidation(t *testing.T) {
	t.Parallel()

	t.Run("non-embedded parses correctly", func(t *testing.T) {
		t.Parallel()

		parser, err := participle.Build[TestStatement](
			participle.Lexer(testLexer),
			participle.Elide("Whitespace"),
			participle.CaseInsensitive("Ident"),
		)
		require.NoError(t, err)

		sql := `-- leading comment
CREATE TEST myname -- trailing comment
;`
		stmt, err := parser.ParseString("", sql)
		require.NoError(t, err)
		require.Equal(t, "myname", stmt.Name)
		require.Len(t, stmt.LeadingComments, 1)
		require.Equal(t, "-- leading comment", stmt.LeadingComments[0])
		require.Len(t, stmt.TrailingComments, 1)
		require.Equal(t, "-- trailing comment", stmt.TrailingComments[0])
	})

	t.Run("embedded struct parses correctly", func(t *testing.T) {
		t.Parallel()

		parser, err := participle.Build[TestStatementEmbedded](
			participle.Lexer(testLexer),
			participle.Elide("Whitespace"),
			participle.CaseInsensitive("Ident"),
		)
		require.NoError(t, err)

		sql := `-- leading comment
CREATE TEST myname -- trailing comment
;`
		stmt, err := parser.ParseString("", sql)
		require.NoError(t, err)
		require.Equal(t, "myname", stmt.Name)

		// Verify embedded fields work correctly
		require.Len(t, stmt.LeadingComments, 1)
		require.Equal(t, "-- leading comment", stmt.LeadingComments[0])
		require.Len(t, stmt.TrailingComments, 1)
		require.Equal(t, "-- trailing comment", stmt.TrailingComments[0])

		// Verify accessor methods work through embedding
		require.Equal(t, stmt.LeadingComments, stmt.GetLeadingComments())
		require.Equal(t, stmt.TrailingComments, stmt.GetTrailingComments())
	})

	t.Run("embedded struct satisfies interface", func(t *testing.T) {
		t.Parallel()

		parser, err := participle.Build[TestStatementEmbedded](
			participle.Lexer(testLexer),
			participle.Elide("Whitespace"),
			participle.CaseInsensitive("Ident"),
		)
		require.NoError(t, err)

		sql := `CREATE TEST foo;`
		stmt, err := parser.ParseString("", sql)
		require.NoError(t, err)

		// Test that it satisfies CommentAccessor interface
		var accessor CommentAccessor = stmt
		require.Empty(t, accessor.GetLeadingComments())
		require.Empty(t, accessor.GetTrailingComments())
	})
}
