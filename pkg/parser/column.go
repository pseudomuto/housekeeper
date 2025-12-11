package parser

import "github.com/pseudomuto/housekeeper/pkg/compare"

type (
	// Column represents a complete column definition in ClickHouse DDL.
	// It includes the column name, data type, and all possible modifiers
	// such as DEFAULT values, MATERIALIZED expressions, ALIAS definitions,
	// compression CODECs, TTL settings, and comments.
	Column struct {
		LeadingComments  []string          `parser:"@(Comment | MultilineComment)*"`
		Name             string            `parser:"@(Ident | BacktickIdent)"`
		DataType         *DataType         `parser:"@@"`
		Attributes       []ColumnAttribute `parser:"@@*"`
		TrailingComments []string          `parser:"@(Comment | MultilineComment)*"`
	}

	// ColumnAttribute represents any attribute that can appear after the data type
	// This allows attributes to be specified in any order
	ColumnAttribute struct {
		Default *DefaultClause `parser:"@@"`
		Codec   *CodecClause   `parser:"| @@"`
		TTL     *TTLClause     `parser:"| @@"`
		Comment *string        `parser:"| ('COMMENT' @String)"`
	}

	// DefaultClause represents DEFAULT, MATERIALIZED, EPHEMERAL, or ALIAS expressions
	DefaultClause struct {
		Type       string     `parser:"@('DEFAULT' | 'MATERIALIZED' | 'EPHEMERAL' | 'ALIAS')"`
		Expression Expression `parser:"@@"`
	}

	// TTLClause represents column-level TTL specification
	TTLClause struct {
		TTL        string     `parser:"'TTL'"`
		Expression Expression `parser:"@@"`
	}
)

// Equal compares two TTLClause instances for equality
func (t *TTLClause) Equal(other *TTLClause) bool {
	if eq, done := compare.NilCheck(t, other); !done {
		return eq
	}
	return t.Expression.Equal(&other.Expression)
}

// Equal compares two DefaultClause instances for equality
func (d *DefaultClause) Equal(other *DefaultClause) bool {
	if eq, done := compare.NilCheck(d, other); !done {
		return eq
	}
	return d.Type == other.Type && d.Expression.Equal(&other.Expression)
}

// GetDefault returns the default clause for the column, if present
func (c *Column) GetDefault() *DefaultClause {
	for _, attr := range c.Attributes {
		if attr.Default != nil {
			return attr.Default
		}
	}
	return nil
}

// GetCodec returns the codec clause for the column, if present
func (c *Column) GetCodec() *CodecClause {
	for _, attr := range c.Attributes {
		if attr.Codec != nil {
			return attr.Codec
		}
	}
	return nil
}

// GetTTL returns the TTL clause for the column, if present
func (c *Column) GetTTL() *TTLClause {
	for _, attr := range c.Attributes {
		if attr.TTL != nil {
			return attr.TTL
		}
	}
	return nil
}

// GetComment returns the comment for the column, if present
func (c *Column) GetComment() *string {
	for _, attr := range c.Attributes {
		if attr.Comment != nil {
			return attr.Comment
		}
	}
	return nil
}
