package parser

import (
	"strings"

	"github.com/pseudomuto/housekeeper/pkg/compare"
)

type (
	// CodecClause represents compression codec specification
	CodecClause struct {
		Codec  string      `parser:"'CODEC' '('"`
		Codecs []CodecSpec `parser:"@@ (',' @@)*"`
		Close  string      `parser:"')'"`
	}

	// CodecSpec represents a single codec specification (e.g., ZSTD, LZ4HC(9))
	CodecSpec struct {
		Name       string          `parser:"@(Ident | BacktickIdent)"`
		Parameters []TypeParameter `parser:"('(' @@ (',' @@)* ')')?"`
	}
)

// Equal compares two CodecClause instances for equality
func (c *CodecClause) Equal(other *CodecClause) bool {
	if eq, done := compare.NilCheck(c, other); !done {
		return eq
	}
	return compare.Slices(c.Codecs, other.Codecs, func(a, b CodecSpec) bool {
		return a.Equal(&b)
	})
}

// String returns the SQL representation of a codec clause.
func (c *CodecClause) String() string {
	if c == nil || len(c.Codecs) == 0 {
		return ""
	}

	specs := make([]string, 0, len(c.Codecs))
	for _, spec := range c.Codecs {
		specs = append(specs, spec.String())
	}

	return "CODEC(" + strings.Join(specs, ", ") + ")"
}

// Equal compares two CodecSpec instances for equality
func (c *CodecSpec) Equal(other *CodecSpec) bool {
	return c.Name == other.Name &&
		compare.Slices(c.Parameters, other.Parameters, func(a, b TypeParameter) bool {
			return a.Equal(&b)
		})
}

// String returns the SQL representation of a codec spec.
func (c *CodecSpec) String() string {
	if len(c.Parameters) == 0 {
		return c.Name
	}

	params := make([]string, 0, len(c.Parameters))
	for _, param := range c.Parameters {
		params = append(params, formatTypeParameter(&param))
	}

	return c.Name + "(" + strings.Join(params, ", ") + ")"
}
