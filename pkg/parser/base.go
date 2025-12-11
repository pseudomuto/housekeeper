package parser

// LeadingCommentField provides leading comment support for statements.
// This is embedded at the start of statement structs to capture comments
// that appear before the statement keywords.
type LeadingCommentField struct {
	LeadingComments []string `parser:"@(Comment | MultilineComment)*"`
}

// GetLeadingComments returns the leading comments.
func (c *LeadingCommentField) GetLeadingComments() []string {
	return c.LeadingComments
}

// TrailingCommentField provides trailing comment support for statements.
// This is embedded near the end of statement structs (before Semicolon)
// to capture comments that appear after the statement body.
type TrailingCommentField struct {
	TrailingComments []string `parser:"@(Comment | MultilineComment)*"`
}

// GetTrailingComments returns the trailing comments.
func (c *TrailingCommentField) GetTrailingComments() []string {
	return c.TrailingComments
}

// CommentAccessor is implemented by all statement types that support comments.
// This interface matches the format package's commentable interface.
type CommentAccessor interface {
	GetLeadingComments() []string
	GetTrailingComments() []string
}
