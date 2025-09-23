package parser

// Dictionary-related grammar types for ClickHouse CREATE DICTIONARY statements

type (
	// CreateDictionaryStmt represents CREATE [OR REPLACE] DICTIONARY statements.
	// ClickHouse syntax:
	//   CREATE [OR REPLACE] DICTIONARY [IF NOT EXISTS] [db.]dict_name [ON CLUSTER cluster]
	//   (
	//     column1 Type1 [DEFAULT|EXPRESSION expr] [IS_OBJECT_ID|HIERARCHICAL|INJECTIVE],
	//     column2 Type2 [DEFAULT|EXPRESSION expr] [IS_OBJECT_ID|HIERARCHICAL|INJECTIVE],
	//     ...
	//   )
	//   PRIMARY KEY key1 [, key2, ...]
	//   SOURCE(source_type(param1 value1 [param2 value2 ...]))
	//   LAYOUT(layout_type[(param1 value1 [param2 value2 ...])])
	//   LIFETIME([MIN min_val MAX max_val] | single_val)
	//   [SETTINGS(setting1 = value1 [, setting2 = value2, ...])]
	//   [COMMENT 'comment']
	CreateDictionaryStmt struct {
		LeadingComments  []string            `parser:"@(Comment | MultilineComment)*"`
		Create           string              `parser:"'CREATE'"`
		OrReplace        bool                `parser:"@('OR' 'REPLACE')?"`
		Dictionary       string              `parser:"'DICTIONARY'"`
		IfNotExists      *string             `parser:"(@'IF' 'NOT' 'EXISTS')?"`
		Database         *string             `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name             string              `parser:"@(Ident | BacktickIdent))"`
		OnCluster        *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Columns          []*DictionaryColumn `parser:"'(' @@* ')'"`
		Clauses          []DictionaryClause  `parser:"@@*"`
		Comment          *string             `parser:"('COMMENT' @String)?"`
		TrailingComments []string            `parser:"@(Comment | MultilineComment)* ';'"`
	}

	// DictionaryClause represents any clause that can appear after columns in a CREATE DICTIONARY statement
	// This allows clauses to be specified in any order
	DictionaryClause struct {
		PrimaryKey *DictionaryPrimaryKey `parser:"@@"`
		Source     *DictionarySource     `parser:"| @@"`
		Layout     *DictionaryLayout     `parser:"| @@"`
		Lifetime   *DictionaryLifetime   `parser:"| @@"`
		Settings   *DictionarySettings   `parser:"| @@"`
	}

	// DictionaryColumn represents a column definition in dictionary
	DictionaryColumn struct {
		Name       string                   `parser:"@(Ident | BacktickIdent)"`
		Type       string                   `parser:"@(Ident | BacktickIdent)"`
		Default    *DictionaryColumnDefault `parser:"@@?"`
		Attributes []*DictionaryColumnAttr  `parser:"@@*"`
		Comma      *string                  `parser:"@','?"`
	}

	// DictionaryColumnDefault represents DEFAULT or EXPRESSION clause
	// Supports both simple values and function calls like now()
	DictionaryColumnDefault struct {
		Type       string     `parser:"(@'DEFAULT' | @'EXPRESSION')"`
		Expression Expression `parser:"@@"`
	}

	// DictionaryColumnAttr represents column attributes like IS_OBJECT_ID, HIERARCHICAL, INJECTIVE
	DictionaryColumnAttr struct {
		Name string `parser:"@('IS_OBJECT_ID' | 'HIERARCHICAL' | 'INJECTIVE')"`
	}

	// DictionaryPrimaryKey represents PRIMARY KEY clause
	DictionaryPrimaryKey struct {
		Keys []string `parser:"'PRIMARY' 'KEY' @(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))*"`
	}

	// DictionarySource represents SOURCE clause
	DictionarySource struct {
		Name       string                 `parser:"'SOURCE' '(' @(Ident | BacktickIdent)"`
		Parameters []*DictionaryParameter `parser:"('(' @@* ')')? ')'"`
	}

	// DictionaryLayout represents LAYOUT clause
	DictionaryLayout struct {
		Name       string                 `parser:"'LAYOUT' '(' @(Ident | BacktickIdent)"`
		Parameters []*DictionaryParameter `parser:"('(' @@* ')')? ')'"`
	}

	// DictionaryLifetime represents LIFETIME clause
	DictionaryLifetime struct {
		MinMax *DictionaryLifetimeMinMax `parser:"'LIFETIME' '(' (@@"`
		Single *string                   `parser:"| @Number) ')'"`
	}

	// DictionaryLifetimeMinMax represents MIN/MAX lifetime values in flexible order
	DictionaryLifetimeMinMax struct {
		MinFirst *DictionaryLifetimeMinFirst `parser:"@@"`
		MaxFirst *DictionaryLifetimeMaxFirst `parser:"| @@"`
	}

	// DictionaryLifetimeMinFirst represents MIN value followed by MAX value
	DictionaryLifetimeMinFirst struct {
		MinValue string `parser:"'MIN' @Number"`
		MaxValue string `parser:"'MAX' @Number"`
	}

	// DictionaryLifetimeMaxFirst represents MAX value followed by MIN value
	DictionaryLifetimeMaxFirst struct {
		MaxValue string `parser:"'MAX' @Number"`
		MinValue string `parser:"'MIN' @Number"`
	}

	// DictionarySettings represents SETTINGS clause
	DictionarySettings struct {
		Settings []*DictionarySetting `parser:"'SETTINGS' '(' @@* ')'"`
	}

	// DictionarySetting represents individual setting
	DictionarySetting struct {
		Name  string  `parser:"@(Ident | BacktickIdent) '='"`
		Value string  `parser:"@(String | Number | Ident | BacktickIdent)"`
		Comma *string `parser:"@','?"`
	}

	// DictionaryParameter represents parameters in SOURCE or LAYOUT
	DictionaryParameter struct {
		DSLFunction *DictionaryDSLFunc `parser:"(@@ ','?)"`
		SimpleParam *SimpleParameter   `parser:"| (@@ ','?)"`
	}

	// SimpleParameter represents name-value parameters
	SimpleParameter struct {
		Name       string     `parser:"@(Ident | BacktickIdent)"`
		Expression Expression `parser:"@@"`
	}

	// DictionaryDSLFunc represents DSL functions in SOURCE parameters
	// Supports any function name including: credentials, header, headers, HTTP, MySQL, etc.
	DictionaryDSLFunc struct {
		Name   string                `parser:"@(Ident | BacktickIdent)"`
		Params []*DictionaryDSLParam `parser:"'(' @@* ')'"`
	}

	// DictionaryDSLParam represents parameters in DSL functions
	// Supports simple name-value pairs and nested function calls
	DictionaryDSLParam struct {
		SimpleParam *SimpleDSLParam    `parser:"(@@ ','?)"`
		NestedFunc  *DictionaryDSLFunc `parser:"| (@@ ','?)"`
	}

	// SimpleDSLParam represents simple name-value parameters in DSL functions
	SimpleDSLParam struct {
		Name  string     `parser:"@(Ident | BacktickIdent | 'USER' | 'PASSWORD' | 'VALUE' | 'NAME')"`
		Value Expression `parser:"@@"`
	}

	// AttachDictionaryStmt represents ATTACH DICTIONARY statements.
	// ClickHouse syntax:
	//   ATTACH DICTIONARY [IF NOT EXISTS] [db.]dict_name [ON CLUSTER cluster]
	AttachDictionaryStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		IfNotExists      *string  `parser:"'ATTACH' 'DICTIONARY' (@'IF' 'NOT' 'EXISTS')?"`
		Database         *string  `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name             string   `parser:"@(Ident | BacktickIdent))"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)* ';'"`
	}

	// DetachDictionaryStmt represents DETACH DICTIONARY statements.
	// ClickHouse syntax:
	//   DETACH DICTIONARY [IF EXISTS] [db.]dict_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachDictionaryStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		IfExists         *string  `parser:"'DETACH' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database         *string  `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name             string   `parser:"@(Ident | BacktickIdent))"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently      *string  `parser:"(@'PERMANENTLY')?"`
		Sync             *string  `parser:"(@'SYNC')?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)* ';'"`
	}

	// DropDictionaryStmt represents DROP DICTIONARY statements.
	// ClickHouse syntax:
	//   DROP DICTIONARY [IF EXISTS] [db.]dict_name [ON CLUSTER cluster] [SYNC]
	DropDictionaryStmt struct {
		LeadingComments  []string `parser:"@(Comment | MultilineComment)*"`
		IfExists         *string  `parser:"'DROP' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database         *string  `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name             string   `parser:"@(Ident | BacktickIdent))"`
		OnCluster        *string  `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync             *string  `parser:"(@'SYNC')?"`
		TrailingComments []string `parser:"@(Comment | MultilineComment)* ';'"`
	}

	// RenameDictionaryStmt represents RENAME DICTIONARY statements
	// Syntax: RENAME DICTIONARY [db.]name1 TO [db.]new_name1 [, [db.]name2 TO [db.]new_name2, ...] [ON CLUSTER cluster];
	RenameDictionaryStmt struct {
		LeadingComments  []string            `parser:"@(Comment | MultilineComment)*"`
		Renames          []*DictionaryRename `parser:"'RENAME' 'DICTIONARY' @@ (',' @@)*"`
		OnCluster        *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		TrailingComments []string            `parser:"@(Comment | MultilineComment)* ';'"`
	}

	// DictionaryRename represents a single dictionary rename operation
	DictionaryRename struct {
		FromDatabase *string `parser:"((@(Ident | BacktickIdent) '.')?"`
		FromName     string  `parser:"@(Ident | BacktickIdent))"`
		ToDatabase   *string `parser:"'TO' ((@(Ident | BacktickIdent) '.')?"`
		ToName       string  `parser:"@(Ident | BacktickIdent))"`
	}
)

// GetValue returns the string representation of the parameter value
func (p *DictionaryParameter) GetValue() string {
	if p.DSLFunction != nil {
		return p.DSLFunction.String()
	}
	if p.SimpleParam != nil {
		return p.SimpleParam.Expression.String()
	}
	return ""
}

// GetName returns the parameter name
func (p *DictionaryParameter) GetName() string {
	if p.DSLFunction != nil {
		return p.DSLFunction.Name
	}
	if p.SimpleParam != nil {
		return p.SimpleParam.Name
	}
	return ""
}

// String returns the string representation of a DictionaryDSLFunc
func (d *DictionaryDSLFunc) String() string {
	result := d.Name + "("
	for i, param := range d.Params {
		if i > 0 {
			result += " "
		}
		if param.NestedFunc != nil {
			result += param.NestedFunc.String()
		} else if param.SimpleParam != nil {
			result += param.SimpleParam.Name + " " + param.SimpleParam.Value.String()
		}
	}
	result += ")"
	return result
}

// GetValue returns the string representation of the default value
func (d *DictionaryColumnDefault) GetValue() string {
	return d.Expression.String()
}

// Convenience methods for CreateDictionaryStmt to access clauses from the new flexible structure
// These maintain backward compatibility with existing code

// GetPrimaryKey returns the PRIMARY KEY clause if present
func (c *CreateDictionaryStmt) GetPrimaryKey() *DictionaryPrimaryKey {
	for _, clause := range c.Clauses {
		if clause.PrimaryKey != nil {
			return clause.PrimaryKey
		}
	}
	return nil
}

// GetSource returns the SOURCE clause if present
func (c *CreateDictionaryStmt) GetSource() *DictionarySource {
	for _, clause := range c.Clauses {
		if clause.Source != nil {
			return clause.Source
		}
	}
	return nil
}

// GetLayout returns the LAYOUT clause if present
func (c *CreateDictionaryStmt) GetLayout() *DictionaryLayout {
	for _, clause := range c.Clauses {
		if clause.Layout != nil {
			return clause.Layout
		}
	}
	return nil
}

// GetLifetime returns the LIFETIME clause if present
func (c *CreateDictionaryStmt) GetLifetime() *DictionaryLifetime {
	for _, clause := range c.Clauses {
		if clause.Lifetime != nil {
			return clause.Lifetime
		}
	}
	return nil
}

// GetSettings returns the SETTINGS clause if present
func (c *CreateDictionaryStmt) GetSettings() *DictionarySettings {
	for _, clause := range c.Clauses {
		if clause.Settings != nil {
			return clause.Settings
		}
	}
	return nil
}

// GetLeadingComments returns the leading comments for CreateDictionaryStmt
func (c *CreateDictionaryStmt) GetLeadingComments() []string {
	return c.LeadingComments
}

// GetTrailingComments returns the trailing comments for CreateDictionaryStmt
func (c *CreateDictionaryStmt) GetTrailingComments() []string {
	return c.TrailingComments
}

// GetLeadingComments returns the leading comments for AttachDictionaryStmt
func (a *AttachDictionaryStmt) GetLeadingComments() []string {
	return a.LeadingComments
}

// GetTrailingComments returns the trailing comments for AttachDictionaryStmt
func (a *AttachDictionaryStmt) GetTrailingComments() []string {
	return a.TrailingComments
}

// GetLeadingComments returns the leading comments for DetachDictionaryStmt
func (d *DetachDictionaryStmt) GetLeadingComments() []string {
	return d.LeadingComments
}

// GetTrailingComments returns the trailing comments for DetachDictionaryStmt
func (d *DetachDictionaryStmt) GetTrailingComments() []string {
	return d.TrailingComments
}

// GetLeadingComments returns the leading comments for DropDictionaryStmt
func (d *DropDictionaryStmt) GetLeadingComments() []string {
	return d.LeadingComments
}

// GetTrailingComments returns the trailing comments for DropDictionaryStmt
func (d *DropDictionaryStmt) GetTrailingComments() []string {
	return d.TrailingComments
}

// GetLeadingComments returns the leading comments for RenameDictionaryStmt
func (r *RenameDictionaryStmt) GetLeadingComments() []string {
	return r.LeadingComments
}

// GetTrailingComments returns the trailing comments for RenameDictionaryStmt
func (r *RenameDictionaryStmt) GetTrailingComments() []string {
	return r.TrailingComments
}
