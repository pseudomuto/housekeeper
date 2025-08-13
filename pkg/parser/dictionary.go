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
		Create      string              `parser:"'CREATE'"`
		OrReplace   bool                `parser:"@('OR' 'REPLACE')?"`
		Dictionary  string              `parser:"'DICTIONARY'"`
		IfNotExists *string             `parser:"(@'IF' 'NOT' 'EXISTS')?"`
		Database    *string             `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name        string              `parser:"@(Ident | BacktickIdent))"`
		OnCluster   *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Columns     []*DictionaryColumn `parser:"'(' @@* ')'"`
		Clauses     []DictionaryClause  `parser:"@@*"`
		Comment     *string             `parser:"('COMMENT' @String)? ';'"`
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
	DictionaryColumnDefault struct {
		Type       string `parser:"(@'DEFAULT' | @'EXPRESSION')"`
		Expression string `parser:"@(String | Number | Ident | BacktickIdent)"`
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
	// Values are parsed as expressions to handle both simple literals and function calls
	DictionaryParameter struct {
		Name       string     `parser:"@(Ident | BacktickIdent)"`
		Expression Expression `parser:"@@"`
		Comma      *string    `parser:"@','?"`
	}

	// AttachDictionaryStmt represents ATTACH DICTIONARY statements.
	// ClickHouse syntax:
	//   ATTACH DICTIONARY [IF NOT EXISTS] [db.]dict_name [ON CLUSTER cluster]
	AttachDictionaryStmt struct {
		IfNotExists *string `parser:"'ATTACH' 'DICTIONARY' (@'IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent))"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))? ';'"`
	}

	// DetachDictionaryStmt represents DETACH DICTIONARY statements.
	// ClickHouse syntax:
	//   DETACH DICTIONARY [IF EXISTS] [db.]dict_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachDictionaryStmt struct {
		IfExists    *string `parser:"'DETACH' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database    *string `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent))"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently *string `parser:"(@'PERMANENTLY')?"`
		Sync        *string `parser:"(@'SYNC')? ';'"`
	}

	// DropDictionaryStmt represents DROP DICTIONARY statements.
	// ClickHouse syntax:
	//   DROP DICTIONARY [IF EXISTS] [db.]dict_name [ON CLUSTER cluster] [SYNC]
	DropDictionaryStmt struct {
		IfExists  *string `parser:"'DROP' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database  *string `parser:"((@(Ident | BacktickIdent) '.')?"`
		Name      string  `parser:"@(Ident | BacktickIdent))"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync      *string `parser:"(@'SYNC')? ';'"`
	}

	// RenameDictionaryStmt represents RENAME DICTIONARY statements
	// Syntax: RENAME DICTIONARY [db.]name1 TO [db.]new_name1 [, [db.]name2 TO [db.]new_name2, ...] [ON CLUSTER cluster];
	RenameDictionaryStmt struct {
		Renames   []*DictionaryRename `parser:"'RENAME' 'DICTIONARY' @@ (',' @@)*"`
		OnCluster *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))? ';'"`
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
	return p.Expression.String()
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
