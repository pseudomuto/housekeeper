package parser

// Dictionary-related grammar types for ClickHouse CREATE DICTIONARY statements

type (
	// CreateDictionaryStmt represents CREATE [OR REPLACE] DICTIONARY statements
	CreateDictionaryStmt struct {
		Create      string                `parser:"'CREATE'"`
		OrReplace   bool                  `parser:"@('OR' 'REPLACE')?"`
		Dictionary  string                `parser:"'DICTIONARY'"`
		IfNotExists *string               `parser:"(@'IF' 'NOT' 'EXISTS')?"`
		Database    *string               `parser:"((@Ident '.')?"`
		Name        string                `parser:"@Ident)"`
		OnCluster   *string               `parser:"('ON' 'CLUSTER' @Ident)?"`
		Columns     []*DictionaryColumn   `parser:"'(' @@* ')'"`
		PrimaryKey  *DictionaryPrimaryKey `parser:"@@?"`
		Source      *DictionarySource     `parser:"@@"`
		Layout      *DictionaryLayout     `parser:"@@"`
		Lifetime    *DictionaryLifetime   `parser:"@@?"`
		Settings    *DictionarySettings   `parser:"@@?"`
		Comment     *string               `parser:"('COMMENT' @String)? ';'"`
	}

	// DictionaryColumn represents a column definition in dictionary
	DictionaryColumn struct {
		Name       string                   `parser:"@Ident"`
		Type       string                   `parser:"@Ident"`
		Default    *DictionaryColumnDefault `parser:"@@?"`
		Attributes []*DictionaryColumnAttr  `parser:"@@*"`
		Comma      *string                  `parser:"@','?"`
	}

	// DictionaryColumnDefault represents DEFAULT or EXPRESSION clause
	DictionaryColumnDefault struct {
		Type       string `parser:"(@'DEFAULT' | @'EXPRESSION')"`
		Expression string `parser:"@(String | Number | Ident)"`
	}

	// DictionaryColumnAttr represents column attributes like IS_OBJECT_ID, HIERARCHICAL, INJECTIVE
	DictionaryColumnAttr struct {
		Name string `parser:"@('IS_OBJECT_ID' | 'HIERARCHICAL' | 'INJECTIVE')"`
	}

	// DictionaryPrimaryKey represents PRIMARY KEY clause
	DictionaryPrimaryKey struct {
		Keys []string `parser:"'PRIMARY' 'KEY' @Ident (',' @Ident)*"`
	}

	// DictionarySource represents SOURCE clause
	DictionarySource struct {
		Name       string                 `parser:"'SOURCE' '(' @Ident"`
		Parameters []*DictionaryParameter `parser:"('(' @@* ')')? ')'"`
	}

	// DictionaryLayout represents LAYOUT clause
	DictionaryLayout struct {
		Name       string                 `parser:"'LAYOUT' '(' @Ident"`
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
		Name  string  `parser:"@Ident '='"`
		Value string  `parser:"@(String | Number | Ident)"`
		Comma *string `parser:"@','?"`
	}

	// DictionaryParameter represents parameters in SOURCE or LAYOUT
	DictionaryParameter struct {
		Name  string  `parser:"@Ident"`
		Value string  `parser:"@(String | Number | Ident)"`
		Comma *string `parser:"@','?"`
	}

	// AttachDictionaryStmt represents ATTACH DICTIONARY statements
	AttachDictionaryStmt struct {
		IfNotExists *string `parser:"'ATTACH' 'DICTIONARY' (@'IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"((@Ident '.')?"`
		Name        string  `parser:"@Ident)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)? ';'"`
	}

	// DetachDictionaryStmt represents DETACH DICTIONARY statements
	DetachDictionaryStmt struct {
		IfExists    *string `parser:"'DETACH' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database    *string `parser:"((@Ident '.')?"`
		Name        string  `parser:"@Ident)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Permanently *string `parser:"(@'PERMANENTLY')?"`
		Sync        *string `parser:"(@'SYNC')? ';'"`
	}

	// DropDictionaryStmt represents DROP DICTIONARY statements
	DropDictionaryStmt struct {
		IfExists  *string `parser:"'DROP' 'DICTIONARY' (@'IF' 'EXISTS')?"`
		Database  *string `parser:"((@Ident '.')?"`
		Name      string  `parser:"@Ident)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @Ident)?"`
		Sync      *string `parser:"(@'SYNC')? ';'"`
	}

	// RenameDictionaryStmt represents RENAME DICTIONARY statements
	// Syntax: RENAME DICTIONARY [db.]name1 TO [db.]new_name1 [, [db.]name2 TO [db.]new_name2, ...] [ON CLUSTER cluster];
	RenameDictionaryStmt struct {
		Renames   []*DictionaryRename `parser:"'RENAME' 'DICTIONARY' @@ (',' @@)*"`
		OnCluster *string             `parser:"('ON' 'CLUSTER' @Ident)? ';'"`
	}

	// DictionaryRename represents a single dictionary rename operation
	DictionaryRename struct {
		FromDatabase *string `parser:"((@Ident '.')?"`
		FromName     string  `parser:"@Ident)"`
		ToDatabase   *string `parser:"'TO' ((@Ident '.')?"`
		ToName       string  `parser:"@Ident)"`
	}
)
