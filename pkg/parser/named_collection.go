package parser

import (
	"fmt"
	"strconv"
)

// Named collection-related grammar types for ClickHouse NAMED COLLECTION statements

type (
	// CreateNamedCollectionStmt represents CREATE [OR REPLACE] NAMED COLLECTION statements.
	// ClickHouse syntax:
	//   CREATE [OR REPLACE] NAMED COLLECTION [IF NOT EXISTS] collection_name [ON CLUSTER cluster] AS
	//     key1 = value1 [OVERRIDABLE | NOT OVERRIDABLE],
	//     key2 = value2 [OVERRIDABLE | NOT OVERRIDABLE],
	//     ...
	//   [COMMENT 'comment']
	CreateNamedCollectionStmt struct {
		Create         string                      `parser:"'CREATE'"`
		OrReplace      bool                        `parser:"@('OR' 'REPLACE')?"`
		Named          string                      `parser:"'NAMED'"`
		Collection     string                      `parser:"'COLLECTION'"`
		IfNotExists    *string                     `parser:"(@'IF' 'NOT' 'EXISTS')?"`
		Name           string                      `parser:"@(Ident | BacktickIdent)"`
		OnCluster      *string                     `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		As             string                      `parser:"'AS'"`
		Parameters     []*NamedCollectionParameter `parser:"@@*"`
		GlobalOverride *NamedCollectionOverride    `parser:"@@?"`
		Comment        *string                     `parser:"('COMMENT' @String)? ';'"`
	}

	// AlterNamedCollectionStmt represents ALTER NAMED COLLECTION statements.
	// ClickHouse syntax:
	//   ALTER NAMED COLLECTION [IF EXISTS] collection_name [ON CLUSTER cluster]
	//     [SET key1 = value1 [OVERRIDABLE | NOT OVERRIDABLE] [, ...]]
	//     [DELETE key1, key2, ...]
	//     [SET key3 = value3 [OVERRIDABLE | NOT OVERRIDABLE] [DELETE key4]]
	AlterNamedCollectionStmt struct {
		Alter      string                          `parser:"'ALTER'"`
		Named      string                          `parser:"'NAMED'"`
		Collection string                          `parser:"'COLLECTION'"`
		IfExists   *string                         `parser:"(@'IF' 'EXISTS')?"`
		Name       string                          `parser:"@(Ident | BacktickIdent)"`
		OnCluster  *string                         `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Operations *AlterNamedCollectionOperations `parser:"@@? ';'"`
	}

	// AlterNamedCollectionOperations represents all operations in an ALTER NAMED COLLECTION statement
	AlterNamedCollectionOperations struct {
		SetParams    []*NamedCollectionSetParameter `parser:"('SET' @@ (',' @@)*)?"`
		DeleteParams []*string                      `parser:"('DELETE' @(Ident | BacktickIdent) (',' @(Ident | BacktickIdent))*)?"`
	}

	// NamedCollectionSetParameter represents a single SET parameter in an ALTER NAMED COLLECTION
	NamedCollectionSetParameter struct {
		Key      string                   `parser:"@(Ident | BacktickIdent) '='"`
		Value    *NamedCollectionValue    `parser:"@@"`
		Override *NamedCollectionOverride `parser:"@@?"`
	}

	// DropNamedCollectionStmt represents DROP NAMED COLLECTION statements.
	// ClickHouse syntax:
	//   DROP NAMED COLLECTION [IF EXISTS] collection_name [ON CLUSTER cluster]
	DropNamedCollectionStmt struct {
		Drop       string  `parser:"'DROP'"`
		Named      string  `parser:"'NAMED'"`
		Collection string  `parser:"'COLLECTION'"`
		IfExists   *string `parser:"(@'IF' 'EXISTS')?"`
		Name       string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster  *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon  string  `parser:"';'"`
	}

	// NamedCollectionParameter represents a key-value pair in a CREATE NAMED COLLECTION statement
	NamedCollectionParameter struct {
		Key      string                   `parser:"@(Ident | BacktickIdent) '='"`
		Value    *NamedCollectionValue    `parser:"@@"`
		Override *NamedCollectionOverride `parser:"@@?"`
		Comma    *string                  `parser:"@','?"`
	}

	// NamedCollectionValue represents the value part of a named collection parameter
	NamedCollectionValue struct {
		String *string  `parser:"@String"`
		Number *float64 `parser:"| @Number"`
		Bool   *string  `parser:"| @('TRUE' | 'FALSE')"`
		Null   *string  `parser:"| @'NULL'"`
	}

	// NamedCollectionOverride represents the OVERRIDABLE or NOT OVERRIDABLE clause
	NamedCollectionOverride struct {
		NotOverridable bool `parser:"@('NOT' 'OVERRIDABLE')"`
		Overridable    bool `parser:"| @'OVERRIDABLE'"`
	}
)

// IsOverridable returns true if the override is explicitly overridable
func (o *NamedCollectionOverride) IsOverridable() bool {
	if o == nil {
		return true // default is overridable if not specified
	}
	return o.Overridable
}

// GetValue returns the string representation of the value
func (v *NamedCollectionValue) GetValue() string {
	if v.String != nil {
		return *v.String
	}
	if v.Number != nil {
		// Format the number appropriately
		if float64(int64(*v.Number)) == *v.Number {
			// It's an integer
			return strconv.FormatInt(int64(*v.Number), 10)
		}
		// It's a float
		return fmt.Sprintf("%g", *v.Number)
	}
	if v.Bool != nil {
		return *v.Bool
	}
	if v.Null != nil {
		return "NULL"
	}
	return ""
}
