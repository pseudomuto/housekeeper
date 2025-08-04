package parser

type (
	// CreateTableStmt represents a CREATE TABLE statement with full ClickHouse syntax support.
	// ClickHouse syntax:
	//   CREATE [OR REPLACE] TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
	//   (
	//     column1 Type1 [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr1] [CODEC(codec1)] [TTL expr1] [COMMENT 'comment'],
	//     column2 Type2 [DEFAULT|MATERIALIZED|EPHEMERAL|ALIAS expr2] [CODEC(codec2)] [TTL expr2] [COMMENT 'comment'],
	//     ...
	//     [INDEX index_name expression TYPE index_type GRANULARITY value],
	//     [CONSTRAINT constraint_name CHECK expression],
	//     ...
	//   )
	//   ENGINE = engine_name([parameters])
	//   [ORDER BY expression]
	//   [PARTITION BY expression]
	//   [PRIMARY KEY expression]
	//   [SAMPLE BY expression]
	//   [TTL expression]
	//   [SETTINGS name=value, ...]
	//   [COMMENT 'comment']
	CreateTableStmt struct {
		Create      string                `parser:"'CREATE'"`
		OrReplace   bool                  `parser:"@('OR' 'REPLACE')?"`
		Table       string                `parser:"'TABLE'"`
		IfNotExists bool                  `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Database    *string               `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string                `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string               `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Elements    []TableElement        `parser:"'(' @@ (',' @@)* ')'"`
		Engine      *TableEngine          `parser:"@@"`
		OrderBy     *OrderByClause        `parser:"@@?"`
		PartitionBy *PartitionByClause    `parser:"@@?"`
		PrimaryKey  *PrimaryKeyClause     `parser:"@@?"`
		SampleBy    *SampleByClause       `parser:"@@?"`
		TTL         *TableTTLClause       `parser:"@@?"`
		Settings    *TableSettingsClause  `parser:"@@?"`
		Comment     *string               `parser:"('COMMENT' @String)?"`
		Semicolon   bool                  `parser:"';'"`
	}

	// TableElement represents an element within table definition (column, index, constraint, or projection)
	TableElement struct {
		Index      *IndexDefinition      `parser:"@@"`
		Constraint *ConstraintDefinition `parser:"| @@"`
		Projection *ProjectionDefinition `parser:"| @@"`
		Column     *Column               `parser:"| @@"`
	}

	// IndexDefinition represents an INDEX definition within CREATE TABLE
	// ClickHouse syntax:
	//   INDEX index_name expression TYPE index_type [GRANULARITY value]
	IndexDefinition struct {
		Index       string     `parser:"'INDEX'"`
		Name        string     `parser:"@(Ident | BacktickIdent)"`
		Expression  Expression `parser:"@@"`
		Type        string     `parser:"'TYPE'"`
		IndexType   IndexType  `parser:"@@"`
		Granularity *string    `parser:"('GRANULARITY' @Number)?"`
	}

	// IndexType represents different ClickHouse index types
	IndexType struct {
		// Simple index types
		BloomFilter  bool `parser:"@'bloom_filter'"`
		MinMax       bool `parser:"| @'minmax'"`  
		Hypothesis   bool `parser:"| @'hypothesis'"`
		
		// Parametric index types
		Set          *IndexSetType          `parser:"| @@"`
		TokenBF      *IndexTokenBFType      `parser:"| @@"`
		NGramBF      *IndexNGramBFType      `parser:"| @@"`
		
		// Custom/future index types - fallback to string
		Custom       *string                `parser:"| @(Ident | BacktickIdent)"`
	}

	// IndexSetType represents set(max_rows) index type
	IndexSetType struct {
		Set     string `parser:"'set' '('"`
		MaxRows string `parser:"@Number"`
		Close   string `parser:"')'"`
	}

	// IndexTokenBFType represents tokenbf_v1(size, hashes, seed) index type
	IndexTokenBFType struct {
		TokenBF string `parser:"'tokenbf_v1' '('"`
		Size    string `parser:"@Number"`
		Comma1  string `parser:"','"`
		Hashes  string `parser:"@Number"`
		Comma2  string `parser:"','"`
		Seed    string `parser:"@Number"`
		Close   string `parser:"')'"`
	}

	// IndexNGramBFType represents ngrambf_v1(n, size, hashes, seed) index type
	IndexNGramBFType struct {
		NGramBF string `parser:"'ngrambf_v1' '('"`
		N       string `parser:"@Number"`
		Comma1  string `parser:"','"`
		Size    string `parser:"@Number"`
		Comma2  string `parser:"','"`
		Hashes  string `parser:"@Number"`
		Comma3  string `parser:"','"`
		Seed    string `parser:"@Number"`
		Close   string `parser:"')'"`
	}

	// ConstraintDefinition represents a CONSTRAINT definition within CREATE TABLE
	// ClickHouse syntax:
	//   CONSTRAINT constraint_name CHECK expression
	ConstraintDefinition struct {
		Constraint string     `parser:"'CONSTRAINT'"`
		Name       string     `parser:"@(Ident | BacktickIdent)"`
		Check      string     `parser:"'CHECK'"`
		Expression Expression `parser:"@@"`
	}

	// ProjectionDefinition represents a PROJECTION definition within CREATE TABLE
	// ClickHouse syntax:
	//   PROJECTION projection_name (SELECT ... [GROUP BY ...] [ORDER BY ...])
	ProjectionDefinition struct {
		Projection  string            `parser:"'PROJECTION'"`
		Name        string            `parser:"@(Ident | BacktickIdent)"`
		SelectClause ProjectionSelect `parser:"@@"`
	}

	// ProjectionSelect represents the SELECT clause within projection parentheses
	ProjectionSelect struct {
		OpenParen   string                     `parser:"'('"`
		Select      string                     `parser:"'SELECT'"`
		Columns     []ProjectionSelectColumn   `parser:"@@ (',' @@)*"`
		GroupBy     *ProjectionGroupBy         `parser:"@@?"`
		OrderBy     *ProjectionOrderBy         `parser:"@@?"`
		CloseParen  string                     `parser:"')'"`
	}

	// ProjectionSelectColumn represents columns in projection SELECT
	ProjectionSelectColumn struct {
		Star  *string    `parser:"@'*'"`
		Expr  *Expression `parser:"| @@"`
		Alias *string    `parser:"('AS' @(Ident | BacktickIdent))?"`
	}

	// ProjectionGroupBy represents GROUP BY clause in projections
	ProjectionGroupBy struct {
		GroupBy    string       `parser:"'GROUP' 'BY'"`
		Columns    []Expression `parser:"@@ (',' @@)*"`
	}

	// ProjectionOrderBy represents ORDER BY clause in projections  
	ProjectionOrderBy struct {
		OrderBy    string                   `parser:"'ORDER' 'BY'"`
		Columns    []ProjectionOrderColumn  `parser:"@@ (',' @@)*"`
	}

	// ProjectionOrderColumn represents a column in ORDER BY with optional ASC/DESC
	ProjectionOrderColumn struct {
		Expr      Expression `parser:"@@"`
		Direction *string    `parser:"@('ASC' | 'DESC')?"`
	}

	// TableEngine represents the ENGINE clause for tables
	// Examples: ENGINE = MergeTree(), ENGINE = ReplicatedMergeTree('/path', 'replica')
	TableEngine struct {
		Engine     string            `parser:"'ENGINE' '='"`
		Name       string            `parser:"@(Ident | BacktickIdent)"`
		Parameters []EngineParameter `parser:"'(' (@@ (',' @@)*)? ')'"`
	}

	// EngineParameter represents a parameter in an ENGINE clause
	EngineParameter struct {
		String *string `parser:"@String"`
		Number *string `parser:"| @Number"`
		Ident  *string `parser:"| @(Ident | BacktickIdent)"`
	}

	// OrderByClause represents ORDER BY expression
	OrderByClause struct {
		OrderBy    string     `parser:"'ORDER' 'BY'"`
		Expression Expression `parser:"@@"`
	}

	// PartitionByClause represents PARTITION BY expression
	PartitionByClause struct {
		PartitionBy string     `parser:"'PARTITION' 'BY'"`
		Expression  Expression `parser:"@@"`
	}

	// PrimaryKeyClause represents PRIMARY KEY expression
	PrimaryKeyClause struct {
		PrimaryKey string     `parser:"'PRIMARY' 'KEY'"`
		Expression Expression `parser:"@@"`
	}

	// SampleByClause represents SAMPLE BY expression
	SampleByClause struct {
		SampleBy   string     `parser:"'SAMPLE' 'BY'"`
		Expression Expression `parser:"@@"`
	}

	// TableTTLClause represents table-level TTL expression
	TableTTLClause struct {
		TTL        string     `parser:"'TTL'"`
		Expression Expression `parser:"@@"`
	}

	// TableSettingsClause represents SETTINGS clause
	TableSettingsClause struct {
		Settings []TableSetting `parser:"'SETTINGS' @@ (',' @@)*"`
	}

	// TableSetting represents a single setting in SETTINGS clause
	TableSetting struct {
		Name  string `parser:"@(Ident | BacktickIdent)"`
		Eq    string `parser:"'='"`
		Value string `parser:"@(String | Number | Ident | BacktickIdent)"`
	}

	// AttachTableStmt represents an ATTACH TABLE statement.
	// Used for materialized views: ATTACH TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   ATTACH TABLE [IF NOT EXISTS] [db.]table_name [ON CLUSTER cluster]
	AttachTableStmt struct {
		Attach      string  `parser:"'ATTACH' 'TABLE'"`
		IfNotExists bool    `parser:"('IF' 'NOT' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DetachTableStmt represents a DETACH TABLE statement.
	// Used for materialized views: DETACH TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   DETACH TABLE [IF EXISTS] [db.]table_name [ON CLUSTER cluster] [PERMANENTLY] [SYNC]
	DetachTableStmt struct {
		Detach      string  `parser:"'DETACH' 'TABLE'"`
		IfExists    bool    `parser:"('IF' 'EXISTS')?"`
		Database    *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name        string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster   *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Permanently bool    `parser:"@'PERMANENTLY'?"`
		Sync        bool    `parser:"@'SYNC'?"`
		Semicolon   bool    `parser:"';'"`
	}

	// DropTableStmt represents a DROP TABLE statement.
	// Used for materialized views: DROP TABLE [db.]materialized_view_name
	// ClickHouse syntax:
	//   DROP TABLE [IF EXISTS] [db.]table_name [ON CLUSTER cluster] [SYNC]
	DropTableStmt struct {
		Drop      string  `parser:"'DROP' 'TABLE'"`
		IfExists  bool    `parser:"('IF' 'EXISTS')?"`
		Database  *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Sync      bool    `parser:"@'SYNC'?"`
		Semicolon bool    `parser:"';'"`
	}

	// RenameTableStmt represents a RENAME TABLE statement.
	// Used for both regular views and materialized views.
	// ClickHouse syntax:
	//   RENAME TABLE [db.]table1 TO [db.]table2, [db.]table3 TO [db.]table4, ... [ON CLUSTER cluster]
	RenameTableStmt struct {
		Rename    string        `parser:"'RENAME' 'TABLE'"`
		Renames   []TableRename `parser:"@@ (',' @@)*"`
		OnCluster *string       `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon bool          `parser:"';'"`
	}

	// TableRename represents a single table rename operation
	TableRename struct {
		FromDatabase *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		FromName     string  `parser:"@(Ident | BacktickIdent)"`
		To           string  `parser:"'TO'"`
		ToDatabase   *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		ToName       string  `parser:"@(Ident | BacktickIdent)"`
	}

	// AlterTableStmt represents an ALTER TABLE statement.
	// ClickHouse syntax:
	//   ALTER TABLE [IF EXISTS] [db.]table [ON CLUSTER cluster]
	//   operation1 [, operation2, ...]
	//
	// ClickHouse supports many ALTER TABLE operations including:
	// - ADD/DROP/MODIFY/RENAME COLUMN
	// - ADD/DROP INDEX
	// - ADD/DROP CONSTRAINT
	// - ADD/DROP PROJECTION
	// - MODIFY TTL
	// - UPDATE/DELETE data
	// - FREEZE/ATTACH/DETACH/DROP/MOVE/REPLACE PARTITION
	// - MODIFY ORDER BY/SAMPLE BY
	// - MODIFY SETTING
	AlterTableStmt struct {
		Alter      string                `parser:"'ALTER' 'TABLE'"`
		IfExists   bool                  `parser:"@('IF' 'EXISTS')?"`
		Database   *string               `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name       string                `parser:"@(Ident | BacktickIdent)"`
		OnCluster  *string               `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Operations []AlterTableOperation `parser:"@@ (',' @@)*"`
		Semicolon  bool                  `parser:"';'"`
	}

	// AlterTableOperation represents a single ALTER TABLE operation
	AlterTableOperation struct {
		AddColumn        *AddColumnOperation        `parser:"@@"`
		DropColumn       *DropColumnOperation       `parser:"| @@"`
		ModifyColumn     *ModifyColumnOperation     `parser:"| @@"`
		RenameColumn     *RenameColumnOperation     `parser:"| @@"`
		CommentColumn    *CommentColumnOperation    `parser:"| @@"`
		ClearColumn      *ClearColumnOperation      `parser:"| @@"`
		ModifyTTL        *ModifyTTLOperation        `parser:"| @@"`
		DeleteTTL        *DeleteTTLOperation        `parser:"| @@"`
		AddIndex         *AddIndexOperation         `parser:"| @@"`
		DropIndex        *DropIndexOperation        `parser:"| @@"`
		AddConstraint    *AddConstraintOperation    `parser:"| @@"`
		DropConstraint   *DropConstraintOperation   `parser:"| @@"`
		Update           *UpdateOperation           `parser:"| @@"`
		Delete           *DeleteOperation           `parser:"| @@"`
		Freeze           *FreezeOperation           `parser:"| @@"`
		AttachPartition  *AttachPartitionOperation  `parser:"| @@"`
		DetachPartition  *DetachPartitionOperation  `parser:"| @@"`
		DropPartition    *DropPartitionOperation    `parser:"| @@"`
		MovePartition    *MovePartitionOperation    `parser:"| @@"`
		ReplacePartition *ReplacePartitionOperation `parser:"| @@"`
		FetchPartition   *FetchPartitionOperation   `parser:"| @@"`
		ModifyOrderBy    *ModifyOrderByOperation    `parser:"| @@"`
		ModifySampleBy   *ModifySampleByOperation   `parser:"| @@"`
		RemoveSampleBy   *RemoveSampleByOperation   `parser:"| @@"`
		ModifySetting    *ModifySettingOperation    `parser:"| @@"`
		ResetSetting     *ResetSettingOperation     `parser:"| @@"`
		AddProjection    *AddProjectionOperation    `parser:"| @@"`
		DropProjection   *DropProjectionOperation   `parser:"| @@"`
	}

	// AddColumnOperation represents ADD COLUMN operation
	AddColumnOperation struct {
		Add         string  `parser:"'ADD' 'COLUMN'"`
		IfNotExists bool    `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Column      Column  `parser:"@@"`
		After       *string `parser:"('AFTER' @(Ident | BacktickIdent))?"`
		First       bool    `parser:"@'FIRST'?"`
	}

	// DropColumnOperation represents DROP COLUMN operation
	DropColumnOperation struct {
		Drop     string `parser:"'DROP' 'COLUMN'"`
		IfExists bool   `parser:"@('IF' 'EXISTS')?"`
		Name     string `parser:"@(Ident | BacktickIdent)"`
	}

	// ModifyColumnOperation represents MODIFY COLUMN operation
	ModifyColumnOperation struct {
		Modify   string              `parser:"'MODIFY' 'COLUMN'"`
		IfExists bool                `parser:"@('IF' 'EXISTS')?"`
		Name     string              `parser:"@(Ident | BacktickIdent)"`
		Type     *DataType           `parser:"@@?"`
		Default  *DefaultClause      `parser:"@@?"`
		Codec    *string             `parser:"('CODEC' '(' @String ')')?"`
		TTL      *Expression         `parser:"('TTL' @@)?"`
		Comment  *string             `parser:"('COMMENT' @String)?"`
		Remove   *ModifyColumnRemove `parser:"@@?"`
	}

	// ModifyColumnRemove represents REMOVE clause in MODIFY COLUMN
	ModifyColumnRemove struct {
		Remove string `parser:"'REMOVE'"`
		What   string `parser:"@('DEFAULT' | 'MATERIALIZED' | 'ALIAS' | 'COMMENT' | 'CODEC' | 'TTL')"`
	}

	// RenameColumnOperation represents RENAME COLUMN operation
	RenameColumnOperation struct {
		Rename   string `parser:"'RENAME' 'COLUMN'"`
		IfExists bool   `parser:"@('IF' 'EXISTS')?"`
		From     string `parser:"@(Ident | BacktickIdent)"`
		To       string `parser:"'TO' @(Ident | BacktickIdent)"`
	}

	// CommentColumnOperation represents COMMENT COLUMN operation
	CommentColumnOperation struct {
		Comment  string `parser:"'COMMENT' 'COLUMN'"`
		IfExists bool   `parser:"@('IF' 'EXISTS')?"`
		Name     string `parser:"@(Ident | BacktickIdent)"`
		Value    string `parser:"@String"`
	}

	// ClearColumnOperation represents CLEAR COLUMN operation
	ClearColumnOperation struct {
		Clear     string `parser:"'CLEAR' 'COLUMN'"`
		IfExists  bool   `parser:"@('IF' 'EXISTS')?"`
		Name      string `parser:"@(Ident | BacktickIdent)"`
		In        string `parser:"'IN'"`
		Partition string `parser:"'PARTITION' @(String | Ident | BacktickIdent)"`
	}

	// ModifyTTLOperation represents MODIFY TTL operation
	ModifyTTLOperation struct {
		Modify     string     `parser:"'MODIFY' 'TTL'"`
		Expression Expression `parser:"@@"`
		Delete     *TTLDelete `parser:"@@?"`
	}

	// TTLDelete represents DELETE clause in TTL
	TTLDelete struct {
		Delete string      `parser:"'DELETE'"`
		Where  *Expression `parser:"('WHERE' @@)?"`
	}

	// DeleteTTLOperation represents DELETE TTL operation
	DeleteTTLOperation struct {
		Delete string `parser:"'DELETE' 'TTL'"`
	}

	// AddIndexOperation represents ADD INDEX operation
	AddIndexOperation struct {
		Add         string     `parser:"'ADD' 'INDEX'"`
		IfNotExists bool       `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name        string     `parser:"@(Ident | BacktickIdent)"`
		Expression  Expression `parser:"@@"`
		Type        string     `parser:"'TYPE' @(Ident | BacktickIdent)"`
		Granularity string     `parser:"'GRANULARITY' @Number"`
		After       *string    `parser:"('AFTER' @(Ident | BacktickIdent))?"`
		First       bool       `parser:"@'FIRST'?"`
	}

	// DropIndexOperation represents DROP INDEX operation
	DropIndexOperation struct {
		Drop     string `parser:"'DROP' 'INDEX'"`
		IfExists bool   `parser:"@('IF' 'EXISTS')?"`
		Name     string `parser:"@(Ident | BacktickIdent)"`
	}

	// AddConstraintOperation represents ADD CONSTRAINT operation
	AddConstraintOperation struct {
		Add         string     `parser:"'ADD' 'CONSTRAINT'"`
		IfNotExists bool       `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name        string     `parser:"@(Ident | BacktickIdent)"`
		Check       string     `parser:"'CHECK'"`
		Expression  Expression `parser:"@@"`
	}

	// DropConstraintOperation represents DROP CONSTRAINT operation
	DropConstraintOperation struct {
		Drop     string `parser:"'DROP' 'CONSTRAINT'"`
		IfExists bool   `parser:"@('IF' 'EXISTS')?"`
		Name     string `parser:"@(Ident | BacktickIdent)"`
	}

	// UpdateOperation represents UPDATE operation
	UpdateOperation struct {
		Update     string      `parser:"'UPDATE'"`
		Column     string      `parser:"@(Ident | BacktickIdent)"`
		Eq         string      `parser:"'='"`
		Expression Expression  `parser:"@@"`
		Where      *Expression `parser:"('WHERE' @@)?"`
	}

	// DeleteOperation represents DELETE operation
	DeleteOperation struct {
		Delete string     `parser:"'DELETE'"`
		Where  Expression `parser:"'WHERE' @@"`
	}

	// FreezeOperation represents FREEZE operation
	FreezeOperation struct {
		Freeze    string  `parser:"'FREEZE'"`
		Partition *string `parser:"('PARTITION' @(String | Ident | BacktickIdent))?"`
		With      *string `parser:"('WITH' 'NAME' @String)?"`
	}

	// AttachPartitionOperation represents ATTACH PARTITION operation
	AttachPartitionOperation struct {
		Attach    string               `parser:"'ATTACH' 'PARTITION'"`
		Partition string               `parser:"@(String | Ident | BacktickIdent)"`
		From      *AttachPartitionFrom `parser:"@@?"`
	}

	// AttachPartitionFrom represents FROM clause in ATTACH PARTITION
	AttachPartitionFrom struct {
		From     string  `parser:"'FROM'"`
		Database *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Table    string  `parser:"@(Ident | BacktickIdent)"`
	}

	// DetachPartitionOperation represents DETACH PARTITION operation
	DetachPartitionOperation struct {
		Detach    string `parser:"'DETACH' 'PARTITION'"`
		Partition string `parser:"@(String | Ident | BacktickIdent)"`
	}

	// DropPartitionOperation represents DROP PARTITION operation
	DropPartitionOperation struct {
		Drop      string `parser:"'DROP' 'PARTITION'"`
		Partition string `parser:"@(String | Ident | BacktickIdent)"`
	}

	// MovePartitionOperation represents MOVE PARTITION operation
	MovePartitionOperation struct {
		Move      string       `parser:"'MOVE' 'PARTITION'"`
		Partition string       `parser:"@(String | Ident | BacktickIdent)"`
		To        string       `parser:"'TO'"`
		Disk      *string      `parser:"(('DISK' @String)"`
		Volume    *string      `parser:"| ('VOLUME' @String)"`
		Table     *MoveToTable `parser:"| ('TABLE' @@))?"`
	}

	// MoveToTable represents destination table in MOVE PARTITION
	MoveToTable struct {
		Database *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Name     string  `parser:"@(Ident | BacktickIdent)"`
	}

	// ReplacePartitionOperation represents REPLACE PARTITION operation
	ReplacePartitionOperation struct {
		Replace   string  `parser:"'REPLACE' 'PARTITION'"`
		Partition string  `parser:"@(String | Ident | BacktickIdent)"`
		From      string  `parser:"'FROM'"`
		Database  *string `parser:"(@(Ident | BacktickIdent) '.')?"`
		Table     string  `parser:"@(Ident | BacktickIdent)"`
	}

	// FetchPartitionOperation represents FETCH PARTITION operation
	FetchPartitionOperation struct {
		Fetch     string `parser:"'FETCH' 'PARTITION'"`
		Partition string `parser:"@(String | Ident | BacktickIdent)"`
		From      string `parser:"'FROM' @String"`
	}

	// ModifyOrderByOperation represents MODIFY ORDER BY operation
	ModifyOrderByOperation struct {
		Modify     string     `parser:"'MODIFY' 'ORDER' 'BY'"`
		Expression Expression `parser:"@@"`
	}

	// ModifySampleByOperation represents MODIFY SAMPLE BY operation
	ModifySampleByOperation struct {
		Modify     string     `parser:"'MODIFY' 'SAMPLE' 'BY'"`
		Expression Expression `parser:"@@"`
	}

	// RemoveSampleByOperation represents REMOVE SAMPLE BY operation
	RemoveSampleByOperation struct {
		Remove string `parser:"'REMOVE' 'SAMPLE' 'BY'"`
	}

	// ModifySettingOperation represents MODIFY SETTING operation
	ModifySettingOperation struct {
		Modify  string       `parser:"'MODIFY' 'SETTING'"`
		Setting TableSetting `parser:"@@"`
	}

	// ResetSettingOperation represents RESET SETTING operation
	ResetSettingOperation struct {
		Reset string `parser:"'RESET' 'SETTING'"`
		Name  string `parser:"@(Ident | BacktickIdent)"`
	}

	// AddProjectionOperation represents ADD PROJECTION operation
	// ClickHouse syntax:
	//   ADD PROJECTION [IF NOT EXISTS] projection_name (SELECT ...)
	AddProjectionOperation struct {
		Add           string            `parser:"'ADD' 'PROJECTION'"`
		IfNotExists   bool              `parser:"@('IF' 'NOT' 'EXISTS')?"`
		Name          string            `parser:"@(Ident | BacktickIdent)"`
		SelectClause  ProjectionSelect  `parser:"@@"`
	}

	// DropProjectionOperation represents DROP PROJECTION operation
	// ClickHouse syntax:
	//   DROP PROJECTION [IF EXISTS] projection_name
	DropProjectionOperation struct {
		Drop       string `parser:"'DROP' 'PROJECTION'"`
		IfExists   bool   `parser:"@('IF' 'EXISTS')?"`
		Name       string `parser:"@(Ident | BacktickIdent)"`
	}
)
