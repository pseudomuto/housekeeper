package parser

type (
	CreateUserStmt struct {
		Create   string `parser:"'CREATE'"`
		User     string `parser:"'USER'"`
		Modifier *struct {
			IfNotExists bool `parser:"  @('IF' 'NOT' 'EXISTS')"`
			OrReplace   bool `parser:"| @('OR' 'REPLACE')"`
		} `parser:"@@?"`
		Name           string              `parser:"@(Ident | BacktickIdent)"`
		OnCluster      *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Identification *UserIdentification `parser:"@@?"`
		Semicolon      bool                `parser:"';'"`
	}

	UserIdentification struct {
		NotIdentified bool                      `parser:"(@'NOT' 'IDENTIFIED')"`
		Identified    *UserIdentificationMethod `parser:"| 'IDENTIFIED' @@"`
	}

	UserIdentificationMethod struct {
		With *string `parser:"('WITH' @Ident)?"`
		By   string  `parser:"'BY' @String"`
	}
)
