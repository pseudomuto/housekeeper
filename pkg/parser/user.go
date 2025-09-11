package parser

type (
	CreateUserStmt struct {
		Create   string `parser:"'CREATE'"`
		User     string `parser:"'USER'"`
		Modifier *struct {
			IfNotExists bool `parser:"  @('IF' 'NOT' 'EXISTS')"`
			OrReplace   bool `parser:"| @('OR' 'REPLACE')"`
		} `parser:"@@?"`
		Name              string              `parser:"@(Ident | BacktickIdent)"`
		OnCluster         *string             `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Identification    *UserIdentification `parser:"@@?"`
		Host              *UserHost           `parser:"@@?"`
		ValidUntil        *string             `parser:"('VALID' 'UNTIL' @String)?"`
		AccessStorageType *string             `parser:"('IN' @Ident)?"`
		Roles             *Roles              `parser:"@@?"`
		DefaultDatabase   *DefaultDatabase    `parser:"@@?"`
		Grantees          *UserGrantees       `parser:"@@?"`
		Semicolon         bool                `parser:"';'"`
	}

	UserIdentification struct {
		NotIdentified                bool                              `parser:"(@'NOT' 'IDENTIFIED')"`
		IdentifiedWithHttp           *string                           `parser:"| 'IDENTIFIED' 'WITH' 'HTTP' 'SERVER' @String"`
		IdentifiedWithLdap           *string                           `parser:"| 'IDENTIFIED' 'WITH' 'ldap' 'SERVER' @String"`
		IdentifiedWithKerberos       *UserIdentificationKerberosMethod `parser:"| 'IDENTIFIED' 'WITH' 'kerberos' @@"`
		IdentifiedWithSslCertificate *string                           `parser:"| 'IDENTIFIED' 'WITH' 'ssl_certificate' 'CN' @String"`
		IdentifiedWithOther          *UserIdentificationOtherMethod    `parser:"| 'IDENTIFIED' @@"`
	}

	UserIdentificationKerberosMethod struct {
		Realm *string `parser:"('REALM' @String)?"`
	}

	UserIdentificationOtherMethod struct {
		With       *string `parser:"('WITH' @Ident)?"`
		By         string  `parser:"'BY' @String"`
		Salt       *string `parser:"('SALT' @String)?"`
		ValidUntil *string `parser:"('VALID' 'UNTIL' @String)?"`
	}

	UserHost struct {
		IP     *string `parser:"'HOST' 'IP' @String"`
		Any    bool    `parser:"| 'HOST' @'ANY'"`
		Local  bool    `parser:"| 'HOST' @'LOCAL'"`
		Name   *string `parser:"| 'HOST' 'NAME' @String"`
		Regexp *string `parser:"| 'HOST' 'REGEXP' @String"`
		Like   *string `parser:"| 'HOST' 'LIKE' @String"`
	}

	UserGrantees struct {
		Grantees string        `parser:"'GRANTEES'"`
		Items    []GranteeItem `parser:"@@ (',' @@)*"`
		Except   *UserExcept   `parser:"@@?"`
	}

	GranteeItem struct {
		UserOrRole *string `parser:"@(Ident | BacktickIdent | String)"`
		Any        bool    `parser:"| @'ANY'"`
		None       bool    `parser:"| @'NONE'"`
	}

	UserExcept struct {
		Except string        `parser:"'EXCEPT'"`
		Items  []GranteeItem `parser:"@@ (',' @@)*"`
	}

	Roles struct {
		Roles  []string `parser:"'DEFAULT' 'ROLE' @(Ident | BacktickIdent | String) (',' @(Ident | BacktickIdent | String))*"`
		Except []string `parser:"('EXCEPT' @(Ident | BacktickIdent | String) (',' @(Ident | BacktickIdent | String))*)?"`
	}

	DefaultDatabase struct {
		Database *string `parser:"'DEFAULT' 'DATABASE' @(Ident | BacktickIdent | String)"`
		None     bool    `parser:"| 'DEFAULT' 'DATABASE' @'NONE'"`
	}

	DropUserStmt struct {
		Drop      string  `parser:"'DROP'"`
		User      string  `parser:"'USER'"`
		IfExists  bool    `parser:"@('IF' 'EXISTS')?"`
		Name      string  `parser:"@(Ident | BacktickIdent)"`
		OnCluster *string `parser:"('ON' 'CLUSTER' @(Ident | BacktickIdent))?"`
		Semicolon bool    `parser:"';'"`
	}
)
