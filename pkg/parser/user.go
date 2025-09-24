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
		Host           *UserHost           `parser:"@@?"`
		ValidUntil     *string             `parser:"('VALID' 'UNTIL' @String)?"`
		Semicolon      bool                `parser:"';'"`
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
)
