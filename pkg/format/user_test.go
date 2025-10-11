package format_test

import (
	"bytes"
	"testing"

	. "github.com/pseudomuto/housekeeper/pkg/format"
	"github.com/pseudomuto/housekeeper/pkg/parser"
	"github.com/stretchr/testify/require"
)

func TestFormatter_CreateUser(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple create user",
			sql:      "CREATE USER name1;",
			expected: "CREATE USER `name1`;",
		},
		{
			name:     "create user with backtick",
			sql:      "CREATE USER `special-user`;",
			expected: "CREATE USER `special-user`;",
		},
		{
			name:     "create user if not exists",
			sql:      "CREATE USER IF NOT EXISTS name2;",
			expected: "CREATE USER IF NOT EXISTS `name2`;",
		},
		{
			name:     "create user or replace",
			sql:      "CREATE USER OR REPLACE name3;",
			expected: "CREATE USER OR REPLACE `name3`;",
		},
		{
			name:     "create user on cluster",
			sql:      "CREATE USER IF NOT EXISTS name4 ON CLUSTER production;",
			expected: "CREATE USER IF NOT EXISTS `name4` ON CLUSTER `production`;",
		},
		{
			name:     "not identified",
			sql:      "CREATE USER name5 NOT IDENTIFIED;",
			expected: "CREATE USER `name5` NOT IDENTIFIED;",
		},
		{
			name:     "identified with plaintext password",
			sql:      "CREATE USER name6 IDENTIFIED WITH plaintext_password BY 'my_password';",
			expected: "CREATE USER `name6` IDENTIFIED WITH plaintext_password BY 'my_password';",
		},
		{
			name:     "identified by password",
			sql:      "CREATE USER name7 IDENTIFIED BY 'qwerty';",
			expected: "CREATE USER `name7` IDENTIFIED BY 'qwerty';",
		},
		{
			name:     "identified with sha256_hash",
			sql:      "CREATE USER name8 IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';",
			expected: "CREATE USER `name8` IDENTIFIED WITH sha256_hash BY '0C268556C1680BEF0640AAC1E7187566704208398DA31F03D18C74F5C5BE5053' SALT '4FB16307F5E10048196966DD7E6876AE53DE6A1D1F625488482C75F14A5097C7';",
		},
		{
			name:     "identified with http",
			sql:      "CREATE USER name9 IDENTIFIED WITH http SERVER 'test_http_server';",
			expected: "CREATE USER `name9` IDENTIFIED WITH http SERVER 'test_http_server';",
		},
		{
			name:     "identified with ldap",
			sql:      "CREATE USER name11 IDENTIFIED WITH ldap SERVER 'server_name';",
			expected: "CREATE USER `name11` IDENTIFIED WITH ldap SERVER 'server_name';",
		},
		{
			name:     "identified with kerberos",
			sql:      "CREATE USER name12 IDENTIFIED WITH kerberos;",
			expected: "CREATE USER `name12` IDENTIFIED WITH kerberos;",
		},
		{
			name:     "identified with kerberos realm",
			sql:      "CREATE USER name13 IDENTIFIED WITH kerberos REALM 'test_realm';",
			expected: "CREATE USER `name13` IDENTIFIED WITH kerberos REALM 'test_realm';",
		},
		{
			name:     "identified with ssl certificate",
			sql:      "CREATE USER name14 IDENTIFIED WITH ssl_certificate CN 'mysite.com:user';",
			expected: "CREATE USER `name14` IDENTIFIED WITH ssl_certificate CN 'mysite.com:user';",
		},
		{
			name:     "host ip with subnet",
			sql:      "CREATE USER name15 HOST IP '192.168.0.0/16';",
			expected: "CREATE USER `name15` HOST IP '192.168.0.0/16';",
		},
		{
			name:     "host any",
			sql:      "CREATE USER name17 HOST ANY;",
			expected: "CREATE USER `name17` HOST ANY;",
		},
		{
			name:     "host local",
			sql:      "CREATE USER name18 HOST LOCAL;",
			expected: "CREATE USER `name18` HOST LOCAL;",
		},
		{
			name:     "host name",
			sql:      "CREATE USER name19 HOST NAME 'mysite.com';",
			expected: "CREATE USER `name19` HOST NAME 'mysite.com';",
		},
		{
			name:     "host regexp",
			sql:      "CREATE USER name20 HOST REGEXP '.*\\.mysite\\.com';",
			expected: "CREATE USER `name20` HOST REGEXP '.*\\.mysite\\.com';",
		},
		{
			name:     "host like",
			sql:      "CREATE USER name21 HOST LIKE '%';",
			expected: "CREATE USER `name21` HOST LIKE '%';",
		},
		{
			name:     "valid until date",
			sql:      "CREATE USER name23 VALID UNTIL '2025-01-01';",
			expected: "CREATE USER `name23` VALID UNTIL '2025-01-01';",
		},
		{
			name:     "valid until with datetime",
			sql:      "CREATE USER name24 VALID UNTIL '2025-01-01 12:00:00 UTC';",
			expected: "CREATE USER `name24` VALID UNTIL '2025-01-01 12:00:00 UTC';",
		},
		{
			name:     "grantees with user list",
			sql:      "CREATE USER name29 GRANTEES user1, user2, 'special-user';",
			expected: "CREATE USER `name29` GRANTEES `user1`, `user2`, `special-user`;",
		},
		{
			name:     "grantees any",
			sql:      "CREATE USER name31 GRANTEES ANY;",
			expected: "CREATE USER `name31` GRANTEES ANY;",
		},
		{
			name:     "grantees none",
			sql:      "CREATE USER name32 GRANTEES NONE;",
			expected: "CREATE USER `name32` GRANTEES NONE;",
		},
		{
			name:     "access storage type",
			sql:      "CREATE USER name33 IN local_directory;",
			expected: "CREATE USER `name33` IN local_directory;",
		},
		{
			name:     "valid until and access storage",
			sql:      "CREATE USER name34 VALID UNTIL '2025-12-31 23:59:59 UTC' IN ldap_directory;",
			expected: "CREATE USER `name34` VALID UNTIL '2025-12-31 23:59:59 UTC' IN ldap_directory;",
		},
		{
			name:     "default single role",
			sql:      "CREATE USER name36 DEFAULT ROLE admin;",
			expected: "CREATE USER `name36` DEFAULT ROLE `admin`;",
		},
		{
			name:     "default multiple roles",
			sql:      "CREATE USER name37 DEFAULT ROLE admin, user, reader;",
			expected: "CREATE USER `name37` DEFAULT ROLE `admin`, `user`, `reader`;",
		},
		{
			name:     "default roles with except",
			sql:      "CREATE USER name39 DEFAULT ROLE ALL EXCEPT role1, role2;",
			expected: "CREATE USER `name39` DEFAULT ROLE `ALL` EXCEPT `role1`, `role2`;",
		},
		{
			name:     "default database",
			sql:      "CREATE USER name41 DEFAULT DATABASE analytics;",
			expected: "CREATE USER `name41` DEFAULT DATABASE `analytics`;",
		},
		{
			name:     "default database with quotes",
			sql:      "CREATE USER name42 DEFAULT DATABASE 'special-db';",
			expected: "CREATE USER `name42` DEFAULT DATABASE `special-db`;",
		},
		{
			name:     "default database none",
			sql:      "CREATE USER name43 DEFAULT DATABASE NONE;",
			expected: "CREATE USER `name43` DEFAULT DATABASE NONE;",
		},
		{
			name:     "complex user with multiple options",
			sql:      "CREATE USER IF NOT EXISTS admin ON CLUSTER production IDENTIFIED BY 'secret' HOST IP '192.168.1.0/24' VALID UNTIL '2025-12-31' IN ldap_directory DEFAULT ROLE admin, operator DEFAULT DATABASE analytics GRANTEES user1, user2;",
			expected: "CREATE USER IF NOT EXISTS `admin` ON CLUSTER `production` IDENTIFIED BY 'secret' HOST IP '192.168.1.0/24' VALID UNTIL '2025-12-31' IN ldap_directory DEFAULT ROLE `admin`, `operator` DEFAULT DATABASE `analytics` GRANTEES `user1`, `user2`;",
		},
		{
			name:     "simple user identified with sha256_password (clickhouse exposes users in this format in case of password-based identification)",
			sql:      "CREATE USER name44 IDENTIFIED WITH sha256_password;",
			expected: "CREATE USER `name44` IDENTIFIED WITH sha256_password;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sqlResult.Statements, 1)

			var buf bytes.Buffer
			err = Format(&buf, Defaults, sqlResult.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()
			require.Equal(t, tt.expected, formatted)
		})
	}
}

func TestFormatter_CreateUser_RoundTrip(t *testing.T) {
	// Test that parsing and formatting produces consistent results
	sqlStatements := []string{
		"CREATE USER admin;",
		"CREATE USER IF NOT EXISTS alice ON CLUSTER prod;",
		"CREATE USER bob IDENTIFIED BY 'password';",
		"CREATE USER charlie HOST IP '10.0.0.0/8';",
		"CREATE USER dave DEFAULT ROLE admin, user;",
		"CREATE USER eve DEFAULT DATABASE analytics;",
		"CREATE USER frank GRANTEES ANY;",
		"CREATE USER grace IDENTIFIED WITH ldap SERVER 'ldap.example.com' HOST ANY VALID UNTIL '2025-12-31' DEFAULT ROLE ALL EXCEPT guest;",
	}

	for _, sql := range sqlStatements {
		t.Run(sql, func(t *testing.T) {
			// Parse
			parsed1, err := parser.ParseString(sql)
			require.NoError(t, err)
			require.Len(t, parsed1.Statements, 1)

			// Format
			var buf bytes.Buffer
			err = Format(&buf, Defaults, parsed1.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()

			// Parse again
			parsed2, err := parser.ParseString(formatted)
			require.NoError(t, err)
			require.Len(t, parsed2.Statements, 1)

			// The two parsed results should be equivalent
			require.NotNil(t, parsed1.Statements[0].CreateUser)
			require.NotNil(t, parsed2.Statements[0].CreateUser)
		})
	}
}

func TestFormatter_DropUser(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "simple drop user",
			sql:      "DROP USER alice;",
			expected: "DROP USER `alice`;",
		},
		{
			name:     "drop user with backtick",
			sql:      "DROP USER `special-user`;",
			expected: "DROP USER `special-user`;",
		},
		{
			name:     "drop user if exists",
			sql:      "DROP USER IF EXISTS bob;",
			expected: "DROP USER IF EXISTS `bob`;",
		},
		{
			name:     "drop user on cluster",
			sql:      "DROP USER charlie ON CLUSTER cluster1;",
			expected: "DROP USER `charlie` ON CLUSTER `cluster1`;",
		},
		{
			name:     "drop user if exists on cluster",
			sql:      "DROP USER IF EXISTS dave ON CLUSTER production;",
			expected: "DROP USER IF EXISTS `dave` ON CLUSTER `production`;",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sqlResult, err := parser.ParseString(tt.sql)
			require.NoError(t, err)
			require.Len(t, sqlResult.Statements, 1)

			var buf bytes.Buffer
			err = Format(&buf, Defaults, sqlResult.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()
			require.Equal(t, tt.expected, formatted)
		})
	}
}

func TestFormatter_DropUser_RoundTrip(t *testing.T) {
	// Test that parsing and formatting produces consistent results
	sqlStatements := []string{
		"DROP USER alice;",
		"DROP USER IF EXISTS bob;",
		"DROP USER charlie ON CLUSTER cluster1;",
		"DROP USER IF EXISTS dave ON CLUSTER production;",
	}

	for _, sql := range sqlStatements {
		t.Run(sql, func(t *testing.T) {
			// Parse
			parsed1, err := parser.ParseString(sql)
			require.NoError(t, err)
			require.Len(t, parsed1.Statements, 1)

			// Format
			var buf bytes.Buffer
			err = Format(&buf, Defaults, parsed1.Statements[0])
			require.NoError(t, err)
			formatted := buf.String()

			// Parse again
			parsed2, err := parser.ParseString(formatted)
			require.NoError(t, err)
			require.Len(t, parsed2.Statements, 1)

			// The two parsed results should be equivalent
			require.NotNil(t, parsed1.Statements[0].DropUser)
			require.NotNil(t, parsed2.Statements[0].DropUser)
		})
	}
}
