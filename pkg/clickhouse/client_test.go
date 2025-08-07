package clickhouse_test

import (
	"context"
	"strings"
	"testing"

	"github.com/pseudomuto/housekeeper/pkg/clickhouse"
	"github.com/stretchr/testify/require"
)

func TestNewClient_DSNParsing(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name      string
		dsn       string
		shouldErr bool
		errMsg    string
	}{
		{
			name:      "valid simple host:port",
			dsn:       "localhost:9000",
			shouldErr: true,                    // Will fail connection but DSN should parse
			errMsg:    "authentication failed", // Should fail at connection, not parsing
		},
		{
			name:      "valid clickhouse:// DSN",
			dsn:       "clickhouse://default:@localhost:9000/default",
			shouldErr: true,                    // Will fail connection but DSN should parse
			errMsg:    "authentication failed", // Should fail at connection, not parsing
		},
		{
			name:      "valid tcp:// DSN",
			dsn:       "tcp://localhost:9000?username=default&password=&database=default",
			shouldErr: true,                    // Will fail connection but DSN should parse
			errMsg:    "authentication failed", // Should fail at connection, not parsing
		},
		{
			name:      "invalid host format",
			dsn:       "malformed[host:9000",
			shouldErr: true,
			errMsg:    "failed to connect", // Will still try to connect with malformed address
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := clickhouse.NewClient(ctx, tt.dsn)

			if tt.shouldErr {
				require.Error(t, err)
				require.Contains(t, strings.ToLower(err.Error()), tt.errMsg,
					"Expected error to contain '%s' but got: %v", tt.errMsg, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestNewClientWithOptions(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		dsn     string
		options clickhouse.ClientOptions
	}{
		{
			name: "client with cluster option",
			dsn:  "localhost:9000",
			options: clickhouse.ClientOptions{
				Cluster: "test_cluster",
			},
		},
		{
			name:    "client with empty options",
			dsn:     "localhost:9000",
			options: clickhouse.ClientOptions{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client, err := clickhouse.NewClientWithOptions(ctx, tt.dsn, tt.options)

			// Should fail connection but succeed in creating client
			require.Error(t, err)
			require.Nil(t, client)
			require.Contains(t, strings.ToLower(err.Error()), "authentication failed")
		})
	}
}
