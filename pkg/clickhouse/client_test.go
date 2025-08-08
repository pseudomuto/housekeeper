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
		name string
		dsn  string
		msg  string
	}{
		{
			name: "valid simple host:port",
			dsn:  "localhost:9000",
		},
		{
			name: "valid clickhouse:// DSN",
			dsn:  "clickhouse://default:@localhost:9000/default",
		},
		{
			name: "valid tcp:// DSN",
			dsn:  "tcp://localhost:9000?username=default&password=&database=default",
		},
		{
			name: "invalid host format",
			dsn:  "malformed[host:9000",
			msg:  "unexpected '[' in address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.msg == "" {
				tt.msg = "connection refused"
			}

			_, err := clickhouse.NewClient(ctx, tt.dsn)
			require.Error(t, err)
			require.Contains(t, err.Error(), tt.msg)
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
			require.Contains(t, strings.ToLower(err.Error()), "connection refused")
		})
	}
}
