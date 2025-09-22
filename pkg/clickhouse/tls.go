package clickhouse

import (
	"crypto/tls"
	"crypto/x509"
	"os"

	"github.com/pkg/errors"
)

// GetTLSConfig creates a TLS config for connection to clickhouse over mTLS
//
// Example usage:
//
// tls, err := GetTLSConfig(opts)
//
//	if err != nil {
//			return err
//	}
func GetTLSConfig(opts ClientOptions) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(opts.CertFile, opts.KeyFile)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to load certfile/keyfile")
	}

	caCert, err := os.ReadFile(opts.CAFile)
	if err != nil {
		return nil, errors.Wrap(err, "Unable to load CAfile")
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	return &tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      caCertPool,
		MinVersion:   tls.VersionTLS12,
	}, nil
}
