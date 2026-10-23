// Package tlsconfig builds mTLS transport credentials: every workload presents
// its own certificate and verifies peers against the shared CA.
package tlsconfig

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"google.golang.org/grpc/credentials"
)

type Files struct {
	CertFile string
	KeyFile  string
	CAFile   string
}

func FilesFromEnv() Files {
	return Files{
		CertFile: os.Getenv("TLS_CERT_FILE"),
		KeyFile:  os.Getenv("TLS_KEY_FILE"),
		CAFile:   os.Getenv("TLS_CA_FILE"),
	}
}

func (files Files) load() (tls.Certificate, *x509.CertPool, error) {
	certificate, err := tls.LoadX509KeyPair(files.CertFile, files.KeyFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("load key pair: %w", err)
	}
	caPEM, err := os.ReadFile(files.CAFile)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("read CA: %w", err)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		return tls.Certificate{}, nil, errors.New("CA file contains no certificates")
	}
	return certificate, caPool, nil
}

func ServerCredentials(files Files) (credentials.TransportCredentials, error) {
	certificate, caPool, err := files.load()
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{certificate},
		ClientCAs:    caPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

func ClientCredentials(files Files) (credentials.TransportCredentials, error) {
	tlsConfig, err := ClientConfig(files)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(tlsConfig), nil
}

// ClientConfig is the mTLS client configuration for non-gRPC clients (AMQP, SMTP).
// ServerName is left empty so each client derives it from the address it dials.
func ClientConfig(files Files) (*tls.Config, error) {
	certificate, caPool, err := files.load()
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      caPool,
		MinVersion:   tls.VersionTLS13,
	}, nil
}
