package kafkaclient

import (
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
)

// TLSConfig builds the *tls.Config implied by this Config, or nil when the
// protocol is not a TLS one.
//
// An empty CACertFile is not an error: a managed cluster with a
// publicly-trusted certificate (Confluent Cloud, Aiven) is verified by the
// system roots, and forcing operators to supply a CA bundle they do not have
// would be worse than useless.
func (c Config) TLSConfig() (*tls.Config, error) {
	if !c.UsesTLS() {
		return nil, nil
	}

	t := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: c.InsecureSkipVerify, //nolint:gosec // operator-controlled, warned about in Warnings()
	}

	if c.CACertFile != "" {
		pem, err := os.ReadFile(c.CACertFile)
		if err != nil {
			return nil, fmt.Errorf("kafkaclient: reading %s=%q: %w", eitherName(EnvSSLCALocation, EnvTLSCAAlias), c.CACertFile, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			// AppendCertsFromPEM reports only "nothing was added", so say what
			// that actually means rather than passing the silence along.
			return nil, fmt.Errorf("kafkaclient: %s=%q contained no PEM certificates", eitherName(EnvSSLCALocation, EnvTLSCAAlias), c.CACertFile)
		}
		t.RootCAs = pool
	}

	if c.ClientCertFile != "" {
		cert, err := tls.LoadX509KeyPair(c.ClientCertFile, c.ClientKeyFile)
		if err != nil {
			return nil, fmt.Errorf("kafkaclient: loading client keypair (%s=%q, %s=%q): %w",
				eitherName(EnvSSLCertLocation, EnvTLSCertAlias), c.ClientCertFile,
				eitherName(EnvSSLKeyLocation, EnvTLSKeyAlias), c.ClientKeyFile, err)
		}
		t.Certificates = []tls.Certificate{cert}
	}

	return t, nil
}

// eitherName renders both accepted spellings of a setting.
//
// The path is read from whichever variable was set, so an error that named only
// the canonical one would tell an operator who followed the documentation to go
// look at a variable they never set — and the thing they are already debugging
// is a certificate that appears not to be loading.
func eitherName(canonical, alias string) string {
	return canonical + " (or " + alias + ")"
}
