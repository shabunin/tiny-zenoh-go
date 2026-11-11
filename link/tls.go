//go:build !tinygo

package link

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

// ErrTLSConfig reports missing or unreadable TLS material.
var ErrTLSConfig = errors.New("link: tls configuration")

// tlsMaterial resolves one PEM item: locator config (_raw, _base64, _file)
// first, then the file named in the TLSConfig.
func tlsMaterial(cfg map[string]string, key, file string) ([]byte, error) {
	if v, ok := cfg[key+"_raw"]; ok {
		return []byte(v), nil
	}
	if v, ok := cfg[key+"_base64"]; ok {
		b, err := base64.StdEncoding.DecodeString(v)
		if err != nil {
			return nil, fmt.Errorf("%w: %s_base64: %v", ErrTLSConfig, key, err)
		}
		return b, nil
	}
	if v, ok := cfg[key+"_file"]; ok {
		file = v
	}
	if file == "" {
		return nil, nil
	}
	b, err := os.ReadFile(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %s: %v", ErrTLSConfig, key, err)
	}
	return b, nil
}

func tlsBool(cfg map[string]string, key string, def bool) bool {
	switch cfg[key] {
	case "true":
		return true
	case "false":
		return false
	}
	return def
}

type tlsSettings struct {
	roots      *x509.CertPool // nil: system roots
	cert       *tls.Certificate
	mtls       bool
	verifyName bool
}

// tlsSettingsFor gathers the settings for a locator; listen picks the
// listen_* or connect_* certificate.
func tlsSettingsFor(l Locator, def *TLSConfig, listen bool) (*tlsSettings, error) {
	if def == nil {
		def = &TLSConfig{}
	}
	cfg := l.ConfigValues()
	st := &tlsSettings{
		mtls:       tlsBool(cfg, "enable_mtls", def.EnableMTLS),
		verifyName: tlsBool(cfg, "verify_name_on_connect", !def.SkipNameVerification),
	}
	ca, err := tlsMaterial(cfg, "root_ca_certificate", def.RootCACertificate)
	if err != nil {
		return nil, err
	}
	if ca != nil {
		st.roots = x509.NewCertPool()
		if !st.roots.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("%w: no certificate in root_ca_certificate", ErrTLSConfig)
		}
	}
	certKey, keyKey, certFile, keyFile := "connect_certificate", "connect_private_key", def.ConnectCertificate, def.ConnectPrivateKey
	if listen {
		certKey, keyKey, certFile, keyFile = "listen_certificate", "listen_private_key", def.ListenCertificate, def.ListenPrivateKey
	}
	certPEM, err := tlsMaterial(cfg, certKey, certFile)
	if err != nil {
		return nil, err
	}
	keyPEM, err := tlsMaterial(cfg, keyKey, keyFile)
	if err != nil {
		return nil, err
	}
	if certPEM != nil || keyPEM != nil {
		c, err := tls.X509KeyPair(certPEM, keyPEM)
		if err != nil {
			return nil, fmt.Errorf("%w: %s: %v", ErrTLSConfig, certKey, err)
		}
		st.cert = &c
	}
	return st, nil
}

// TLSClientConfig builds the client-side tls.Config for a locator (tls/ or
// quic/): its config keys over def.
func TLSClientConfig(l Locator, def *TLSConfig) (*tls.Config, error) {
	st, err := tlsSettingsFor(l, def, false)
	if err != nil {
		return nil, err
	}
	host, _, err := net.SplitHostPort(l.Address)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{ServerName: host, RootCAs: st.roots, MinVersion: tls.VersionTLS12}
	if st.cert != nil {
		cfg.Certificates = []tls.Certificate{*st.cert}
	} else if st.mtls {
		return nil, fmt.Errorf("%w: enable_mtls needs connect_certificate and connect_private_key", ErrTLSConfig)
	}
	if !st.verifyName {
		// Verify the chain ourselves, without the host name.
		roots := st.roots
		cfg.InsecureSkipVerify = true
		cfg.VerifyConnection = func(cs tls.ConnectionState) error {
			if len(cs.PeerCertificates) == 0 {
				return errors.New("link: tls server sent no certificate")
			}
			opts := x509.VerifyOptions{Roots: roots, Intermediates: x509.NewCertPool()}
			for _, c := range cs.PeerCertificates[1:] {
				opts.Intermediates.AddCert(c)
			}
			_, err := cs.PeerCertificates[0].Verify(opts)
			return err
		}
	}
	return cfg, nil
}

// TLSServerConfig builds the server-side tls.Config for a locator (tls/ or
// quic/): its config keys over def.
func TLSServerConfig(l Locator, def *TLSConfig) (*tls.Config, error) {
	st, err := tlsSettingsFor(l, def, true)
	if err != nil {
		return nil, err
	}
	if st.cert == nil {
		return nil, fmt.Errorf("%w: a %s listener needs listen_certificate and listen_private_key", ErrTLSConfig, l.Protocol)
	}
	cfg := &tls.Config{Certificates: []tls.Certificate{*st.cert}, MinVersion: tls.VersionTLS12}
	if st.mtls {
		if st.roots == nil {
			return nil, fmt.Errorf("%w: enable_mtls needs root_ca_certificate", ErrTLSConfig)
		}
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
		cfg.ClientCAs = st.roots
	}
	return cfg, nil
}

func dialTLS(l Locator, timeout time.Duration, def *TLSConfig) (transport.Link, error) {
	cfg, err := TLSClientConfig(l, def)
	if err != nil {
		return nil, err
	}
	d := &net.Dialer{Timeout: timeout}
	c, err := tls.DialWithDialer(d, "tcp", l.Address, cfg)
	if err != nil {
		return nil, err
	}
	if tc, ok := c.NetConn().(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return c, nil
}

type tlsListener struct {
	ln  net.Listener
	cfg *tls.Config
	loc Locator
}

func listenTLS(l Locator, bound Locator, def *TLSConfig) (Listener, error) {
	cfg, err := TLSServerConfig(l, def)
	if err != nil {
		return nil, err
	}
	ln, err := net.Listen("tcp", l.Address)
	if err != nil {
		return nil, err
	}
	bound.Address = ln.Addr().String()
	return &tlsListener{ln: ln, cfg: cfg, loc: bound}, nil
}

// Accept returns the next connection; its TLS handshake runs on first use,
// under the transport's handshake deadline.
func (l *tlsListener) Accept() (transport.Link, error) {
	c, err := l.ln.Accept()
	if err != nil {
		return nil, err
	}
	if tc, ok := c.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	return tls.Server(c, l.cfg), nil
}

func (l *tlsListener) Close() error     { return l.ln.Close() }
func (l *tlsListener) Locator() Locator { return l.loc }

func tlsCommonName(l any) string {
	c, ok := l.(*tls.Conn)
	if !ok {
		return ""
	}
	if st := c.ConnectionState(); len(st.PeerCertificates) > 0 {
		return st.PeerCertificates[0].Subject.CommonName
	}
	return ""
}
