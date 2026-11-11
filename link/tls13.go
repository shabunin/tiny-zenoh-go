package link

import (
	"encoding/pem"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/shabunin/tiny-zenoh-go/tls13"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// TLS13ClientConfig builds a package tls13 client configuration for a tls/
// locator: its config keys over def. A root CA is required.
func TLS13ClientConfig(locator string, def *TLSConfig) (*tls13.Config, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	return tls13Config(l, def)
}

func tls13Config(l Locator, def *TLSConfig) (*tls13.Config, error) {
	if def == nil {
		def = &TLSConfig{}
	}
	cfg := l.ConfigValues()
	host, _, err := net.SplitHostPort(l.Address)
	if err != nil {
		return nil, err
	}
	c := &tls13.Config{ServerName: host, SkipNameVerification: !tlsBool(cfg, "verify_name_on_connect", !def.SkipNameVerification)}
	ca, err := tlsMaterial(cfg, "root_ca_certificate", def.RootCACertificate, def.RootCACertificatePEM)
	if err != nil {
		return nil, err
	}
	if ca == nil {
		return nil, fmt.Errorf("%w: root_ca_certificate is required (tls13 has no system roots)", ErrTLSConfig)
	}
	if c.RootCAs, err = tls13.ParseCertificatesPEM(ca); err != nil {
		return nil, fmt.Errorf("%w: root_ca_certificate: %v", ErrTLSConfig, err)
	}
	certPEM, err := tlsMaterial(cfg, "connect_certificate", def.ConnectCertificate, def.ConnectCertificatePEM)
	if err != nil {
		return nil, err
	}
	keyPEM, err := tlsMaterial(cfg, "connect_private_key", def.ConnectPrivateKey, def.ConnectPrivateKeyPEM)
	if err != nil {
		return nil, err
	}
	if certPEM != nil || keyPEM != nil {
		for rest := certPEM; ; {
			var blk *pem.Block
			if blk, rest = pem.Decode(rest); blk == nil {
				break
			}
			if blk.Type == "CERTIFICATE" {
				c.Certificate = append(c.Certificate, blk.Bytes)
			}
		}
		if len(c.Certificate) == 0 {
			return nil, fmt.Errorf("%w: no certificate in connect_certificate", ErrTLSConfig)
		}
		if c.Key, err = tls13.ParsePrivateKeyPEM(keyPEM); err != nil {
			return nil, fmt.Errorf("%w: connect_private_key: %v", ErrTLSConfig, err)
		}
	} else if tlsBool(cfg, "enable_mtls", def.EnableMTLS) {
		return nil, fmt.Errorf("%w: enable_mtls needs connect_certificate and connect_private_key", ErrTLSConfig)
	}
	return c, nil
}

// TLS13Client runs a TLS 1.3 handshake (package tls13) over conn, an
// established connection to a tls/ locator, e.g. from a zenoh.Config.Dial
// on a board with its own network stack.
func TLS13Client(conn io.ReadWriteCloser, locator string, def *TLSConfig) (transport.Link, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	cfg, err := tls13Config(l, def)
	if err != nil {
		return nil, err
	}
	return tls13.Client(conn, cfg)
}

// DialTLS13 dials a tls/ locator with package tls13, the TLS of TinyGo
// builds (see package link/tls13link). In regular Go builds it is an
// alternative to crypto/tls, the default.
func DialTLS13(locator string, timeout time.Duration, def *TLSConfig) (transport.Link, error) {
	l, err := Parse(locator)
	if err != nil {
		return nil, err
	}
	return dialTLS13(l, timeout, def)
}

func dialTLS13(l Locator, timeout time.Duration, def *TLSConfig) (transport.Link, error) {
	cfg, err := tls13Config(l, def)
	if err != nil {
		return nil, err
	}
	nc, err := net.DialTimeout("tcp", l.Address, timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
	}
	if timeout > 0 {
		_ = nc.SetDeadline(time.Now().Add(timeout))
	}
	c, err := tls13.Client(nc, cfg)
	if err != nil {
		nc.Close()
		return nil, err
	}
	_ = nc.SetDeadline(time.Time{})
	return c, nil
}

// tls13CommonName: the peer's name on a tls13 link, found through an
// interface so that it doesn't link package tls13 in.
func tls13CommonName(l any) string {
	if c, ok := l.(interface{ PeerCommonName() string }); ok {
		return c.PeerCommonName()
	}
	return ""
}
