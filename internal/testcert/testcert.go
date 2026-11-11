// Package testcert makes throwaway TLS certificates for tests.
package testcert

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"time"
)

// Files are PEM file paths.
type Files struct {
	CA, ServerCert, ServerKey, ClientCert, ClientKey string
}

// Write creates a CA, a server certificate for localhost/127.0.0.1 and a
// client certificate, all signed by the CA, as PEM files in dir.
func Write(dir string) (Files, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return Files{}, err
	}
	now := time.Now()
	caTmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "tiny-zenoh-go test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	if err != nil {
		return Files{}, err
	}
	ca, _ := x509.ParseCertificate(caDER)
	leaf := func(serial int64, name string, usage x509.ExtKeyUsage) ([]byte, []byte, error) {
		k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		if err != nil {
			return nil, nil, err
		}
		tmpl := &x509.Certificate{
			SerialNumber: big.NewInt(serial), Subject: pkix.Name{CommonName: name},
			NotBefore: now.Add(-time.Hour), NotAfter: now.Add(24 * time.Hour),
			KeyUsage:    x509.KeyUsageDigitalSignature,
			ExtKeyUsage: []x509.ExtKeyUsage{usage},
			DNSNames:    []string{"localhost"},
			IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		}
		der, err := x509.CreateCertificate(rand.Reader, tmpl, ca, &k.PublicKey, caKey)
		if err != nil {
			return nil, nil, err
		}
		kb, err := x509.MarshalPKCS8PrivateKey(k)
		if err != nil {
			return nil, nil, err
		}
		return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
			pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}), nil
	}
	sc, sk, err := leaf(2, "localhost", x509.ExtKeyUsageServerAuth)
	if err != nil {
		return Files{}, err
	}
	cc, ck, err := leaf(3, "client", x509.ExtKeyUsageClientAuth)
	if err != nil {
		return Files{}, err
	}
	f := Files{
		CA: filepath.Join(dir, "ca.pem"), ServerCert: filepath.Join(dir, "server.pem"),
		ServerKey: filepath.Join(dir, "server.key"), ClientCert: filepath.Join(dir, "client.pem"),
		ClientKey: filepath.Join(dir, "client.key"),
	}
	for path, data := range map[string][]byte{
		f.CA:         pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		f.ServerCert: sc, f.ServerKey: sk, f.ClientCert: cc, f.ClientKey: ck,
	} {
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return Files{}, err
		}
	}
	return f, nil
}
