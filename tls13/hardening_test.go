package tls13

import (
	"crypto/tls"
	"crypto/x509"
	"testing"
	"time"
)

func mustParseTLSCert(t *testing.T, der []byte) *Certificate {
	t.Helper()
	c, err := ParseCertificate(der)
	if err != nil {
		t.Fatalf("ParseCertificate: %v", err)
	}
	return c
}

func TestVerifyChainHardening(t *testing.T) {
	now := time.Now()
	if !clockSet(now) {
		now = time.Date(2025, 6, 1, 0, 0, 0, 0, time.UTC)
	}

	rootKey := genKey(t, "p256")
	rootIssued := issue(t, rootKey, nil, caTmpl("root"))
	rootCert := mustParseTLSCert(t, rootIssued.cert.Raw)

	// (a) A pinned non-CA leaf in RootCAs authenticates itself, but cannot act as a CA to sign another cert.
	pinnedLeafKey := genKey(t, "p256")
	pinnedLeafTmpl := leafTmpl("pinned.example.org", x509.ExtKeyUsageServerAuth)
	pinnedLeafTmpl.IsCA = false
	pinnedLeafTmpl.BasicConstraintsValid = true
	pinnedLeafTmpl.KeyUsage = x509.KeyUsageDigitalSignature
	pinnedIssued := issue(t, pinnedLeafKey, &rootIssued, pinnedLeafTmpl)
	pinnedCert := mustParseTLSCert(t, pinnedIssued.cert.Raw)

	if err := verifyChain([]*Certificate{pinnedCert}, []*Certificate{pinnedCert}, now, oidEKUServer); err != nil {
		t.Fatalf("pinned leaf direct match rejected: %v", err)
	}

	forgedKey := genKey(t, "p256")
	forgedIssued := issue(t, forgedKey, &pinnedIssued, leafTmpl("forged.example.org", x509.ExtKeyUsageServerAuth))
	forgedCert := mustParseTLSCert(t, forgedIssued.cert.Raw)

	if err := verifyChain([]*Certificate{forgedCert}, []*Certificate{pinnedCert}, now, oidEKUServer); err == nil {
		t.Fatal("non-CA pinned cert in RootCAs was accepted as an issuer")
	}
	if err := verifyChain([]*Certificate{forgedCert, pinnedCert}, []*Certificate{pinnedCert}, now, oidEKUServer); err == nil {
		t.Fatal("non-CA intermediate cert was accepted as an issuer")
	}

	// (b) pathLenConstraint: 0 on an intermediate CA permits signing a leaf directly,
	// but rejects any nested sub-CA beneath it.
	inter0Tmpl := caTmpl("inter-path0")
	inter0Tmpl.MaxPathLen = 0
	inter0Tmpl.MaxPathLenZero = true
	inter0Key := genKey(t, "p256")
	inter0Issued := issue(t, inter0Key, &rootIssued, inter0Tmpl)
	inter0Cert := mustParseTLSCert(t, inter0Issued.cert.Raw)

	directLeafKey := genKey(t, "p256")
	directLeafIssued := issue(t, directLeafKey, &inter0Issued, leafTmpl("direct.example.org", x509.ExtKeyUsageServerAuth))
	directLeafCert := mustParseTLSCert(t, directLeafIssued.cert.Raw)

	if err := verifyChain([]*Certificate{directLeafCert, inter0Cert}, []*Certificate{rootCert}, now, oidEKUServer); err != nil {
		t.Fatalf("pathLen=0 intermediate rejected for direct leaf: %v", err)
	}

	subCAKey := genKey(t, "p256")
	subCAIssued := issue(t, subCAKey, &inter0Issued, caTmpl("sub-ca"))
	subCACert := mustParseTLSCert(t, subCAIssued.cert.Raw)

	nestedLeafKey := genKey(t, "p256")
	nestedLeafIssued := issue(t, nestedLeafKey, &subCAIssued, leafTmpl("nested.example.org", x509.ExtKeyUsageServerAuth))
	nestedLeafCert := mustParseTLSCert(t, nestedLeafIssued.cert.Raw)

	if err := verifyChain([]*Certificate{nestedLeafCert, subCACert, inter0Cert}, []*Certificate{rootCert}, now, oidEKUServer); err == nil {
		t.Fatal("pathLen=0 intermediate CA allowed a nested sub-CA")
	}

	// pathLenConstraint: 0 on a root CA rejects an intermediate CA, while allowing a direct leaf.
	root0Tmpl := caTmpl("root-path0")
	root0Tmpl.MaxPathLen = 0
	root0Tmpl.MaxPathLenZero = true
	root0Key := genKey(t, "p256")
	root0Issued := issue(t, root0Key, nil, root0Tmpl)
	root0Cert := mustParseTLSCert(t, root0Issued.cert.Raw)

	root0InterKey := genKey(t, "p256")
	root0InterIssued := issue(t, root0InterKey, &root0Issued, caTmpl("root0-inter"))
	root0InterCert := mustParseTLSCert(t, root0InterIssued.cert.Raw)

	root0SubLeafKey := genKey(t, "p256")
	root0SubLeafIssued := issue(t, root0SubLeafKey, &root0InterIssued, leafTmpl("sub.example.org", x509.ExtKeyUsageServerAuth))
	root0SubLeafCert := mustParseTLSCert(t, root0SubLeafIssued.cert.Raw)

	if err := verifyChain([]*Certificate{root0SubLeafCert, root0InterCert}, []*Certificate{root0Cert}, now, oidEKUServer); err == nil {
		t.Fatal("pathLen=0 root CA allowed an intermediate CA")
	}

	root0DirectKey := genKey(t, "p256")
	root0DirectIssued := issue(t, root0DirectKey, &root0Issued, leafTmpl("root0-direct.example.org", x509.ExtKeyUsageServerAuth))
	root0DirectCert := mustParseTLSCert(t, root0DirectIssued.cert.Raw)
	if err := verifyChain([]*Certificate{root0DirectCert}, []*Certificate{root0Cert}, now, oidEKUServer); err != nil {
		t.Fatalf("pathLen=0 root CA rejected direct leaf: %v", err)
	}

	// Missing keyCertSign on a CA must be rejected.
	noCertSignTmpl := caTmpl("no-certsign")
	noCertSignTmpl.KeyUsage = x509.KeyUsageDigitalSignature
	noCertSignKey := genKey(t, "p256")
	noCertSignIssued := issue(t, noCertSignKey, &rootIssued, noCertSignTmpl)
	noCertSignCert := mustParseTLSCert(t, noCertSignIssued.cert.Raw)
	noCertSignLeaf := issue(t, genKey(t, "p256"), &noCertSignIssued, leafTmpl("badca.example.org", x509.ExtKeyUsageServerAuth))
	if err := verifyChain([]*Certificate{mustParseTLSCert(t, noCertSignLeaf.cert.Raw), noCertSignCert}, []*Certificate{rootCert}, now, oidEKUServer); err == nil {
		t.Fatal("CA without keyCertSign was accepted")
	}
}

func TestHostnameAndEmptyServerNameHardening(t *testing.T) {
	c := &Certificate{
		DNSNames: []string{
			"*.com",
			"*.*.example.org",
			"foo*.example.org",
			"*.example.com",
		},
	}

	for _, bad := range []string{"", "*", "*.", "sub.*.example.org", "*.example.com", "foo.com", "foo.bar.example.org", "foox.example.org", "a.b.example.com"} {
		if err := c.VerifyHostname(bad); err == nil {
			t.Fatalf("VerifyHostname(%q) succeeded, want rejection", bad)
		}
	}
	if err := c.VerifyHostname("sub.example.com"); err != nil {
		t.Fatalf("VerifyHostname(valid) failed: %v", err)
	}

	// (c) Empty ServerName fails during handshake unless SkipNameVerification is set.
	cert, ca := serverCert(t, "p256")
	addr, errs := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	if conn, err := dial(t, addr, &Config{RootCAs: roots(t, ca)}); err == nil {
		_ = conn.Close()
		t.Fatal("handshake succeeded with empty ServerName and SkipNameVerification=false")
	}
	<-errs

	addr2, errs2 := echoServer(t, &tls.Config{Certificates: []tls.Certificate{cert}})
	conn, err := dial(t, addr2, &Config{RootCAs: roots(t, ca), SkipNameVerification: true})
	if err != nil {
		t.Fatalf("SkipNameVerification=true with empty ServerName failed: %v", err)
	}
	echo(t, conn, 64)
	if err := <-errs2; err != nil {
		t.Fatal(err)
	}
}
