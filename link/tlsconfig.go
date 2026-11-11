package link

// TLSConfig configures "tls/" links, like zenoh's transport/link/tls section.
// Paths name PEM files. Each setting can also be given per locator in its
// config part with zenoh's keys, e.g.
//
//	tls/0.0.0.0:7447#listen_certificate_file=srv.pem;listen_private_key_file=srv.key
//
// (root_ca_certificate, listen_certificate, listen_private_key,
// connect_certificate, connect_private_key — each as _file, _base64 or _raw —
// plus enable_mtls and verify_name_on_connect).
type TLSConfig struct {
	RootCACertificate  string // CA to verify the other side; default: system roots
	ListenCertificate  string // server certificate chain
	ListenPrivateKey   string // server key
	ConnectCertificate string // client certificate (mutual TLS)
	ConnectPrivateKey  string // client key (mutual TLS)
	// EnableMTLS makes listeners require client certificates signed by
	// RootCACertificate, and dialers present theirs.
	EnableMTLS bool
	// SkipNameVerification accepts a server certificate whose names don't
	// match the dialed host (zenoh's verify_name_on_connect=false). The chain
	// is still verified.
	SkipNameVerification bool
}
