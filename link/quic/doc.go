// Package quic adds "quic/" locators to package link, interoperating with
// zenoh-rust's QUIC link (io/zenoh-links/zenoh-link-quic). Import it for its
// side effect:
//
//	import _ "github.com/shabunin/gozenoh/link/quic"
//
// The connection carries one bidirectional stream with length-prefixed
// batches, as on TCP (zenoh's base "zenoh" ALPN protocol, or the legacy
// "hq-29"). TLS settings are the same as for "tls/" locators
// (link.TLSConfig or the locator's "#..." keys); a listener needs a
// certificate. zenoh's optional multi-stream and mixed-reliability QUIC
// variants are negotiated away through ALPN.
//
// It uses github.com/quic-go/quic-go (pure Go) and is not available with
// TinyGo.
package quic
