// Package quic adds "quic/" locators to package link, interoperating with
// zenoh-rust's QUIC links (io/zenoh-links/zenoh-link-quic and
// zenoh-link-quic_datagram). Import it for its side effect:
//
//	import _ "github.com/shabunin/gozenoh/link/quic"
//
// The variant is chosen through ALPN, from the locator's metadata, as in
// zenoh-rust:
//
//   - single stream ("zenoh", legacy "hq-29"): one bidirectional stream
//     with length-prefixed batches, as on TCP;
//   - multistream ("zenoh-ms", "?multistream=1", proposed by default
//     alongside single stream): the bidirectional stream carries the
//     Control priority, and each other priority has a unidirectional stream
//     each way, so with QoS a congested priority doesn't block the others;
//   - mixed reliability ("zenoh-mr", "zenoh-ms-mr", "?mixed_rel=1" or
//     "auto"; off by default): best-effort traffic goes in QUIC datagrams on
//     the same connection, as a second, unreliable link of the transport;
//   - datagrams only ("?rel=0"): every batch is one QUIC datagram (the
//     handshake too), like UDP but encrypted.
//
// TLS settings are the same as for "tls/" locators (link.TLSConfig or the
// locator's "#..." keys); a listener needs a certificate.
//
// zenoh-rust's "reliable UDP" (udp/...?rel=1) is QUIC with packet protection
// turned off inside quinn; quic-go cannot do that, so it is not supported.
//
// It uses github.com/quic-go/quic-go (pure Go) and is not available with
// TinyGo.
package quic
