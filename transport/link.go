// Package transport implements the Zenoh unicast transport: the INIT/OPEN
// handshake, stream batch framing, FRAME batching and sequence numbers,
// FRAGMENT splitting and reassembly, keep-alives and lease expiry.
//
// It is link-agnostic: anything that is an io.ReadWriteCloser can carry a
// session (a net.Conn, a TLS conn, a TinyGo netdev socket, a serial port with
// its own framing...). The package imports no "net" so it stays usable on
// TinyGo targets; dialing lives in package link.
package transport

import (
	"errors"
	"io"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Link is a connection between two Zenoh nodes: by default a reliable,
// ordered byte stream on which batches are length-prefixed. A link that
// implements PacketLink carries one batch per packet instead.
//
// If the Link also implements SetDeadline(time.Time) error (as net.Conn does),
// the transport uses it to bound the handshake.
type Link interface {
	io.ReadWriteCloser
}

// PacketLink is a Link that preserves message boundaries, like UDP: each
// Write sends one whole batch and each Read returns one whole batch, without
// the 2-byte length prefix. Delivery may be lossy and unordered; the
// transport's sequence numbers drop duplicates and stale batches.
type PacketLink interface {
	Link
	// MTU is the largest packet the link carries; it caps the batch size.
	MTU() int
}

// PriorityLink is a stream link that may carry each message priority on a
// stream of its own (zenoh-rust's supports_priorities: QUIC multistream),
// so that a congested priority doesn't hold up the others. The Link's own
// Read and Write are the Control priority's stream, which also carries the
// handshake and keep-alives; with QoS, batches of priority p go on stream p,
// without QoS everything stays on Control.
type PriorityLink interface {
	Link
	// PriorityStreams reports whether per-priority streams are in use.
	PriorityStreams() bool
	// PriorityWriter and PriorityReader return the stream of priority p
	// (1 to 7; 0 is the link itself).
	PriorityWriter(p protocol.Priority) io.Writer
	PriorityReader(p protocol.Priority) io.Reader
}

// MixedReliabilityLink is a reliable link that may come with a best-effort
// companion on the same connection (zenoh-rust's NewLink::MixedReliability:
// QUIC streams plus QUIC datagrams). After the handshake on the link, the
// transport uses the companion as a second, unreliable link with no
// handshake of its own, for best-effort traffic.
type MixedReliabilityLink interface {
	Link
	// BestEffort returns the companion, or nil if there is none.
	BestEffort() PacketLink
}

// ErrBatchLength is returned by ReadBatch when an incoming batch is larger than
// the receive buffer.
var ErrBatchLength = errors.New("transport: incoming batch larger than buffer")

// ReadBatch reads one stream-framed batch (2-byte little-endian length prefix,
// then that many bytes) into buf and returns the filled sub-slice.
func ReadBatch(r io.Reader, buf []byte) ([]byte, error) {
	if len(buf) < 2 {
		return nil, ErrBatchLength
	}
	if _, err := io.ReadFull(r, buf[:2]); err != nil { // the length, read into buf: no allocation
		return nil, err
	}
	n := int(buf[0]) | int(buf[1])<<8
	if n > len(buf) {
		return nil, ErrBatchLength
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return nil, err
	}
	return buf[:n], nil
}
