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

// ErrBatchLength is returned by ReadBatch when an incoming batch is larger than
// the receive buffer.
var ErrBatchLength = errors.New("transport: incoming batch larger than buffer")

// ReadBatch reads one stream-framed batch (2-byte little-endian length prefix,
// then that many bytes) into buf and returns the filled sub-slice.
func ReadBatch(r io.Reader, buf []byte) ([]byte, error) {
	var l [2]byte
	if _, err := io.ReadFull(r, l[:]); err != nil {
		return nil, err
	}
	n := int(l[0]) | int(l[1])<<8
	if n > len(buf) {
		return nil, ErrBatchLength
	}
	if _, err := io.ReadFull(r, buf[:n]); err != nil {
		return nil, err
	}
	return buf[:n], nil
}
