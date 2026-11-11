// Package shm implements zenoh's POSIX shared-memory transport optimization,
// wire- and memory-compatible with zenoh-rust 1.x (commons/zenoh-shm, SHM
// protocol version 2).
//
// Two nodes on the same host that both enable it negotiate SHM in the
// INIT/OPEN handshake (each proves it can read the other's "auth" segment).
// Large payloads are then written once into a shared data segment and only a
// small reference (metadata segment id, header index, generation) crosses the
// link; the receiver copies the bytes out of shared memory and releases the
// reference.
//
// Memory layout (all in /dev/shm/<id>.zenoh, little-endian, see the Rust
// sources for the authoritative definitions):
//
//   - metadata segment (u16 id): 32768 chunk headers of 32 bytes
//     (refcount u32, watchdog_invalidated bool, generation u32, protocol u32,
//     segment u32, chunk offset u32, len u64), followed by 32768 u64
//     watchdog words (bit i%64 of word i/64 belongs to header i);
//   - data segment (u32 id): raw bytes, a chunk is a byte offset;
//   - auth segment (u32 id): challenge, version, protocol list and 2810 u32
//     "handoff" counters.
//
// Every holder of a buffer sets its watchdog bit every 50 ms; the owner
// clears the bits every 100 ms and invalidates buffers nobody confirmed, so a
// crashed reader cannot leak memory. The owner frees chunks whose refcount is
// zero (or that were invalidated).
//
// The package is available on Linux with the standard Go toolchain; elsewhere
// (and under TinyGo) New returns ErrUnsupported.
package shm

import (
	"errors"

	"github.com/shabunin/gozenoh/protocol"
	"github.com/shabunin/gozenoh/transport"
)

// ErrUnsupported is returned by New where shared memory is not implemented.
var ErrUnsupported = errors.New("shm: shared memory is not supported on this platform")

// Errors reading a shared-memory reference.
var (
	ErrInvalidBuffer = errors.New("shm: buffer reference is invalid or was reclaimed")
	ErrNoMemory      = errors.New("shm: shared-memory pool exhausted")
)

// Options configures a Manager. Zero fields take zenoh-rust's defaults.
type Options struct {
	// PoolSize is the size in bytes of the data segment payloads are copied
	// into for sending. Default 16 MiB.
	PoolSize int
	// Threshold is the smallest payload sent through shared memory. Default
	// 3 KiB.
	Threshold int
}

func (o *Options) setDefaults() {
	if o.PoolSize <= 0 {
		o.PoolSize = 16 << 20
	}
	if o.Threshold <= 0 {
		o.Threshold = 3 << 10
	}
}

// ExtID is the INIT/OPEN extension id zenoh uses for SHM negotiation.
const ExtID = 0x2

// Wrapped is a payload placed in shared memory for one link. Call Sent after
// the message carrying Parts has been handed to the transport (whether or not
// that succeeded).
type Wrapped struct {
	Parts []protocol.PayloadPart
	sent  func()
}

// Sent releases the sender's own reference to the buffer.
func (w *Wrapped) Sent() {
	if w.sent != nil {
		w.sent()
		w.sent = nil
	}
}

var _ transport.HandshakeExt = (*Link)(nil)
