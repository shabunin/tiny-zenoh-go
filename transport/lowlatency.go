package transport

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
)

// LowLatency transport (zenoh-rust io/zenoh-transport/src/unicast/
// lowlatency). Both nodes ask for it with the unit INIT extension 0x5. After
// the handshake every message travels alone: a network message encoded
// bare (no FRAME, no sequence numbers, no fragmentation, no batching) or a
// KEEP_ALIVE / CLOSE. On stream links each one is preceded by its length as
// a u32 LE; on packet links it is the whole packet. A message must fit in
// the negotiated batch size. zenoh-rust refuses QoS together with it, and it
// carries one link only.

const (
	extLowLatency = 0x5
	llPrefix      = 4 // stream length prefix, u32 LE
)

func llExt() protocol.Extension {
	return protocol.Extension{ID: extLowLatency, Enc: protocol.ExtUnit}
}

// LowLatency reports whether the transport runs in low-latency mode.
func (t *Unicast) LowLatency() bool { return t.lowLatency }

// enableLowLatency switches l's framing after the handshake.
func (l *tlink) enableLowLatency() {
	l.lowLatency = true
	if l.prefix > 0 {
		l.prefix = llPrefix
	}
}

// readLowLatency reads one u32-length-prefixed message from a stream.
func (l *tlink) readLowLatency() ([]byte, error) {
	var b [llPrefix]byte
	if _, err := io.ReadFull(l.link, b[:]); err != nil {
		return nil, err
	}
	n := binary.LittleEndian.Uint32(b[:])
	if int64(n) > int64(len(l.rxBuf)) {
		return nil, ErrBatchLength
	}
	if _, err := io.ReadFull(l.link, l.rxBuf[:n]); err != nil {
		return nil, err
	}
	return l.rxBuf[:n], nil
}

// sendLowLatencyLocked sends each message on its own.
func (t *Unicast) sendLowLatencyLocked(l *tlink, msgs []Encoder) error {
	for _, m := range msgs {
		t.startBatchLocked(l)
		m.Encode(&t.txBatch)
		if err := t.flushBatchLocked(l); err != nil {
			return err
		}
	}
	return nil
}

// handleLowLatency processes one received low-latency packet.
func (t *Unicast) handleLowLatency(b []byte) error {
	r := codec.NewReader(b)
	for r.Remaining() > 0 {
		hdr, _ := r.PeekByte()
		switch codec.MsgID(hdr) {
		case protocol.IDKeepAlive:
			if _, err := protocol.DecodeTransport(r); err != nil {
				return err
			}
		case protocol.IDClose:
			m, err := protocol.DecodeTransport(r)
			if err != nil {
				return err
			}
			return fmt.Errorf("%w (reason %#x)", ErrPeerClosed, m.(*protocol.Close).Reason)
		default:
			m, err := protocol.DecodeNetwork(r)
			if err != nil {
				return err
			}
			if err := t.deliver(m, true); err != nil {
				return err
			}
		}
	}
	return nil
}
