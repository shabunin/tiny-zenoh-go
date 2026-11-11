package transport

import (
	"fmt"

	"github.com/shabunin/gozenoh/protocol"
)

// QoS negotiation (zenoh-rust io/zenoh-transport/src/unicast/establishment/
// ext/qos.rs). INIT carries extension 0x1 either as a unit ("QoS on") or as
// a Z64 ("QoSLink") that may also restrict the link to a priority range
// and/or one reliability:
//
//	bits 2:0   0b001 plain QoS, or bit 1 = range present, bit 2 = reliability present
//	bits 10:3  first priority     bits 18:11  last priority
//	bit  19    reliable
//
// FRAME and FRAGMENT then carry the priority in their own mandatory Z64
// extension 0x1, omitted for the default priority (Data).

const (
	extQoS  = 0x1
	numPrio = 8
)

func (t *Unicast) setTxSN(isn uint64) {
	for p := range t.txSN {
		t.txSN[p] = [2]uint64{isn, isn}
	}
}

// negotiateQoS reads the peer's QoS extension (INIT SYN or ACK). We never
// restrict our own end, so the peer's restrictions, if any, are the result:
// a priority range and a reliability tell link selection what the handshake
// link is for (see selectLink).
func (t *Unicast) negotiateQoS(exts []protocol.Extension) error {
	l := t.hs
	var unit, link *protocol.Extension
	for i := range exts {
		if exts[i].ID != extQoS {
			continue
		}
		switch exts[i].Enc {
		case protocol.ExtUnit:
			unit = &exts[i]
		case protocol.ExtZ64:
			link = &exts[i]
		}
	}
	switch {
	case unit != nil && link != nil:
		return fmt.Errorf("%w: peer sent both QoS and QoSLink", ErrHandshake)
	case unit != nil:
		t.qos = true
	case link != nil:
		v := link.U64
		switch {
		case v == 0:
		case v == 0b001:
			t.qos = true
		case v&0b110 != 0:
			t.qos = true
			if v&0b010 != 0 {
				lo, hi := protocol.Priority(v>>3), protocol.Priority(v>>11)
				if lo > hi || hi >= numPrio {
					return fmt.Errorf("%w: bad QoS priority range %d..%d", ErrHandshake, lo, hi)
				}
				l.hasRange, l.prioLo, l.prioHi = true, lo, hi
			}
			if v&0b100 != 0 {
				l.reliable = v>>19&1 == 1
			}
			l.qosLink = v
		default:
			return fmt.Errorf("%w: invalid QoS extension %#x", ErrHandshake, v)
		}
	}
	return nil
}

// qosExt is the responder's answer: the agreed state, i.e. the peer's.
func (l *tlink) qosExt() protocol.Extension {
	if l.qosLink != 0 {
		return protocol.Extension{ID: extQoS, Enc: protocol.ExtZ64, U64: l.qosLink}
	}
	return protocol.Extension{ID: extQoS, Enc: protocol.ExtUnit}
}

// QoS reports whether the QoS extension is in use (per-priority channels).
func (t *Unicast) QoS() bool { return t.qos }
