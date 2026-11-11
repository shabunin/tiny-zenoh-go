package transport

import (
	"fmt"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/protocol"
)

// rxState is the receive side of one sender: a sequence-number window and
// a fragment buffer per channel (priority x reliability). A unicast
// transport has one; a multicast transport has one per remote node.
type rxState struct {
	snMask  uint64
	qos     bool
	maxMsg  int
	sn      [numPrio][2]uint64 // last accepted SN ([0] best-effort, [1] reliable)
	frag    [numPrio][2]fragState
	deliver func(m protocol.Message, reliable bool) error
}

func (r *rxState) init(mask uint64, qos bool, maxMsg int, deliver func(protocol.Message, bool) error) {
	r.snMask, r.qos, r.maxMsg, r.deliver = mask, qos, maxMsg, deliver
}

// sync makes next the next expected SN on every channel: as zenoh-rust,
// behave as if the SN before it was seen.
func (r *rxState) sync(next uint64) {
	last := (next - 1) & r.snMask
	for p := range r.sn {
		r.sn[p] = [2]uint64{last, last}
	}
}

// syncChannel does the same for one priority.
func (r *rxState) syncChannel(p protocol.Priority, next protocol.PrioritySN) {
	r.sn[p] = [2]uint64{(next.BestEffort - 1) & r.snMask, (next.Reliable - 1) & r.snMask}
}

// frame handles a FRAME at r: its network messages run until the next
// transport header or the end of the batch. Messages of a duplicate or stale
// frame are decoded (to find where the frame ends) but not delivered.
func (r *rxState) frame(rd *codec.Reader) error {
	fh, err := protocol.DecodeFrameHeader(rd)
	if err != nil {
		return err
	}
	prio, err := r.priority(fh.Exts)
	if err != nil {
		return err
	}
	accept, err := r.acceptSN(prio, fh.Reliable, fh.SN)
	if err != nil {
		return err
	}
	for rd.Remaining() > 0 {
		if h, _ := rd.PeekByte(); protocol.IsTransportID(h) {
			break
		}
		m, err := protocol.DecodeNetwork(rd)
		if err != nil {
			return err
		}
		if accept {
			if err := r.deliver(m, fh.Reliable); err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *rxState) fragment(f *protocol.Fragment) error {
	prio, err := r.priority(f.Exts)
	if err != nil {
		return err
	}
	accept, err := r.acceptSN(prio, f.Reliable, f.SN)
	if err != nil || !accept {
		return err
	}
	return r.reassemble(prio, f)
}

// priority finds the channel of a FRAME or FRAGMENT from its QoS extension.
// Without QoS only the default priority is valid, as in zenoh-rust.
func (r *rxState) priority(exts []protocol.Extension) (protocol.Priority, error) {
	prio := protocol.PriorityData
	if e := findExt(exts, extQoS); e != nil {
		prio = protocol.Priority(e.U64 & 0x07)
	}
	if !r.qos && prio != protocol.PriorityData {
		return 0, fmt.Errorf("transport: priority %d on a transport without QoS", prio)
	}
	return prio, nil
}

// acceptSN applies zenoh-rust's receive window (SeqNum::roll): an SN outside
// the resolution is a protocol error; otherwise it is accepted iff it is ahead
// of the last accepted SN by 1..mask/2. Duplicates and stale SNs are dropped.
func (r *rxState) acceptSN(prio protocol.Priority, reliable bool, sn uint64) (bool, error) {
	if sn&^r.snMask != 0 {
		return false, fmt.Errorf("transport: SN %#x outside resolution", sn)
	}
	i := 0
	if reliable {
		i = 1
	}
	last := &r.sn[prio][i]
	gap := (sn - *last) & r.snMask
	if gap == 0 || gap > r.snMask>>1 {
		return false, nil
	}
	*last = sn
	return true, nil
}

func (r *rxState) reassemble(prio protocol.Priority, f *protocol.Fragment) error {
	i := 0
	if f.Reliable {
		i = 1
	}
	s := &r.frag[prio][i]
	if s.active && f.SN != s.nextSN {
		// A fragment went missing: the partial message is unrecoverable.
		s.buf, s.skip = s.buf[:0], true
	}
	if findExt(f.Exts, extFragFirst) != nil {
		s.buf, s.skip = s.buf[:0], false // explicit start (zenoh "patch" peers)
	}
	if findExt(f.Exts, extFragDrop) != nil {
		s.buf, s.active, s.skip = s.buf[:0], false, false // sender gave up on it
		return nil
	}
	s.active = true
	s.nextSN = (f.SN + 1) & r.snMask
	if !s.skip {
		if len(s.buf)+len(f.Payload) > r.maxMsg {
			s.buf, s.skip = s.buf[:0], true
		} else {
			s.buf = append(s.buf, f.Payload...)
		}
	}
	if f.More {
		return nil
	}
	msg, complete := s.buf, !s.skip
	s.buf, s.active, s.skip = s.buf[:0], false, false
	if !complete {
		return nil
	}
	rd := codec.NewReader(msg)
	for rd.Remaining() > 0 {
		m, err := protocol.DecodeNetwork(rd)
		if err != nil {
			return err
		}
		if err := r.deliver(m, f.Reliable); err != nil {
			return err
		}
	}
	return nil
}

// FRAGMENT extensions sent by "patched" zenoh nodes (multicast JOIN patch 1).
const (
	extFragFirst = 0x2
	extFragDrop  = 0x3
)
