package zenoh

import (
	"errors"
	"sync/atomic"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/shm"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Shared memory (Config.SharedMemory): payloads to and from same-host nodes
// that negotiated SHM travel through /dev/shm instead of the link: large
// ones copied into the session's pool, and buffers the application wrote
// itself (ShmProvider, WithShmBuf) by reference. Received SHM payloads are
// copied out at once, so the rest of the session (and router forwarding)
// only sees ordinary payloads.

// ShmProvider returns the session's shared-memory pool (nil without
// Config.SharedMemory), to allocate buffers to publish with WithShmBuf:
//
//	b, err := s.ShmProvider().Alloc(len(frame))
//	copy(b.Bytes(), frame) // or render into b.Bytes() directly
//	err = s.Put("camera/0", nil, zenoh.WithShmBuf(b))
//	b.Close()
func (s *Session) ShmProvider() *shm.Manager { return s.shm }

var errNoShm = errors.New("zenoh: shared-memory payload on a link without SHM")

// shmStats counts payloads moved through shared memory (for tests):
// copied into it, referenced without a copy (WithShmBuf), read from it.
var shmStats struct{ wrapped, zeroCopy, resolved atomic.Int64 }

// shmLink returns the face's negotiated SHM state, if any.
func shmLink(tr *transport.Unicast) *shm.Link {
	if l, ok := tr.Ext().(*shm.Link); ok && l.Active() {
		return l
	}
	return nil
}

func (f *face) resolveParts(parts []protocol.PayloadPart, qos protocol.QoS) ([]byte, error) {
	if f.shm == nil {
		var out []byte
		for _, p := range parts {
			if p.Shm != nil {
				return nil, errNoShm
			}
			out = append(out, p.Raw...)
		}
		return out, nil
	}
	shmStats.resolved.Add(1)
	return f.shm.Resolve(parts, qos.Priority())
}

// unshm replaces shared-memory payload parts of a received message with the
// bytes they reference. An error means the message must be dropped.
func (f *face) unshm(m protocol.Message) error {
	resolvePut := func(p *protocol.Put, qos protocol.QoS) error {
		if p.Parts == nil {
			return nil
		}
		b, err := f.resolveParts(p.Parts, qos)
		p.Payload, p.Parts = b, nil
		return err
	}
	switch m := m.(type) {
	case *protocol.Push:
		if p, ok := m.Body.(*protocol.Put); ok {
			return resolvePut(p, m.QoS)
		}
	case *protocol.Request:
		if q := m.Query; q != nil && q.Body != nil && q.Body.Parts != nil {
			b, err := f.resolveParts(q.Body.Parts, m.QoS)
			q.Body.Payload, q.Body.Parts = b, nil
			return err
		}
	case *protocol.Response:
		switch b := m.Body.(type) {
		case *protocol.Reply:
			if p, ok := b.Body.(*protocol.Put); ok {
				return resolvePut(p, m.QoS)
			}
		case *protocol.Err:
			if b.Parts != nil {
				pl, err := f.resolveParts(b.Parts, m.QoS)
				b.Payload, b.Parts = pl, nil
				return err
			}
		}
	}
	return nil
}

// toShm returns msgs with large payloads moved to shared memory for this
// face, and a function to call once they were handed to the transport.
// Messages are copied, never modified: the same message may go to several
// faces.
func (f *face) toShm(msgs []transport.Encoder) ([]transport.Encoder, func()) {
	var wrapped []*shm.Wrapped
	threshold := f.s.shm.Threshold()
	wrap := func(payload []byte, qos protocol.QoS, src any) []protocol.PayloadPart {
		if b, ok := src.(*shm.Buf); ok && b != nil {
			if w, err := f.shm.WrapBuf(b, qos.Priority()); err == nil {
				shmStats.zeroCopy.Add(1)
				return w.Parts
			}
		}
		if len(payload) < threshold {
			return nil
		}
		w, err := f.shm.Wrap(payload, qos.Priority())
		if err != nil {
			return nil // e.g. pool exhausted: send it inline
		}
		wrapped = append(wrapped, w)
		shmStats.wrapped.Add(1)
		return w.Parts
	}
	var out []transport.Encoder
	for i, e := range msgs {
		var repl transport.Encoder
		switch m := e.(type) {
		case *protocol.Push:
			if p, ok := m.Body.(*protocol.Put); ok && p.Parts == nil {
				if parts := wrap(p.Payload, m.QoS, p.ShmBuf); parts != nil {
					pc, mc := *p, *m
					pc.Payload, pc.Parts = nil, parts
					mc.Body = &pc
					repl = &mc
				}
			}
		case *protocol.Request:
			if q := m.Query; q != nil && q.Body != nil && q.Body.Parts == nil {
				if parts := wrap(q.Body.Payload, m.QoS, nil); parts != nil {
					bc, qc, mc := *q.Body, *q, *m
					bc.Payload, bc.Parts = nil, parts
					qc.Body = &bc
					mc.Query = &qc
					repl = &mc
				}
			}
		case *protocol.Response:
			switch b := m.Body.(type) {
			case *protocol.Reply:
				if p, ok := b.Body.(*protocol.Put); ok && p.Parts == nil {
					if parts := wrap(p.Payload, m.QoS, p.ShmBuf); parts != nil {
						pc, bc, mc := *p, *b, *m
						pc.Payload, pc.Parts = nil, parts
						bc.Body = &pc
						mc.Body = &bc
						repl = &mc
					}
				}
			case *protocol.Err:
				if b.Parts == nil {
					if parts := wrap(b.Payload, m.QoS, nil); parts != nil {
						bc, mc := *b, *m
						bc.Payload, bc.Parts = nil, parts
						mc.Body = &bc
						repl = &mc
					}
				}
			}
		}
		if repl != nil {
			if out == nil {
				out = append([]transport.Encoder(nil), msgs...)
			}
			out[i] = repl
		}
	}
	if out == nil {
		return msgs, func() {}
	}
	return out, func() {
		for _, w := range wrapped {
			w.Sent()
		}
	}
}
