package transport

import (
	"sync/atomic"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Traffic statistics (zenoh-rust commons/zenoh-stats, the "stats" feature):
// per link, the bytes and transport messages that crossed it and its network
// messages by direction, priority, kind and SHM use. They are kept when
// Config.Stats (MulticastConfig.Stats) is set; the session above aggregates
// them per transport and serves them as metrics.
//
// What is counted, as zenoh-rust does after the handshake:
//   - bytes: what went on the wire, length prefix included (zenoh-rust
//     counts outgoing batches before compression);
//   - transport messages: FRAMEs, FRAGMENTs, KEEP_ALIVEs, CLOSEs (and JOINs
//     on multicast), one per low-latency message;
//   - network messages: each one sent, and each one received and delivered
//     (a fragmented message counts once, duplicates not at all).

// Directions index the [2] arrays of the statistics.
const (
	Tx = 0
	Rx = 1
)

// MessageKind is a network message's kind as zenoh's statistics label it.
type MessageKind uint8

const (
	KindPut MessageKind = iota
	KindDel
	KindQuery
	KindReply
	KindReplyErr
	KindResponseFinal
	KindInterest
	KindDeclare
	KindOAM
	NumMessageKinds
)

var kindNames = [NumMessageKinds]string{"put", "delete", "query", "reply", "reply-err",
	"response-final", "interest", "declare", "oam"}

// String is the kind's metric label value.
func (k MessageKind) String() string {
	if k < NumMessageKinds {
		return kindNames[k]
	}
	return "unknown"
}

// KindOf classifies a network message; ok is false for anything else.
func KindOf(m any) (k MessageKind, ok bool) {
	switch m := m.(type) {
	case *protocol.Push:
		if _, del := m.Body.(*protocol.Del); del {
			return KindDel, true
		}
		return KindPut, true
	case *protocol.Request:
		return KindQuery, true
	case *protocol.Response:
		if _, isErr := m.Body.(*protocol.Err); isErr {
			return KindReplyErr, true
		}
		return KindReply, true
	case *protocol.ResponseFinal:
		return KindResponseFinal, true
	case *protocol.Interest:
		return KindInterest, true
	case *protocol.Declare:
		return KindDeclare, true
	case *protocol.OAM:
		return KindOAM, true
	}
	return 0, false
}

// IsShm reports whether a data message carries its payload in shared memory.
func IsShm(m any) bool {
	shm := func(parts []protocol.PayloadPart) bool {
		for _, p := range parts {
			if p.Shm != nil {
				return true
			}
		}
		return false
	}
	data := func(b protocol.DataBody) bool {
		p, ok := b.(*protocol.Put)
		return ok && shm(p.Parts)
	}
	switch m := m.(type) {
	case *protocol.Push:
		return data(m.Body)
	case *protocol.Request:
		return m.Query != nil && m.Query.Body != nil && shm(m.Query.Body.Parts)
	case *protocol.Response:
		switch b := m.Body.(type) {
		case *protocol.Reply:
			return data(b.Body)
		case *protocol.Err:
			return shm(b.Parts)
		}
	}
	return false
}

// numNet is the number of network message counters: direction, priority,
// kind, SHM use.
const numNet = 2 * 8 * int(NumMessageKinds) * 2

// NetIndex is the index of a network message counter in
// LinkCounts.NetworkMessages: direction d (Tx, Rx), priority p, kind k,
// shm 0 or 1.
func NetIndex(d int, p protocol.Priority, k MessageKind, shm int) int {
	return ((d*8+int(p&7))*int(NumMessageKinds)+int(k))*2 + shm
}

// LinkStats are the live counters of one link.
type LinkStats struct {
	bytes [2]atomic.Uint64
	tmsgs [2]atomic.Uint64
	nmsgs [numNet]atomic.Uint64 // by NetIndex
}

// LinkCounts is a snapshot of LinkStats.
type LinkCounts struct {
	Bytes             [2]uint64
	TransportMessages [2]uint64
	// NetworkMessages are indexed by NetIndex (nil: all zero).
	NetworkMessages []uint64
}

// Snapshot reads the counters (nil: all zero).
func (s *LinkStats) Snapshot() (c LinkCounts) {
	if s == nil {
		return c
	}
	for d := range 2 {
		c.Bytes[d] = s.bytes[d].Load()
		c.TransportMessages[d] = s.tmsgs[d].Load()
	}
	c.NetworkMessages = make([]uint64, numNet)
	for i := range c.NetworkMessages {
		c.NetworkMessages[i] = s.nmsgs[i].Load()
	}
	return c
}

// Add adds o to c.
func (c *LinkCounts) Add(o *LinkCounts) {
	for d := range 2 {
		c.Bytes[d] += o.Bytes[d]
		c.TransportMessages[d] += o.TransportMessages[d]
	}
	if o.NetworkMessages == nil {
		return
	}
	if c.NetworkMessages == nil {
		c.NetworkMessages = make([]uint64, numNet)
	}
	for i, v := range o.NetworkMessages {
		c.NetworkMessages[i] += v
	}
}

// Network is a network message counter (see NetIndex).
func (c *LinkCounts) Network(d int, p protocol.Priority, k MessageKind, shm int) uint64 {
	if c.NetworkMessages == nil {
		return 0
	}
	return c.NetworkMessages[NetIndex(d, p, k, shm)]
}

// NetworkTotal is the number of network messages in direction d with SHM
// use shm (-1: either).
func (c *LinkCounts) NetworkTotal(d, shm int) (n uint64) {
	if c.NetworkMessages == nil {
		return 0
	}
	half := numNet / 2
	for i, v := range c.NetworkMessages[d*half : (d+1)*half] {
		if shm < 0 || i&1 == shm {
			n += v
		}
	}
	return n
}

func (s *LinkStats) addBytes(d, n int) {
	if s != nil {
		s.bytes[d].Add(uint64(n))
	}
}

func (s *LinkStats) addTransport(d int) {
	if s != nil {
		s.tmsgs[d].Add(1)
	}
}

func (s *LinkStats) addNetwork(d int, m any) {
	if s == nil {
		return
	}
	k, ok := KindOf(m)
	if !ok {
		return
	}
	h := 0
	if IsShm(m) {
		h = 1
	}
	s.nmsgs[NetIndex(d, protocol.QoSOf(m).Priority(), k, h)].Add(1)
}
