package zenoh

import (
	"sync"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// A gateway that has a gateway of its own (a peer serving clients while
// attached to a router) must know what lies beyond its gateway to answer its
// south faces' INTERESTs: a client's publisher only sends data once a
// matching subscriber is declared to it. Once it has a south face, it asks
// its gateways about every entity with one INTEREST (current and future);
// their declarations are kept as the gateway faces' entities and reach the
// south faces like any other. A south face's current interest is answered
// once the gateways have answered (or after a timeout).

// upstreamTimeout bounds how long a south face's current interest waits for
// our gateways (zenoh-rust routing/interests/timeout).
const upstreamTimeout = 10 * time.Second

type upstreamInterest struct {
	id      uint32
	pending map[*face]bool // gateways that have not answered yet
	waiters []func()
}

func (u *upstreamInterest) msg() *protocol.Interest {
	return &protocol.Interest{ID: u.id, Mode: protocol.InterestCurrentFuture,
		Options: protocol.InterestKeyExprs | protocol.InterestSubscribers |
			protocol.InterestQueryables | protocol.InterestTokens}
}

// needUpstreamLocked reports whether the upstream interest must be started
// (we have a south face and are not a router: routers have no gateway).
func (s *Session) needUpstreamLocked() bool {
	return s.upstream == nil && s.cfg.Mode == ModePeer
}

// startUpstream sends the upstream interest to our gateways.
func (s *Session) startUpstream() {
	s.mu.Lock()
	if s.upstream != nil {
		s.mu.Unlock()
		return
	}
	s.nextInterest++
	u := &upstreamInterest{id: s.nextInterest, pending: map[*face]bool{}}
	s.upstream = u
	routed := s.routedFacesLocked()
	for _, f := range routed {
		u.pending[f] = true
	}
	s.mu.Unlock()
	for _, f := range routed {
		if err := f.send(true, u.msg()); err != nil {
			s.upstreamDone(f)
		}
	}
}

// upstreamSnapshotLocked is what a new gateway face gets: the interest.
func (s *Session) upstreamSnapshotLocked(f *face) []transport.Encoder {
	if s.upstream == nil {
		return nil
	}
	s.upstream.pending[f] = true
	return []transport.Encoder{s.upstream.msg()}
}

// upstreamDone records that gateway f answered (or is gone).
func (s *Session) upstreamDone(f *face) {
	s.mu.Lock()
	u := s.upstream
	if u == nil || !u.pending[f] {
		s.mu.Unlock()
		return
	}
	delete(u.pending, f)
	var run []func()
	if len(u.pending) == 0 {
		run, u.waiters = u.waiters, nil
	}
	s.mu.Unlock()
	for _, w := range run {
		w()
	}
}

// afterUpstream runs answer once our gateways have answered the upstream
// interest (at once if there is nothing to wait for).
func (s *Session) afterUpstream(answer func()) {
	s.mu.Lock()
	u := s.upstream
	if u == nil || len(u.pending) == 0 {
		s.mu.Unlock()
		answer()
		return
	}
	var once sync.Once
	run := func() { once.Do(answer) }
	u.waiters = append(u.waiters, run)
	s.mu.Unlock()
	time.AfterFunc(upstreamTimeout, run)
}
