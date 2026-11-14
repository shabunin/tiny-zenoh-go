package zenoh

import (
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// FuzzRouterMessages: a router receives arbitrary (well-formed) network
// messages from a client, peer or router it just accepted: unknown key
// expression ids, undeclares of what was never declared, replies to
// queries never sent, link-state from nowhere... It must neither panic nor
// stop serving. A real client with a subscriber, a queryable and liveliness
// entities is attached so that routing has somewhere to go.

var (
	fuzzOnce        sync.Once
	fuzzRouterS     *Session
	fuzzClientS     *Session
	fuzzPeerS       *Session
	fuzzZIDs        atomic.Uint32
	fuzzFailedSetup error
)

// fuzzEntities gives s local entities of every kind.
func fuzzEntities(s *Session) error {
	if _, err := s.DeclareSubscriber("**", func(Sample) {}); err != nil {
		return err
	}
	if _, err := s.DeclareQueryable("**", func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte("r")) }); err != nil {
		return err
	}
	if _, err := s.Liveliness().DeclareToken("fz/token"); err != nil {
		return err
	}
	if _, err := s.Liveliness().DeclareSubscriber("**", func(Sample) {}, false); err != nil {
		return err
	}
	if _, err := s.DeclarePublisher("fz/pub"); err != nil {
		return err
	}
	_, err := s.DeclareQuerier("fz/**")
	return err
}

// fuzzSessions are a router with a client attached, and a peer; both with
// entities of every kind.
func fuzzSessions(t testing.TB) (router, peer *Session) {
	fuzzOnce.Do(func() {
		r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
			Scouting: ScoutingConfig{Disable: true}})
		if err != nil {
			fuzzFailedSetup = err
			return
		}
		c, err := Open(Config{Mode: ModeClient, Connect: r.Locators()})
		if err == nil {
			fuzzClientS = c
			err = fuzzEntities(c)
		}
		if err != nil {
			fuzzFailedSetup = err
			return
		}
		p, err := Open(Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"},
			Scouting: ScoutingConfig{Disable: true}})
		if err == nil {
			err = fuzzEntities(p)
		}
		if err != nil {
			fuzzFailedSetup = err
			return
		}
		fuzzRouterS, fuzzPeerS = r, p
	})
	if fuzzFailedSetup != nil {
		t.Fatal(fuzzFailedSetup)
	}
	return fuzzRouterS, fuzzPeerS
}

func fuzzSeeds() [][]byte {
	enc := func(ms ...protocol.Message) []byte {
		var w codec.Writer
		for _, m := range ms {
			m.Encode(&w)
		}
		return w.Bytes()
	}
	we := protocol.WireExpr{Suffix: "fz/a"}
	return [][]byte{
		enc(&protocol.Push{WireExpr: we, Body: &protocol.Put{Payload: []byte("v")}}),
		enc(&protocol.Declare{Body: &protocol.DeclareKeyExpr{ID: 1, WireExpr: we}},
			&protocol.Push{WireExpr: protocol.WireExpr{Scope: 1, Suffix: "/b", Mapping: true}, Body: &protocol.Del{}},
			&protocol.Declare{Body: &protocol.UndeclareKeyExpr{ID: 1}}),
		enc(&protocol.Declare{Body: &protocol.DeclareSubscriber{ID: 2, WireExpr: protocol.WireExpr{Suffix: "fz/**"}}},
			&protocol.Declare{Body: &protocol.DeclareQueryable{ID: 3, WireExpr: we, Complete: true}},
			&protocol.Declare{Body: &protocol.DeclareToken{ID: 4, WireExpr: we}},
			&protocol.Declare{Body: &protocol.UndeclareSubscriber{ID: 2}},
			&protocol.Declare{Body: &protocol.UndeclareToken{ID: 4}}),
		enc(&protocol.Request{ID: 7, WireExpr: protocol.WireExpr{Suffix: "fz/**"}, Target: protocol.TargetAll,
			Query: &protocol.Query{Parameters: "a=1;_time=[now(-1h)..]"}}),
		enc(&protocol.Response{RequestID: 9, WireExpr: we, Body: &protocol.Reply{Body: &protocol.Put{Payload: []byte("x")}}},
			&protocol.ResponseFinal{RequestID: 9}),
		enc(&protocol.Interest{ID: 5, Mode: protocol.InterestCurrentFuture,
			Options:  protocol.InterestKeyExprs | protocol.InterestSubscribers | protocol.InterestQueryables | protocol.InterestTokens,
			WireExpr: &protocol.WireExpr{Suffix: "**"}},
			&protocol.Interest{ID: 5, Mode: protocol.InterestFinal}),
		enc(&protocol.OAM{ID: 1, Enc: protocol.ExtZBuf, Buf: []byte{0x01, 0x10, 1, 2, 3, 4}}),
		enc(&protocol.Request{ID: 8, WireExpr: protocol.WireExpr{Suffix: "@/**"}, Query: &protocol.Query{}}),
	}
}

func FuzzRouterMessages(f *testing.F) {
	f.Cleanup(func() {
		for _, s := range []*Session{fuzzClientS, fuzzPeerS, fuzzRouterS} {
			if s != nil {
				_ = s.Close()
			}
		}
		fuzzOnce, fuzzRouterS, fuzzClientS, fuzzPeerS, fuzzFailedSetup = sync.Once{}, nil, nil, nil, nil
	})
	for _, s := range fuzzSeeds() {
		for w := range byte(5) {
			f.Add(s, w)
		}
	}
	f.Fuzz(func(t *testing.T, data []byte, who byte) {
		router, peer := fuzzSessions(t)
		var msgs []transport.Encoder
		rd := codec.NewReader(data)
		for rd.Remaining() > 0 && len(msgs) < 64 {
			m, err := protocol.DecodeNetwork(rd)
			if err != nil {
				break
			}
			msgs = append(msgs, m)
		}
		if len(msgs) == 0 {
			return
		}
		n := fuzzZIDs.Add(1)
		zid, _ := protocol.NewZID([]byte{0xf, byte(n >> 24), byte(n >> 16), byte(n >> 8), byte(n)})
		c1, c2 := net.Pipe()
		type res struct {
			u   *transport.Unicast
			err error
		}
		ch := make(chan res, 1)
		var whatami protocol.WhatAmI
		switch who % 5 {
		case 0, 1, 2: // to the router, as a client, peer or router
			whatami = []protocol.WhatAmI{protocol.Client, protocol.Peer, protocol.Router}[who%5]
			go router.accept(c2, "tcp")
		case 3: // to the peer, as a peer
			whatami = protocol.Peer
			go peer.accept(c2, "tcp")
		case 4: // to a new client, as its router
			whatami = protocol.Router
			go func() {
				dialed := false
				s, err := Open(Config{Mode: ModeClient, Connect: []string{"tcp/127.0.0.1:1"},
					Dial: func(string, time.Duration) (transport.Link, error) {
						if dialed {
							return nil, net.ErrClosed
						}
						dialed = true
						return c2, nil
					}})
				if err != nil {
					return
				}
				_ = fuzzEntities(s)
				time.AfterFunc(50*time.Millisecond, func() { _ = s.Close() })
			}()
		}
		go func() {
			var u *transport.Unicast
			var err error
			// Short waits keep a slow iteration short (closing waits a
			// quarter of the lease for queued data).
			cfg := transport.Config{ZID: zid, WhatAmI: whatami, Lease: time.Second,
				TxQueue: transport.TxQueue{WaitBeforeClose: 500 * time.Millisecond}}
			if who%5 == 4 {
				u, err = transport.Accept(c1, cfg, transport.Callbacks{})
			} else {
				u, err = transport.Open(c1, cfg, transport.Callbacks{})
			}
			ch <- res{u, err}
		}()
		var u *transport.Unicast
		select {
		case x := <-ch:
			if x.err != nil {
				t.Fatalf("router refused a %v: %v", whatami, x.err)
			}
			u = x.u
		case <-time.After(5 * time.Second):
			t.Fatal("router stuck: no handshake in 5 s")
		}
		_ = u.Send(true, msgs...)
		_ = u.Close()
	})
}
