package zenoh

import (
	"bytes"
	"errors"
	"math"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// TestSessionHardening_ResolveAndDeclareIDReuse verifies bounds/canonicalization
// in f.resolve, that handleDeclare cannot overwrite an active declaration's key
// by reusing its ID, and that MaxDeclarations cannot be bypassed.
func TestSessionHardening_ResolveAndDeclareIDReuse(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	r, err := Open(Config{
		Mode:            ModeRouter,
		Listen:          []string{"tcp/127.0.0.1:0"},
		Scouting:        ScoutingConfig{Disable: true},
		MaxDeclarations: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()

	// Raw client transport: declare 4 entities, then attempt ID reuse overwrite
	// and limit exceedance.
	l, err := link.Dial(r.Locators()[0], 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	czid := protocol.ZID(NewZID())
	tr, err := transport.Open(l, transport.Config{ZID: czid, WhatAmI: protocol.Client},
		transport.Callbacks{OnMessage: func(protocol.Message, bool) error { return nil }})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	sendDecl := func(body protocol.Declaration) {
		t.Helper()
		if err := tr.Send(true, &protocol.Declare{QoS: protocol.QoSDeclare, Body: body}); err != nil {
			t.Fatalf("send %T: %v", body, err)
		}
	}
	sendDecl(&protocol.DeclareKeyExpr{ID: 1, WireExpr: protocol.WireExpr{Suffix: "k/one"}})
	sendDecl(&protocol.DeclareSubscriber{ID: 1, WireExpr: protocol.WireExpr{Suffix: "s/one"}})
	sendDecl(&protocol.DeclareQueryable{ID: 1, WireExpr: protocol.WireExpr{Suffix: "q/one"}})
	sendDecl(&protocol.DeclareToken{ID: 1, WireExpr: protocol.WireExpr{Suffix: "t/one"}})

	waitFor(t, "initial declarations recorded", func() bool {
		f := r.faceByZID(czid)
		if f == nil {
			return false
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		return f.remoteExprs[1] == "k/one" && f.subs[1] == "s/one" && f.qbls[1].key == "q/one" && f.tokens[1] == "t/one"
	})

	// Reusing IDs with different keys must NOT overwrite existing active keys.
	sendDecl(&protocol.DeclareKeyExpr{ID: 1, WireExpr: protocol.WireExpr{Suffix: "k/two"}})
	sendDecl(&protocol.DeclareSubscriber{ID: 1, WireExpr: protocol.WireExpr{Suffix: "s/two"}})
	sendDecl(&protocol.DeclareQueryable{ID: 1, WireExpr: protocol.WireExpr{Suffix: "q/two"}})
	sendDecl(&protocol.DeclareToken{ID: 1, WireExpr: protocol.WireExpr{Suffix: "t/two"}})
	// Also test key expr ID overwrite even on a declaration with a different slot.
	sendDecl(&protocol.UndeclareKeyExpr{ID: 1})
	// Re-adding one back stays at 4 (<= MaxDeclarations).
	sendDecl(&protocol.DeclareKeyExpr{ID: 1, WireExpr: protocol.WireExpr{Suffix: "k/one"}})

	waitFor(t, "original declarations intact", func() bool {
		f := r.faceByZID(czid)
		if f == nil {
			return false
		}
		r.mu.Lock()
		defer r.mu.Unlock()
		return f.live && f.remoteExprs[1] == "k/one" && f.subs[1] == "s/one" &&
			f.qbls[1].key == "q/one" && f.tokens[1] == "t/one"
	})
	// Re-check overwrite guard specifically for DeclareKeyExpr before undeclare:
	sendDecl(&protocol.DeclareKeyExpr{ID: 1, WireExpr: protocol.WireExpr{Suffix: "k/hijack"}})
	settle()
	f := r.faceByZID(czid)
	r.mu.Lock()
	ke1 := f.remoteExprs[1]
	r.mu.Unlock()
	if ke1 != "k/one" {
		t.Fatalf("remoteExprs[1] overwritten to %q, want %q", ke1, "k/one")
	}

	// Exceeding MaxDeclarations (5th declaration) closes the offending face.
	sendDecl(&protocol.DeclareSubscriber{ID: 2, WireExpr: protocol.WireExpr{Suffix: "s/extra"}})
	waitFor(t, "face closed after exceeding MaxDeclarations", func() bool {
		return r.faceByZID(czid) == nil
	})
}

// TestSessionHardening_PublisherWrapAndErrorCleanup verifies DeclarePublisher
// skips 0 and active localExprs IDs on uint16 wrap, and undeclares on error/close.
func TestSessionHardening_PublisherWrapAndErrorCleanup(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	// Occupy ID 1 explicitly so wrap must skip 0 AND 1 and land on 2.
	s.mu.Lock()
	s.localExprs[1] = "already/used"
	s.nextExpr = math.MaxUint16 - 1
	s.mu.Unlock()

	p1, err := s.DeclarePublisher("pub/wrap1")
	if err != nil {
		t.Fatal(err)
	}
	p2, err := s.DeclarePublisher("pub/wrap2")
	if err != nil {
		t.Fatal(err)
	}
	if p1.exprID != math.MaxUint16 {
		t.Fatalf("p1.exprID = %d, want %d", p1.exprID, math.MaxUint16)
	}
	if p2.exprID == 0 || p2.exprID == 1 {
		t.Fatalf("p2.exprID = %d, must skip 0 and already-used 1", p2.exprID)
	}
	_ = p1.Undeclare()
	_ = p2.Undeclare()

	s.mu.Lock()
	_, p1Left := s.localExprs[p1.exprID]
	_, p2Left := s.localExprs[p2.exprID]
	delete(s.localExprs, 1)
	s.mu.Unlock()
	if p1Left || p2Left {
		t.Fatalf("localExprs leaked after undeclare: p1=%v p2=%v", p1Left, p2Left)
	}

	// Undeclaring twice is safe and DeclarePublisher on closed session returns ErrSessionClosed cleanly.
	if err := p2.Undeclare(); err != nil {
		t.Fatalf("double Undeclare: %v", err)
	}
	_ = s.Close()
	if _, err := s.DeclarePublisher("pub/afterclose"); err == nil {
		t.Fatal("DeclarePublisher on closed session succeeded")
	}
}

// TestSessionHardening_RouterRequestRaceAndInterestCancel exercises concurrent
// routerRequest/routeDone completion under -race and verifies answerInterest does
// not respond on a dead face or cancelled InterestFinal.
func TestSessionHardening_RouterRequestRaceAndInterestCancel(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	r := openRouter(t)
	c := clientOf(t, r)
	if _, err := r.DeclareQueryable("rr/**", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), []byte("fast"))
	}); err != nil {
		t.Fatal(err)
	}
	settle()

	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 15 {
				ch, err := c.Get("rr/x", WithTimeout(5*time.Millisecond))
				if err != nil {
					continue
				}
				for range ch {
				}
			}
		}()
	}
	wg.Wait()

	// InterestCurrentFuture immediately cancelled with InterestFinal or dead face.
	r2 := openRouter(t)
	l, err := link.Dial(r2.Locators()[0], 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	decls := make(chan *protocol.Declare, 16)
	czid := protocol.ZID(NewZID())
	tr, err := transport.Open(l, transport.Config{ZID: czid, WhatAmI: protocol.Client},
		transport.Callbacks{OnMessage: func(m protocol.Message, _ bool) error {
			if d, ok := m.(*protocol.Declare); ok {
				decls <- d
			}
			return nil
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer tr.Close()

	// Direct answerInterest check on a non-live face must emit nothing.
	f := &face{s: r2, live: false, router: newRouterState()}
	r2.answerInterest(f, &protocol.Interest{ID: 77, Mode: protocol.InterestCurrentFuture},
		&remoteInterest{mode: protocol.InterestCurrentFuture, opts: protocol.InterestSubscribers, key: "ic/**"}, true)
	f.outMu.Lock()
	qlen := len(f.outQ)
	f.outMu.Unlock()
	if qlen != 0 {
		t.Fatalf("answerInterest wrote %d messages on non-live face", qlen)
	}

	// Register a subscriber on r2, then send CurrentFuture + immediate Final on interest 88.
	sub, err := r2.DeclareSubscriber("ic/a", func(Sample) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	waitFor(t, "raw client face live", func() bool { return r2.faceByZID(czid) != nil })

	// First cancel immediately in the same loop / back-to-back.
	if err := tr.Send(true, &protocol.Interest{
		ID:       88,
		Mode:     protocol.InterestCurrentFuture,
		Options:  protocol.InterestSubscribers,
		WireExpr: &protocol.WireExpr{Suffix: "ic/**"},
		QoS:      protocol.QoSDeclare,
	}); err != nil {
		t.Fatal(err)
	}
	if err := tr.Send(true, &protocol.Interest{
		ID:   88,
		Mode: protocol.InterestFinal,
		QoS:  protocol.QoSDeclare,
	}); err != nil {
		t.Fatal(err)
	}
	// Trigger another matching subscriber later: must not notify cancelled interest 88.
	settle()
	// Drain any current declarations possibly queued before InterestFinal arrived.
	for len(decls) > 0 {
		<-decls
	}
	sub2, err := r2.DeclareSubscriber("ic/b", func(Sample) {})
	if err != nil {
		t.Fatal(err)
	}
	defer sub2.Undeclare()
	select {
	case d := <-decls:
		if d.HasInterestID && d.InterestID == 88 {
			t.Fatalf("cancelled interest 88 received declaration: %+v", d)
		}
	case <-time.After(150 * time.Millisecond):
	}
}

// TestSessionHardening_MeshAndLinkStateHardening verifies:
// (1) meshRoutersGoneLocked triggers liveliness SampleKindDelete when remote router drops;
// (2) static-ZID restart lower SN is accepted via seenSelf in gossipState;
// (3) mappings per link capped at maxPSMappings;
// (4) connectScouted respects maxConcurrentScoutDials.
func TestSessionHardening_MeshAndLinkStateHardening(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	// Token delete when a router drops.
	r := openRouterTo(t)
	tokEvents := make(chan Sample, 8)
	lsub, err := r.Liveliness().DeclareSubscriber("mesh/tok/**", func(s Sample) {
		select {
		case tokEvents <- s:
		default:
		}
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer lsub.Undeclare()

	n1, _ := protocol.ParseZID("e00101")
	n2, _ := protocol.ParseZID("e00102")
	u1 := fakeRouter(t, r, n1)
	u2 := fakeRouter(t, r, n2)

	sendLS := func(u *transport.Unicast, list []linkState) {
		t.Helper()
		if err := u.Send(true, &protocol.OAM{
			ID:  oamLinkState,
			Enc: protocol.ExtZBuf,
			Buf: encodeLinkStates(list),
			QoS: protocol.QoSDeclare,
		}); err != nil {
			t.Fatal(err)
		}
	}
	sendLS(u1, []linkState{
		{psid: 0, sn: 1, zid: n1, whatami: protocol.Router, hasWAI: true, links: []uint64{1}},
		{psid: 1, sn: 0, zid: r.zid, whatami: protocol.Router, hasWAI: true},
	})
	sendLS(u2, []linkState{
		{psid: 0, sn: 1, zid: n2, whatami: protocol.Router, hasWAI: true, links: []uint64{1}},
		{psid: 1, sn: 0, zid: r.zid, whatami: protocol.Router, hasWAI: true},
		{psid: 2, sn: 1, zid: n1, whatami: protocol.Router, hasWAI: true, links: []uint64{1}},
	})
	time.Sleep(4 * treesDelay)

	// Legitimate token declared by n1 (NodeID=0) -> SampleKindPut, then u1 drops -> SampleKindDelete.
	if err := u1.Send(true, &protocol.Declare{
		QoS:    protocol.QoSDeclare,
		NodeID: 0,
		Body:   &protocol.DeclareToken{WireExpr: protocol.WireExpr{Suffix: "mesh/tok/one"}},
	}); err != nil {
		t.Fatal(err)
	}
	var gotPut bool
	waitFor(t, "remote mesh token PUT event", func() bool {
		select {
		case ev := <-tokEvents:
			if ev.KeyExpr == "mesh/tok/one" && ev.Kind == SampleKindPut {
				gotPut = true
			}
			return gotPut
		default:
			return false
		}
	})
	_ = u1.Close()
	var gotDel bool
	waitFor(t, "remote mesh token DELETE event on router disconnect", func() bool {
		select {
		case ev := <-tokEvents:
			if ev.KeyExpr == "mesh/tok/one" && ev.Kind == SampleKindDelete {
				gotDel = true
			}
			return gotDel
		default:
			return false
		}
	})

	selfZ, _ := protocol.ParseZID("a00001")
	peerZ, _ := protocol.ParseZID("a00002")

	// maxPSMappings cap in lsLink.
	var overflowLS []linkState
	for i := uint64(0); i < maxPSMappings+50; i++ {
		z := protocol.ZID(NewZID())
		overflowLS = append(overflowLS, linkState{psid: i + 100, sn: 1, zid: z, whatami: protocol.Router, hasWAI: true})
	}
	netwRouter := newNetwork(selfZ)
	rFace := &face{zid: peerZ, whatami: protocol.Router}
	netwRouter.addLink(rFace)
	netwRouter.linkStates(overflowLS, rFace)
	if len(netwRouter.links[rFace].mappings) > maxPSMappings {
		t.Fatalf("lsLink mappings = %d > maxPSMappings (%d)", len(netwRouter.links[rFace].mappings), maxPSMappings)
	}

	// Static-ZID restart & maxPSMappings cap in gossipOAM (with direct gLink state).
	gPeer, err := Open(Config{
		Mode:           ModePeer,
		Listen:         []string{"tcp/127.0.0.1:0"},
		Scouting:       ScoutingConfig{Disable: true},
		ConnectTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer gPeer.Close()
	gPeer.mu.Lock()
	if gPeer.gossip == nil {
		gPeer.gossip = newGossip(gPeer.zid, protocol.Peer)
	}
	gf := &face{s: gPeer, zid: peerZ, whatami: protocol.Router, live: true}
	gPeer.gossip.addNode(&gNode{zid: peerZ, whatami: protocol.Router, sn: 15})
	gl := &gLink{f: gf, south: true, mappings: map[uint64]string{}}
	gPeer.gossip.links = append(gPeer.gossip.links, gl)
	gPeer.mu.Unlock()

	gPeer.gossipOAM(gf, &protocol.OAM{
		ID:  oamLinkState,
		Enc: protocol.ExtZBuf,
		Buf: encodeLinkStates([]linkState{{psid: 0, sn: 2, zid: peerZ, whatami: protocol.Router, hasWAI: true, hasLoc: true, locators: []string{"tcp/127.0.0.1:8888"}}}),
	})
	gPeer.gossipOAM(gf, &protocol.OAM{
		ID:  oamLinkState,
		Enc: protocol.ExtZBuf,
		Buf: encodeLinkStates(overflowLS),
	})
	gPeer.mu.Lock()
	gotGNode := gPeer.gossip.nodes[gPeer.gossip.idx(peerZ)]
	gotGSN := gotGNode.sn
	gotGLoc := append([]string(nil), gotGNode.locators...)
	gfMappings := len(gl.mappings)
	for i, x := range gPeer.gossip.links {
		if x == gl {
			gPeer.gossip.links = append(gPeer.gossip.links[:i], gPeer.gossip.links[i+1:]...)
			break
		}
	}
	gPeer.mu.Unlock()
	if gotGSN != 2 || len(gotGLoc) != 1 || gotGLoc[0] != "tcp/127.0.0.1:8888" {
		t.Fatalf("static-ZID restart lower SN rejected on gossipState: sn=%d loc=%v", gotGSN, gotGLoc)
	}
	if gfMappings > maxPSMappings {
		t.Fatalf("gLink mappings = %d > maxPSMappings (%d)", gfMappings, maxPSMappings)
	}

	// (7) connectScouted respects maxConcurrentScoutDials.
	sDial, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer sDial.Close()
	sDial.mu.Lock()
	for range maxConcurrentScoutDials {
		sDial.connecting[string(NewZID().Bytes())] = true
	}
	sDial.mu.Unlock()
	extraZ := NewZID()
	var calls atomic.Int32
	sDial.connectScouted(extraZ, []string{"tcp/127.0.0.1:1"})
	sDial.mu.Lock()
	_, added := sDial.connecting[string(extraZ.Bytes())]
	sDial.connecting = map[string]bool{}
	sDial.mu.Unlock()
	if added || calls.Load() != 0 {
		t.Fatal("connectScouted exceeded maxConcurrentScoutDials")
	}
}

// TestQueryLivelinessCancelMatchingHardening verifies:
// (1) untimestamped replies are preserved in Monotonic and superseded by arrival in Latest;
// (2) Liveliness.Get InterestCurrent does not leak into f.tokens or hit MaxDeclarations;
// (3) @/ admin queries, queryables, and liveliness work when Namespace is set;
// (4) concurrent CancellationToken.Cancel + Get/Liveliness.Get and ModeClient send errors clean up handlers;
// (5) MatchingListener.notify is race-free under concurrent status updates.
func TestQueryLivelinessCancelMatchingHardening(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	// (1) ConsolidationMonotonic & ConsolidationLatest with nil timestamps + tsAccept rules.
	sCons, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer sCons.Close()
	tsLater := sCons.NewTimestamp()
	qCons, err := sCons.DeclareQueryable("consnil/**", func(q *Query) {
		switch q.KeyExpr() {
		case "consnil/mono":
			_ = q.Reply("consnil/mono", []byte("u1"))
			_ = q.Reply("consnil/mono", []byte("u2"))
			_ = q.Reply("consnil/mono", []byte("t1"), WithTimestamp(tsLater))
			_ = q.Reply("consnil/mono", []byte("u3_dropped"))
		case "consnil/latest":
			_ = q.Reply("consnil/latest", []byte("u1"))
			_ = q.Reply("consnil/latest", []byte("u2"))
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	defer qCons.Undeclare()

	collectPayloads := func(sel string, mode ConsolidationMode) string {
		t.Helper()
		ch, err := sCons.Get(sel, WithConsolidation(mode), WithTimeout(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var out []string
		for r := range ch {
			smp, err := r.Result()
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, string(smp.Payload))
		}
		return strings.Join(out, ",")
	}
	if got := collectPayloads("consnil/mono", ConsolidationMonotonic); got != "u1,u2,t1" {
		t.Fatalf("ConsolidationMonotonic nil/ts = %q, want u1,u2,t1", got)
	}
	if got := collectPayloads("consnil/latest", ConsolidationLatest); got != "u2" {
		t.Fatalf("ConsolidationLatest nil = %q, want u2", got)
	}

	// (2) Liveliness.Get InterestCurrent does NOT leak into f.tokens or exhaust MaxDeclarations.
	rLiv, err := Open(Config{
		Mode:     ModeRouter,
		Listen:   []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer rLiv.Close()
	tok1, err := rLiv.Liveliness().DeclareToken("liv/t1")
	if err != nil {
		t.Fatal(err)
	}
	defer tok1.Undeclare()
	tok2, err := rLiv.Liveliness().DeclareToken("liv/t2")
	if err != nil {
		t.Fatal(err)
	}
	defer tok2.Undeclare()

	cLiv, err := Open(Config{
		Mode:            ModeClient,
		Connect:         rLiv.Locators(),
		Scouting:        ScoutingConfig{Disable: true},
		MaxDeclarations: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer cLiv.Close()
	for i := range 5 {
		ch, err := cLiv.Liveliness().Get("liv/**", WithTimeout(2*time.Second))
		if err != nil {
			t.Fatalf("iter %d Liveliness.Get: %v", i, err)
		}
		var gotKeys []string
		for r := range ch {
			if r.Sample != nil {
				gotKeys = append(gotKeys, r.Sample.KeyExpr)
			}
		}
		if len(gotKeys) != 2 {
			t.Fatalf("iter %d got %v, want 2 tokens", i, gotKeys)
		}
	}
	cLiv.mu.Lock()
	var tokenMapEntries int
	for _, f := range cLiv.faces {
		tokenMapEntries += len(f.tokens)
	}
	cLiv.mu.Unlock()
	if tokenMapEntries != 0 {
		t.Fatalf("Liveliness.Get leaked %d entries into client face tokens", tokenMapEntries)
	}

	// (3) @/ admin Query / DeclareQueryable / Liveliness under Namespace.
	nsRouter, err := Open(Config{
		Mode:      ModeRouter,
		Listen:    []string{"tcp/127.0.0.1:0"},
		Namespace: "app/ns",
		Scouting:  ScoutingConfig{Disable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nsRouter.Close()

	nsClient, err := Open(Config{
		Mode:      ModeClient,
		Connect:   nsRouter.Locators(),
		Namespace: "app/ns",
		Scouting:  ScoutingConfig{Disable: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer nsClient.Close()

	adminQbl, err := nsRouter.DeclareQueryable("@/test/ping", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), []byte("pong"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer adminQbl.Undeclare()
	if adminQbl.KeyExpr() != "@/test/ping" {
		t.Fatalf("adminQbl.KeyExpr() = %q, want @/test/ping", adminQbl.KeyExpr())
	}

	nsAdminTokCh := make(chan Sample, 4)
	nsAdminLSub, err := nsRouter.Liveliness().DeclareSubscriber("@/tok/**", func(s Sample) {
		select {
		case nsAdminTokCh <- s:
		default:
		}
	}, false)
	if err != nil {
		t.Fatal(err)
	}
	defer nsAdminLSub.Undeclare()

	nsAdminTok, err := nsClient.Liveliness().DeclareToken("@/tok/a")
	if err != nil {
		t.Fatal(err)
	}
	defer nsAdminTok.Undeclare()

	waitFor(t, "admin @/ query reply through namespace", func() bool {
		ch, err := nsClient.Get("@/test/ping", WithTimeout(time.Second))
		if err != nil {
			return false
		}
		for r := range ch {
			if r.Sample != nil && r.Sample.KeyExpr == "@/test/ping" && string(r.Sample.Payload) == "pong" {
				return true
			}
		}
		return false
	})
	waitFor(t, "admin @/ liveliness event through namespace", func() bool {
		select {
		case s := <-nsAdminTokCh:
			return s.KeyExpr == "@/tok/a" && s.Kind == SampleKindPut
		default:
			return false
		}
	})

	// (4) Concurrent CancellationToken.Cancel + Get / Liveliness.Get race stress & ModeClient send failure cleanup.
	for range 25 {
		ct := NewCancellationToken()
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			ch, err := nsClient.Get("doesnotmatter/x", WithCancellation(ct), WithTimeout(200*time.Millisecond))
			if err == nil {
				for range ch {
				}
			} else if !errors.Is(err, ErrCancelled) {
				t.Errorf("Get err = %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			ch, err := nsClient.Liveliness().Get("liv/**", WithCancellation(ct), WithTimeout(200*time.Millisecond))
			if err == nil {
				for range ch {
				}
			} else if !errors.Is(err, ErrCancelled) {
				t.Errorf("Liveliness.Get err = %v", err)
			}
		}()
		go func() {
			defer wg.Done()
			ct.Cancel()
		}()
		wg.Wait()
	}
	nsClient.mu.Lock()
	leakedQueries := len(nsClient.queries)
	leakedLivGets := len(nsClient.livGets)
	nsClient.mu.Unlock()
	if leakedQueries != 0 || leakedLivGets != 0 {
		t.Fatalf("leaked queries=%d livGets=%d after cancellation race", leakedQueries, leakedLivGets)
	}

	// ModeClient closed router face send failure must still unbind CancellationToken handler.
	_ = nsRouter.Close()
	waitFor(t, "nsClient face closed", func() bool {
		nsClient.mu.Lock()
		defer nsClient.mu.Unlock()
		return len(nsClient.faces) == 0
	})
	ctErr := NewCancellationToken()
	_ = nsClient.GetFunc("never/sent", func(Reply) {}, func() {},
		WithCancellation(ctErr), WithAllowedDestination(LocalityRemote), WithTimeout(time.Second))
	ctErr.mu.Lock()
	remHandlers := len(ctErr.handlers)
	ctErr.mu.Unlock()
	if remHandlers != 0 {
		t.Fatalf("ctErr.handlers = %d after failed/no-dst GetFunc, want 0", remHandlers)
	}

	// (5) MatchingListener.notify concurrency race check.
	mPeer := openPeer(t)
	pubM, err := mPeer.DeclarePublisher("match/key")
	if err != nil {
		t.Fatal(err)
	}
	defer pubM.Undeclare()
	var mEvents atomic.Int32
	ml, err := pubM.DeclareMatchingListener(func(MatchingStatus) { mEvents.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	for range 10 {
		subM, err := mPeer.DeclareSubscriber("match/key", func(Sample) {})
		if err != nil {
			t.Fatal(err)
		}
		_ = subM.Undeclare()
	}
	_ = ml.Undeclare()
}

// TestAdvancedPubSubHardening verifies:
// (1) AdvancedPublisher.prepare clones cached Payload and Attachment slices;
// (2) recovery st.pending is bounded by advMaxPending.
func TestAdvancedPubSubHardening(t *testing.T) {
	t.Cleanup(func() { leaktest.Check(t) })

	// (1) Slice aliasing in AdvancedPublisher.prepare.
	s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ap, err := s.DeclareAdvancedPublisher("advslice/k", AdvancedPublisherConfig{
		Cache: &CacheConfig{MaxSamples: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Undeclare()

	payload := []byte("original-payload")
	att := []byte("original-att")
	if err := ap.Put(payload, WithAttachment(att)); err != nil {
		t.Fatal(err)
	}
	payload[0] = 'X'
	att[0] = 'Y'

	ap.mu.Lock()
	if len(ap.cache) != 1 || !bytes.Equal(ap.cache[0].Payload, []byte("original-payload")) || !bytes.Equal(ap.cache[0].Attachment, []byte("original-att")) {
		ap.mu.Unlock()
		t.Fatalf("cache aliased caller slices: payload=%q att=%q", ap.cache[0].Payload, ap.cache[0].Attachment)
	}
	ap.mu.Unlock()

	id := srcID{string(orderZID.Bytes()), 1}

	// (2) Bounded st.pending during sequenced recovery.
	rCap := newOrderRig(1, 0, &RecoveryConfig{})
	rCap.seq(1) // est. hasLast=true, last=1
	rCap.as.mu.Lock()
	for sn := uint32(3); sn <= advMaxPending+20; sn++ {
		rCap.as.handleLocked(Sample{
			Payload:    []byte(strconv.FormatUint(uint64(sn), 10)),
			SourceInfo: &SourceInfo{ZID: orderZID, EID: 1, SN: sn},
		})
	}
	pendingLen := len(rCap.as.seq[id].pending)
	rCap.as.mu.Unlock()
	if pendingLen > advMaxPending {
		t.Fatalf("recovery st.pending = %d > advMaxPending (%d)", pendingLen, advMaxPending)
	}
}
