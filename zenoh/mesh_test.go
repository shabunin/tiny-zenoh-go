package zenoh

import (
	"errors"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Router mesh tests: Go routers linked with each other and with zenohd.
// The zenohd variants need ZENOH_MESH="tcp/host:port[,tcp/host:port]" naming
// one or two zenohd routers that are not connected to each other.

func meshRouters(t *testing.T, n int) []string {
	t.Helper()
	v := os.Getenv("ZENOH_MESH")
	locs := strings.Split(v, ",")
	if v == "" || len(locs) < n {
		t.Skipf("set ZENOH_MESH to %d zenohd locator(s)", n)
	}
	return locs[:n]
}

func openRouterTo(t *testing.T, connect ...string) *Session {
	t.Helper()
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Connect: connect, Scouting: ScoutingConfig{Disable: true}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = r.Close() })
	return r
}

func clientAt(t *testing.T, loc string) *Session {
	t.Helper()
	s, err := Open(Config{Connect: []string{loc}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func collect(ch <-chan Reply, err error) ([]Reply, error) {
	if err != nil {
		return nil, err
	}
	var out []Reply
	for r := range ch {
		out = append(out, r)
	}
	return out, nil
}

// checkMesh exercises pub/sub, queries and liveliness between a and b,
// sessions attached to different routers.
func checkMesh(t *testing.T, a, b *Session) {
	t.Helper()
	pfx := uniq(t)

	sink := newSink()
	sub, err := b.DeclareSubscriber(pfx+"/data/**", sink.handler)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "data across the mesh", func() bool {
		_ = a.Put(pfx+"/data/x", []byte("hello"))
		select {
		case <-sink.ch:
			return true
		case <-time.After(100 * time.Millisecond):
			return false
		}
	})
	_ = sub.Undeclare()

	q, err := b.DeclareQueryable(pfx+"/q/**", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), []byte("answer from b"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	waitFor(t, "query across the mesh", func() bool {
		replies, err := collect(a.Get(pfx+"/q/one", WithTimeout(time.Second)))
		return err == nil && len(replies) == 1 && replies[0].Err == nil &&
			string(replies[0].Sample.Payload) == "answer from b"
	})

	tok, err := b.Liveliness().DeclareToken(pfx + "/alive")
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, "liveliness across the mesh", func() bool {
		replies, err := collect(a.Liveliness().Get(pfx+"/**", WithTimeout(time.Second)))
		return err == nil && len(replies) == 1 && replies[0].Sample.KeyExpr == pfx+"/alive"
	})
	events := newSink()
	ls, err := a.Liveliness().DeclareSubscriber(pfx+"/**", events.handler, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Undeclare()
	settle() // the subscriber's interest and the undeclaration take different links
	_ = tok.Undeclare()
	if e := events.wait(t, 1)[0]; e.Kind != SampleKindDelete || e.KeyExpr != pfx+"/alive" {
		t.Errorf("liveliness event = %+v", e)
	}
}

func TestGoRouterMesh(t *testing.T) {
	r1 := openRouter(t)
	r2 := openRouterTo(t, r1.Locators()...)
	waitFor(t, "routers linked", func() bool { return len(r1.RoutersZID()) == 1 && len(r2.RoutersZID()) == 1 })
	checkMesh(t, clientOf(t, r1), clientOf(t, r2))
	checkMesh(t, clientOf(t, r2), clientOf(t, r1))
	checkMesh(t, r1, r2) // the routers' own sessions
}

// TestGoRouterMeshChain: r1 - r2 - r3, so r2 forwards on behalf of others.
func TestGoRouterMeshChain(t *testing.T) {
	r1 := openRouter(t)
	r2 := openRouterTo(t, r1.Locators()...)
	r3 := openRouterTo(t, r2.Locators()...)
	waitFor(t, "routers linked", func() bool { return len(r2.RoutersZID()) == 2 })
	checkMesh(t, clientOf(t, r1), clientOf(t, r3))
	checkMesh(t, clientOf(t, r3), clientOf(t, r1))
}

func TestGoRouterMeshZenohd(t *testing.T) {
	zd := meshRouters(t, 1)[0]
	r := openRouterTo(t, zd)
	waitFor(t, "linked to zenohd", func() bool { return len(r.RoutersZID()) == 1 })
	checkMesh(t, clientAt(t, zd), clientOf(t, r))
	checkMesh(t, clientOf(t, r), clientAt(t, zd))
}

// TestGoRouterBridgesZenohd: zenohd - go router - zenohd2.
func TestGoRouterBridgesZenohd(t *testing.T) {
	zds := meshRouters(t, 2)
	r := openRouterTo(t, zds...)
	waitFor(t, "linked to both", func() bool { return len(r.RoutersZID()) == 2 })
	checkMesh(t, clientAt(t, zds[0]), clientAt(t, zds[1]))
	checkMesh(t, clientAt(t, zds[1]), clientAt(t, zds[0]))
	checkMesh(t, clientAt(t, zds[0]), clientOf(t, r))
}

// TestMeshRejoinHold: right after the link to a neighbour router went down,
// a new transport from that router is refused for meshRejoinHold (zenoh-rust
// would not see it as a new child of its trees and never send it the
// declarations lost with the link); clients and peers are not held.
func TestMeshRejoinHold(t *testing.T) {
	r := openRouterTo(t)
	z, _ := protocol.ParseZID("e00042")
	r.mu.Lock()
	r.meshGone[string(z.Bytes())] = time.Now()
	r.mu.Unlock()
	if err := r.admit(newFace(r), z, protocol.Router); !errors.Is(err, errRejoinHold) {
		t.Fatalf("router just gone: %v, want errRejoinHold", err)
	}
	c, _ := protocol.ParseZID("e00043")
	r.mu.Lock()
	r.meshGone[string(c.Bytes())] = time.Now()
	r.mu.Unlock()
	// Admitted faces have no transport: unregister them for Close.
	forget := func(z ZID) {
		r.mu.Lock()
		delete(r.faces, string(z.Bytes()))
		r.mu.Unlock()
	}
	if err := r.admit(newFace(r), c, protocol.Client); err != nil {
		t.Fatalf("client: %v", err)
	}
	forget(c)
	r.mu.Lock()
	r.meshGone[string(z.Bytes())] = time.Now().Add(-meshRejoinHold)
	r.mu.Unlock()
	if err := r.admit(newFace(r), z, protocol.Router); err != nil {
		t.Fatalf("after the hold: %v", err)
	}
	forget(z)
}
