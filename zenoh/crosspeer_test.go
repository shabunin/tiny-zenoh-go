package zenoh

import (
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"
)

// Peer-mode cross tests: gozenoh peers against Rust Zenoh peers, directly
// (no router). Need ZENOH_PY (see crossimpl_test.go).

func freeLocator(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return "tcp/" + ln.Addr().String()
}

// rustPeerConf builds a Rust peer config: listening on listen (if set),
// connecting to connect (if set), scouting off.
func rustPeerConf(listen, connect string) string {
	c := map[string]any{
		"mode":                       "peer",
		"scouting/multicast/enabled": false,
		"listen/endpoints":           []string{},
	}
	if listen != "" {
		c["listen/endpoints"] = []string{listen}
	}
	if connect != "" {
		c["connect/endpoints"] = []string{connect}
	}
	b, _ := json.Marshal(c)
	return string(b)
}

func TestRustPeerListensGoConnects(t *testing.T) {
	loc := freeLocator(t)
	pfx := uniq(t)
	rs := startRust(t, rustPeerConf(loc, ""), true, "sub", pfx+"/**", "2")
	s := openPeer(t, loc)
	waitFor(t, "rust peer face", func() bool { return len(s.PeersZID()) == 1 })
	// The Rust subscriber was pushed to us on connect: wait until we know it,
	// since peers only send data to nodes that declared interest in it.
	waitFor(t, "rust subscriber declared", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, f := range s.faces {
			if len(f.subs) > 0 {
				return true
			}
		}
		return false
	})
	_ = s.Put(pfx+"/a", []byte("go-put"), WithEncoding(EncodingTextPlain.WithSchema("v=1")), WithAttachment([]byte("x")))
	pub, _ := s.DeclarePublisher(pfx + "/b")
	_ = pub.Put([]byte("go-pub"))
	e := rs.next()
	if e.Key != pfx+"/a" || e.Payload != "go-put" || e.Encoding != "text/plain;v=1" || e.Attachment == nil {
		t.Errorf("rust got %+v", e)
	}
	if e = rs.next(); e.Key != pfx+"/b" || e.Payload != "go-pub" {
		t.Errorf("rust got %+v", e)
	}
}

func TestGoPeerListensRustConnects(t *testing.T) {
	pfx := uniq(t)
	s := openPeer(t)
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx, sink.handler)
	defer sub.Undeclare()
	startRust(t, rustPeerConf("", s.Locators()[0]), false, "pub", pfx)
	got := sink.wait(t, 2)
	if string(got[0].Payload) != "from-rust" || got[0].Encoding.String() != "text/plain;x" ||
		string(got[0].Attachment) != "att" || got[1].Kind != SampleKindDelete {
		t.Errorf("go got %+v", got)
	}
	if z := s.PeersZID(); len(z) != 1 {
		t.Errorf("peers = %v", z)
	}
}

func TestGoPeerGetRustPeerQueryable(t *testing.T) {
	loc := freeLocator(t)
	pfx := uniq(t)
	startRust(t, rustPeerConf(loc, ""), true, "queryable", pfx+"/**", "5")
	s := openPeer(t, loc)
	waitFor(t, "rust queryable declared", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, f := range s.faces {
			if len(f.qbls) > 0 {
				return true
			}
		}
		return false
	})
	replies, _ := s.Get(pfx+"/q?a=1", WithPayload([]byte("hi"), EncodingTextPlain), WithTimeout(5*time.Second))
	var got []string
	for r := range replies {
		smp, err := r.Result()
		if err != nil {
			t.Fatalf("reply error: %v", err)
		}
		got = append(got, smp.KeyExpr+"="+string(smp.Payload))
	}
	if len(got) != 1 || got[0] != pfx+"/q=py:a=1:hi" {
		t.Errorf("replies = %v", got)
	}
}

func TestRustPeerGetGoPeerQueryable(t *testing.T) {
	pfx := uniq(t)
	s := openPeer(t)
	q, _ := s.DeclareQueryable(pfx+"/**", func(q *Query) {
		_ = q.Reply(pfx+"/x", []byte("go:"+string(q.Payload())))
	})
	defer q.Undeclare()
	rs := startRust(t, rustPeerConf("", s.Locators()[0]), false, "get", pfx+"/**")
	got := map[string]string{}
	for {
		e := rs.next()
		if e.Done {
			break
		}
		if e.Ok == nil {
			t.Fatalf("rust got error reply: %+v", e)
		}
		got[e.Ok.Key] = e.Ok.Payload
	}
	if got[pfx+"/x"] != "go:qbody" || len(got) != 1 {
		t.Errorf("rust replies = %v", got)
	}
}

func TestRustPeerLivelinessCross(t *testing.T) {
	loc := freeLocator(t)
	pfx := uniq(t)
	startRust(t, rustPeerConf(loc, ""), true, "token", pfx+"/rust", "5")
	s := openPeer(t, loc)
	waitFor(t, "rust token", func() bool {
		replies, _ := s.Liveliness().Get(pfx+"/**", WithTimeout(time.Second))
		n := 0
		for range replies {
			n++
		}
		return n == 1
	})
	tok, _ := s.Liveliness().DeclareToken(pfx + "/go")
	defer tok.Undeclare()
	rs := startRust(t, rustPeerConf("", s.Locators()[0]), true, "livsub", pfx+"/go", "1")
	if e := rs.next(); e.Key != pfx+"/go" || e.Kind != "PUT" {
		t.Errorf("rust liveliness = %+v", e)
	}
}

// TestRustPeerScoutsGoPeer: a Rust peer finds a Go peer by multicast scouting
// and the other way round (on a private multicast port).
func TestRustPeerScoutsGoPeer(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}
	addr := fmt.Sprintf("224.0.0.224:%d", 18000+time.Now().Nanosecond()%1000)
	pfx := uniq(t)
	s, err := Open(Config{Mode: ModePeer, Scouting: ScoutingConfig{Address: addr}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	sink := newSink()
	sub, _ := s.DeclareSubscriber(pfx+"/**", sink.handler)
	defer sub.Undeclare()
	conf, _ := json.Marshal(map[string]any{
		"mode":                       "peer",
		"scouting/multicast/enabled": true,
		"scouting/multicast/address": addr,
		"listen/endpoints":           []string{"tcp/127.0.0.1:0"},
	})
	rs := startRust(t, string(conf), true, "sub", pfx+"/rust", "1")
	waitFor(t, "scouted rust peer", func() bool { return len(s.PeersZID()) == 1 })
	waitFor(t, "rust subscriber", func() bool {
		s.mu.Lock()
		defer s.mu.Unlock()
		for _, f := range s.faces {
			if len(f.subs) > 0 {
				return true
			}
		}
		return false
	})
	_ = s.Put(pfx+"/rust", []byte("hello-scouted"))
	if e := rs.next(); e.Payload != "hello-scouted" {
		t.Errorf("rust got %+v", e)
	}
}
