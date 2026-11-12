package zenoh

import (
	"slices"
	"sync"
	"testing"
	"time"
)

// TestLivelinessOwnToken: a session's liveliness subscribers see its own
// tokens come and go exactly once, as zenoh-rust's do: a client through its
// router's echo, a peer by itself (whether attached to a router or not).
// tzd once told a client its own new token was gone, and a peer saw none.
func TestLivelinessOwnToken(t *testing.T) {
	r := openRouter(t)
	cases := map[string]Config{
		"client":          {Connect: r.Locators()},
		"peer-via-router": {Mode: ModePeer, Connect: r.Locators(), Listen: []string{}, Scouting: ScoutingConfig{Disable: true}},
		"peer-alone":      {Mode: ModePeer, Listen: []string{}, Scouting: ScoutingConfig{Disable: true}},
	}
	for name, cfg := range cases {
		t.Run(name, func(t *testing.T) {
			s, err := Open(cfg)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()
			for _, history := range []bool{true, false} {
				key := uniq(t)
				var mu sync.Mutex
				var evs []string
				_, err := s.Liveliness().DeclareSubscriber(key+"/**", func(smp Sample) {
					mu.Lock()
					evs = append(evs, smp.Kind.String()+" "+smp.KeyExpr)
					mu.Unlock()
				}, history)
				if err != nil {
					t.Fatal(err)
				}
				expect := func(what string, want ...string) {
					t.Helper()
					time.Sleep(300 * time.Millisecond)
					mu.Lock()
					defer mu.Unlock()
					if !slices.Equal(evs, want) {
						t.Fatalf("history %v, %s: events %q, want %q", history, what, evs, want)
					}
				}
				tok, err := s.Liveliness().DeclareToken(key + "/me")
				if err != nil {
					t.Fatal(err)
				}
				expect("declared", "PUT "+key+"/me")
				_ = tok.Undeclare()
				expect("undeclared", "PUT "+key+"/me", "DELETE "+key+"/me")
			}
		})
	}
}
