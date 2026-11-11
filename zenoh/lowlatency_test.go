package zenoh

import "testing"

// Low-latency transport against a zenoh-rust peer (transport/unicast/
// lowlatency, which zenoh-rust only allows with QoS off).
func TestLowLatencyRustPeer(t *testing.T) {
	pfx := uniq(t)
	s, err := Open(Config{
		Mode:       ModePeer,
		Listen:     []string{"tcp/127.0.0.1:0"},
		Scouting:   ScoutingConfig{Disable: true},
		LowLatency: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	conf := authRustConf(t, map[string]any{"mode": "peer", "connect/endpoints": s.Locators(),
		"transport/unicast/lowlatency": true, "transport/unicast/qos/enabled": false})

	t.Run("RustToGo", func(t *testing.T) {
		sink := newSink()
		sub, _ := s.DeclareSubscriber(pfx+"/a", sink.handler)
		defer sub.Undeclare()
		startRust(t, conf, false, "pub", pfx+"/a")
		got := sink.wait(t, 2)
		if string(got[0].Payload) != "from-rust" || got[1].Kind != SampleKindDelete {
			t.Fatalf("got %+v", got)
		}
		for _, f := range s.faceList() {
			if f.tr != nil && !f.tr.LowLatency() {
				t.Error("transport is not low latency")
			}
		}
	})
	t.Run("GoToRust", func(t *testing.T) {
		rs := startRust(t, conf, true, "sub", pfx+"/b", "1")
		waitFor(t, "rust subscriber", func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			for _, f := range s.faces {
				if len(f.subs) > 0 && f.tr.LowLatency() {
					return true
				}
			}
			return false
		})
		_ = s.Put(pfx+"/b", []byte("low-latency"))
		if e := rs.next(); e.Payload != "low-latency" {
			t.Fatalf("rust got %+v", e)
		}
	})
	t.Run("Query", func(t *testing.T) {
		q, _ := s.DeclareQueryable(pfx+"/q", func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte("ll-reply")) })
		defer q.Undeclare()
		rs := startRust(t, conf, false, "get", pfx+"/q")
		if e := rs.next(); e.Ok == nil || e.Ok.Payload != "ll-reply" {
			t.Fatalf("rust got %+v", e)
		}
	})
}
