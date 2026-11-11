package zenoh

import (
	"encoding/json"
	"testing"
)

// faceTransports returns the established transports of s.
func compressedBatches(s *Session) (on bool, tx, rx uint64) {
	for _, f := range s.faceList() {
		if f.tr != nil && f.tr.Compression() {
			on = true
			a, b := f.tr.CompressedBatches()
			tx, rx = tx+a, rx+b
		}
	}
	return
}

func TestCompressionRustPeer(t *testing.T) {
	pfx := uniq(t)
	s, err := Open(Config{
		Mode:        ModePeer,
		Listen:      []string{"tcp/127.0.0.1:0"},
		Scouting:    ScoutingConfig{Disable: true},
		Compression: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	c, _ := json.Marshal(map[string]any{
		"mode": "peer", "scouting/multicast/enabled": false, "listen/endpoints": []string{},
		"connect/endpoints":                     s.Locators(),
		"transport/unicast/compression/enabled": true,
	})
	conf := string(c)
	big := pattern(100 << 10)

	t.Run("RustReceives", func(t *testing.T) {
		rs := startRust(t, conf, true, "bigsub", pfx+"/a/**", "3")
		var rf *face // kept: the Rust peer leaves as soon as it has all samples
		waitFor(t, "rust subscriber", func() bool {
			for _, f := range s.faceList() {
				s.mu.Lock()
				n := len(f.subs)
				s.mu.Unlock()
				if n > 0 {
					rf = f
					return true
				}
			}
			return false
		})
		for i := range 3 {
			// Blocking: a burst of droppable puts may be dropped while
			// the batches are compressed (slowly, under -race).
			_ = s.Put(pfx+"/a/x", big[:len(big)-i], WithCongestionControl(CongestionBlock))
		}
		for i := range 3 {
			if e := rs.next(); e.Size != len(big)-i || e.Sum != sum(big[:len(big)-i]) {
				t.Errorf("rust got size %d (sum ok %v)", e.Size, e.Sum == sum(big[:len(big)-i]))
			}
			if i == 0 {
				// All sent; the Rust peer leaves after the last sample, and
				// a closed transport has no links to report on.
				on := rf.tr.Compression()
				tx, _ := rf.tr.CompressedBatches()
				if !on || tx == 0 {
					t.Errorf("compression on=%v, %d batches sent compressed", on, tx)
				}
			}
		}
	})

	t.Run("GoReceives", func(t *testing.T) {
		sink := newSink()
		sub, _ := s.DeclareSubscriber(pfx+"/b", sink.handler)
		defer sub.Undeclare()
		_, _, rx0 := compressedBatches(s)
		startRust(t, conf, false, "bigpub", pfx+"/b", "102400", "5")
		for _, smp := range sink.wait(t, 5) {
			if len(smp.Payload) != 100<<10 || sum(smp.Payload) != sum(big) {
				t.Errorf("go got %d bytes (sum ok %v)", len(smp.Payload), sum(smp.Payload) == sum(big))
			}
		}
		if _, _, rx := compressedBatches(s); rx == rx0 {
			t.Error("no compressed batch received from rust")
		}
	})
}
