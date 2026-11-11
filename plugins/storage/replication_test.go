package storage

import (
	"fmt"
	"math/rand/v2"
	"os"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/zenoh"
)

// The replication settings of the zenohd used by TestReplicationRust:
//
//	R='replication:{interval:1,sub_intervals:5,hot:6,warm:30,propagation_delay:250}'
//	docker run -d --name zenohd-repl --net=host eclipse/zenoh -l tcp/127.0.0.1:17470 \
//	  --no-multicast-scouting \
//	  --cfg "plugins/storage_manager/storages/demo:{key_expr:\"repl/**\",volume:\"memory\",$R}" \
//	  --cfg "plugins/storage_manager/storages/strip:{key_expr:\"replp/**\",strip_prefix:\"replp\",volume:\"memory\",$R}"
func fastReplication() *ReplicationConfig {
	return &ReplicationConfig{Interval: time.Second, SubIntervals: 5, Hot: 6, Warm: 30,
		PropagationDelay: 250 * time.Millisecond}
}

// contents is key=payload for what m stores under prefix, sorted.
func contents(m *Memory, prefix string) []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	es, _ := m.b.Entries()
	for _, e := range es {
		if full := m.full(e.Key, e.Key != ""); strings.HasPrefix(full, prefix) {
			st, _ := m.lookupLocked(e.Key)
			out = append(out, full+"="+string(st.payload))
		}
	}
	sort.Strings(out)
	return out
}

func waitContents(t *testing.T, m *Memory, prefix string, d time.Duration, want ...string) {
	t.Helper()
	sort.Strings(want)
	deadline := time.Now().Add(d)
	var got []string
	for time.Now().Before(deadline) {
		if got = contents(m, prefix); strings.Join(got, ",") == strings.Join(want, ",") {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("storage %s holds %v, want %v", m.s.ZID(), got, want)
}

// inject stores an update in m alone, as if only its subscriber had
// received it, at time at.
func inject(m *Memory, key string, kind zenoh.SampleKind, payload string, at time.Time) zenoh.Timestamp {
	ts := zenoh.Timestamp{Time: protocol.NTP64FromTime(at), ID: m.s.ZID()}
	m.mu.Lock()
	m.processLocked(key, kind, []byte(payload), zenoh.EncodingTextPlain, ts)
	m.mu.Unlock()
	return ts
}

// replicationScenario runs the alignment cases between a storage under
// test (mine, on our side) and another replica, which may be Go's or
// Rust's: readOther returns what the other replica holds under a prefix.
func replicationScenario(t *testing.T, base string, pub *zenoh.Session, mine func() *Memory,
	readOther func(prefix string) []string) {
	pfx := fmt.Sprintf("%s/t%d", base, rand.Uint32())
	waitOther := func(what string, want ...string) {
		t.Helper()
		sort.Strings(want)
		var got []string
		for deadline := time.Now().Add(20 * time.Second); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
			if got = readOther(pfx); strings.Join(got, ",") == strings.Join(want, ",") {
				return
			}
		}
		t.Fatalf("%s: other replica holds %v, want %v", what, got, want)
	}

	// Initial alignment: data published before our storage exists.
	for i := range 3 {
		if err := pub.Put(fmt.Sprintf("%s/init/%d", pfx, i), []byte(fmt.Sprint("v", i))); err != nil {
			t.Fatal(err)
		}
	}
	waitOther("published", pfx+"/init/0=v0", pfx+"/init/1=v1", pfx+"/init/2=v2")
	m := mine()
	waitContents(t, m, pfx, 15*time.Second, pfx+"/init/0=v0", pfx+"/init/1=v1", pfx+"/init/2=v2")

	// Events only we have, in the hot, warm and cold eras: the other
	// replica aligns from our digests.
	now := time.Now()
	inject(m, pfx+"/hot", zenoh.SampleKindPut, "h", now)
	inject(m, pfx+"/warm", zenoh.SampleKindPut, "w", now.Add(-15*time.Second))
	inject(m, pfx+"/cold", zenoh.SampleKindPut, "c", now.Add(-2*time.Minute))
	waitOther("ours to theirs", pfx+"/init/0=v0", pfx+"/init/1=v1", pfx+"/init/2=v2",
		pfx+"/hot=h", pfx+"/warm=w", pfx+"/cold=c")

	// A delete and a wildcard delete only we see.
	inject(m, pfx+"/init/0", zenoh.SampleKindDelete, "", time.Now())
	inject(m, pfx+"/init/*", zenoh.SampleKindDelete, "", time.Now().Add(time.Millisecond))
	waitOther("deletes", pfx+"/hot=h", pfx+"/warm=w", pfx+"/cold=c")

	// An update only they see: we align from their digests.
	_ = m.sub.Undeclare()
	if err := pub.Put(pfx+"/theirs", []byte("t")); err != nil {
		t.Fatal(err)
	}
	waitOther("theirs", pfx+"/hot=h", pfx+"/warm=w", pfx+"/cold=c", pfx+"/theirs=t")
	waitContents(t, m, pfx, 15*time.Second, pfx+"/hot=h", pfx+"/warm=w", pfx+"/cold=c", pfx+"/theirs=t")
}

// TestReplicationGo: two Go replicas align with each other.
func TestReplicationGo(t *testing.T) {
	r := router(t)
	other, err := NewMemory(client(t, r), Config{KeyExpr: "repl/**", Replication: fastReplication()})
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	var m *Memory
	defer func() {
		if m != nil {
			m.Close()
		}
	}()
	replicationScenario(t, "repl", client(t, r), func() *Memory {
		m, err = NewMemory(client(t, r), Config{KeyExpr: "repl/**", Replication: fastReplication()})
		if err != nil {
			t.Fatal(err)
		}
		return m
	}, func(prefix string) []string { return contents(other, prefix) })

	// Both logs end up with the same fingerprints.
	time.Sleep(2 * time.Second)
	now := time.Now()
	m.mu.Lock()
	a := encodeDigest(m.log.digest(now))
	m.mu.Unlock()
	other.mu.Lock()
	b := encodeDigest(other.log.digest(now))
	other.mu.Unlock()
	if string(a) != string(b) {
		t.Errorf("digests differ:\n%x\n%x", a, b)
	}
}

// TestReplicationRust: a Go replica aligns with zenoh-rust's storage
// manager (ZENOH_REPL: a zenohd running the storage above).
func TestReplicationRust(t *testing.T) {
	ep := os.Getenv("ZENOH_REPL")
	if ep == "" {
		t.Skip("ZENOH_REPL not set (a zenohd with a replicated storage on repl/**, see fastReplication)")
	}
	open := func() *zenoh.Session {
		s, err := zenoh.Open(zenoh.Config{Connect: []string{ep}})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	pub := open()
	var rustZID zenoh.ZID
	for _, tr := range pub.Transports() {
		rustZID = tr.ZID
	}
	for _, c := range []Config{{KeyExpr: "repl/**"}, {KeyExpr: "replp/**", StripPrefix: "replp"}} {
		t.Run(c.KeyExpr, func(t *testing.T) {
			c.Replication = fastReplication()
			var m *Memory
			defer func() {
				if m != nil {
					m.Close()
				}
			}()
			replicationScenario(t, strings.TrimSuffix(c.KeyExpr, "/**"), pub, func() *Memory {
				var err error
				if m, err = NewMemory(open(), c); err != nil {
					t.Fatal(err)
				}
				return m
			}, func(prefix string) []string {
				// Only the replies of zenohd's storage.
				ch, err := pub.Get(prefix+"/**", zenoh.WithTimeout(3*time.Second),
					zenoh.WithConsolidation(zenoh.ConsolidationNone))
				if err != nil {
					t.Fatal(err)
				}
				var out []string
				for rep := range ch {
					if rep.Sample != nil && rep.ReplierID != nil && rep.ReplierID.ZID.String() == rustZID.String() {
						out = append(out, rep.Sample.KeyExpr+"="+string(rep.Sample.Payload))
					}
				}
				sort.Strings(out)
				return out
			})

			// zenohd's digest equals ours for the same intervals: both
			// replication logs hold the same events.
			_, digests, err := pub.DeclareSubscriberChan(fmt.Sprintf("@-digest/%s/%d", rustZID, m.log.cfgFP))
			if err != nil {
				t.Fatal(err)
			}
			var last digest
			for deadline := time.After(15 * time.Second); ; {
				select {
				case s := <-digests:
					theirs, err := decodeDigest(s.Payload)
					if err != nil {
						t.Fatal(err)
					}
					last = theirs
					m.mu.Lock()
					upper := m.log.lastElapsed(time.Now())
					same := false
					for _, u := range []uint64{upper, upper - 1} {
						if string(encodeDigest(m.log.digestFrom(u))) == string(encodeDigest(theirs)) {
							same = true
						}
					}
					m.mu.Unlock()
					if same {
						return
					}
				case <-deadline:
					m.mu.Lock()
					mine := m.log.digest(time.Now())
					m.mu.Unlock()
					t.Fatalf("zenohd's digest never matched ours:\n theirs %+v\n ours   %+v", last, mine)
				}
			}
		})
	}
}
