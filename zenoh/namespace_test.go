package zenoh

import (
	"strings"
	"testing"
	"time"
)

func nsClient(t *testing.T, r *Session, ns string) *Session {
	t.Helper()
	s, err := Open(Config{Connect: r.Locators(), Namespace: ns})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestNamespaceInvalid(t *testing.T) {
	for _, ns := range []string{"a/*", "**", "a/$*b", "a//b"} {
		if s, err := Open(Config{Namespace: ns, Listen: []string{}, Scouting: ScoutingConfig{Disable: true}}); err == nil {
			_ = s.Close()
			t.Errorf("namespace %q accepted", ns)
		}
	}
}

func TestNamespacePubSub(t *testing.T) {
	r := openRouter(t)
	plain, ns := clientOf(t, r), nsClient(t, r, "ns")

	in := newSink()
	sub, err := ns.DeclareSubscriber("**", in.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Undeclare()
	if sub.KeyExpr() != "**" {
		t.Fatalf("subscriber key %q", sub.KeyExpr())
	}
	time.Sleep(300 * time.Millisecond)

	_ = plain.Put("ns/a/b", []byte("in"))
	_ = plain.Put("other/x", []byte("outside"))
	if got := in.wait(t, 1); got[0].KeyExpr != "a/b" || string(got[0].Payload) != "in" {
		t.Fatalf("namespaced subscriber got %q %q", got[0].KeyExpr, got[0].Payload)
	}
	in.expectNone(t, 200*time.Millisecond)

	out := newSink()
	psub, err := plain.DeclareSubscriber("ns/**", out.handler)
	if err != nil {
		t.Fatal(err)
	}
	defer psub.Undeclare()
	time.Sleep(300 * time.Millisecond)
	pub, err := ns.DeclarePublisher("c")
	if err != nil {
		t.Fatal(err)
	}
	defer pub.Undeclare()
	if pub.KeyExpr() != "c" {
		t.Fatalf("publisher key %q", pub.KeyExpr())
	}
	_ = pub.Put([]byte("out"))
	if got := out.wait(t, 1); got[0].KeyExpr != "ns/c" {
		t.Fatalf("plain subscriber got %q", got[0].KeyExpr)
	}
	// Our own publication reaches our own subscriber, without the namespace.
	if got := in.wait(t, 1); got[len(got)-1].KeyExpr != "c" {
		t.Fatalf("own sample %q", got[len(got)-1].KeyExpr)
	}
}

func TestNamespaceQueries(t *testing.T) {
	r := openRouter(t)
	plain, ns := clientOf(t, r), nsClient(t, r, "ns")

	var seen []string
	q, err := ns.DeclareQueryable("q/*", func(q *Query) {
		seen = append(seen, q.KeyExpr())
		_ = q.Reply("q/1", []byte("ns-reply"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	pq, err := plain.DeclareQueryable("ns/r", func(q *Query) { _ = q.Reply("ns/r", []byte("plain-reply")) })
	if err != nil {
		t.Fatal(err)
	}
	defer pq.Undeclare()
	time.Sleep(300 * time.Millisecond)

	for sel, want := range map[string]string{"ns/q/1": "q/1", "**/1": "**/1", "*/q/1": "q/1"} {
		replies, err := collect(plain.Get(sel))
		if err != nil || len(replies) != 1 || replies[0].Sample == nil || replies[0].Sample.KeyExpr != "ns/q/1" {
			t.Fatalf("plain get %q: %+v %v", sel, replies, err)
		}
		if seen[len(seen)-1] != want {
			t.Fatalf("queryable saw %q for %q", seen[len(seen)-1], sel)
		}
	}
	replies, err := collect(ns.Get("r"))
	if err != nil || len(replies) != 1 || replies[0].Sample == nil || replies[0].Sample.KeyExpr != "r" {
		t.Fatalf("namespaced get: %+v %v", replies, err)
	}
}

func TestNamespaceLiveliness(t *testing.T) {
	r := openRouter(t)
	plain, ns := clientOf(t, r), nsClient(t, r, "ns")
	tok, err := ns.Liveliness().DeclareToken("t")
	if err != nil {
		t.Fatal(err)
	}
	defer tok.Undeclare()
	if tok.KeyExpr() != "t" {
		t.Fatalf("token key %q", tok.KeyExpr())
	}
	ptok, err := plain.Liveliness().DeclareToken("ns/u")
	if err != nil {
		t.Fatal(err)
	}
	defer ptok.Undeclare()
	otok, err := plain.Liveliness().DeclareToken("other/v")
	if err != nil {
		t.Fatal(err)
	}
	defer otok.Undeclare()
	time.Sleep(300 * time.Millisecond)

	replies, err := collect(plain.Liveliness().Get("ns/t"))
	if err != nil || len(replies) != 1 || replies[0].Sample.KeyExpr != "ns/t" {
		t.Fatalf("plain liveliness get: %+v %v", replies, err)
	}
	replies, err = collect(ns.Liveliness().Get("**"))
	if err != nil {
		t.Fatal(err)
	}
	var keys []string
	for _, r := range replies {
		keys = append(keys, r.Sample.KeyExpr)
	}
	if !strings.Contains(strings.Join(keys, ","), "u") || strings.Contains(strings.Join(keys, ","), "ns/") ||
		strings.Contains(strings.Join(keys, ","), "other") {
		t.Fatalf("namespaced liveliness get: %v", keys)
	}
	k := newSink()
	ls, err := ns.Liveliness().DeclareSubscriber("**", k.handler, false)
	if err != nil {
		t.Fatal(err)
	}
	defer ls.Undeclare()
	time.Sleep(300 * time.Millisecond)
	_ = ptok.Undeclare()
	if got := k.wait(t, 1); got[0].KeyExpr != "u" || got[0].Kind != SampleKindDelete {
		t.Fatalf("liveliness subscriber got %q %v", got[0].KeyExpr, got[0].Kind)
	}
	_ = otok.Undeclare()
	k.expectNone(t, 300*time.Millisecond)
}

// The namespace against zenoh-rust's (config "namespace").
func TestNamespaceRust(t *testing.T) {
	r := routerLocator(t)
	rustNS := func() string {
		return jsonConf(map[string]any{"mode": "client", "connect/endpoints": []string{r},
			"scouting/multicast/enabled": false, "namespace": "ns"})
	}
	goSession := func(ns string) *Session {
		s, err := Open(Config{Connect: []string{r}, Namespace: ns})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	t.Run("go plain to rust namespaced", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, rustNS(), true, "sub", key, "1")
		time.Sleep(300 * time.Millisecond)
		_ = goSession("").Put("ns/"+key, []byte("x"))
		if e := sub.next(); e.Key != key || e.Payload != "x" {
			t.Fatalf("rust got %+v", e)
		}
	})
	t.Run("go namespaced to rust namespaced", func(t *testing.T) {
		key := uniq(t)
		sub := startRust(t, rustNS(), true, "sub", key, "1")
		time.Sleep(300 * time.Millisecond)
		_ = goSession("ns").Put(key, []byte("y"))
		if e := sub.next(); e.Key != key || e.Payload != "y" {
			t.Fatalf("rust got %+v", e)
		}
	})
	t.Run("rust namespaced to go namespaced", func(t *testing.T) {
		key := uniq(t)
		s := goSession("ns")
		k := newSink()
		sub, err := s.DeclareSubscriber(key, k.handler)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Undeclare()
		time.Sleep(300 * time.Millisecond)
		startRust(t, rustNS(), false, "publisher", key, "1")
		if got := k.wait(t, 1); got[0].KeyExpr != key || string(got[0].Payload) != "pub-0" {
			t.Fatalf("go got %q %q", got[0].KeyExpr, got[0].Payload)
		}
	})
	t.Run("rust namespaced queryable", func(t *testing.T) {
		key := uniq(t)
		startRust(t, rustNS(), true, "queryable", key, "10")
		replies, err := collect(goSession("ns").Get(key))
		if err != nil || len(replies) != 1 || replies[0].Sample == nil || replies[0].Sample.KeyExpr != key {
			t.Fatalf("replies %+v %v", replies, err)
		}
	})
}
