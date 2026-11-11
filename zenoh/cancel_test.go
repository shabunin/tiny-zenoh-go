package zenoh

import (
	"errors"
	"testing"
	"time"
)

// TestQueryCancellation: a token ends the queries given it (Get, a
// querier's Get and a liveliness Get) without a timeout error, drops the
// replies still to come, and refuses new queries once cancelled.
func TestQueryCancellation(t *testing.T) {
	r := openRouter(t)
	pfx := uniq(t)
	a, b := clientOf(t, r), clientOf(t, r)
	release := make(chan struct{})
	q, err := b.DeclareQueryable(pfx+"/**", func(q *Query) {
		_ = q.Reply(q.KeyExpr(), []byte("early"))
		<-release // hold the query open
		_ = q.Reply(q.KeyExpr(), []byte("late"))
	})
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	defer close(release)
	tok, _ := b.Liveliness().DeclareToken(pfx + "/tok")
	defer tok.Undeclare()
	settle()

	ct := NewCancellationToken()
	got := make(chan Reply, 8)
	done := make(chan struct{})
	if err := a.GetFunc(pfx+"/x", func(r Reply) { got <- r }, func() { close(done) },
		WithCancellation(ct), WithConsolidation(ConsolidationNone), WithTimeout(10*time.Second)); err != nil {
		t.Fatal(err)
	}
	if r := <-got; r.Sample == nil || string(r.Sample.Payload) != "early" {
		t.Fatalf("first reply %+v", r)
	}
	qr, _ := a.DeclareQuerier(pfx+"/y", WithConsolidation(ConsolidationLatest), WithTimeout(10*time.Second))
	defer qr.Undeclare()
	qch, err := qr.Get("", WithCancellation(ct))
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond) // the querier's "early" is held by Latest consolidation

	ct.Cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Get not completed by Cancel")
	}
	for r := range qch {
		t.Errorf("querier delivered %+v after cancel", r)
	}
	select {
	case r := <-got:
		t.Errorf("Get delivered %+v after cancel", r)
	case <-time.After(100 * time.Millisecond):
	}
	if !ct.IsCancelled() {
		t.Error("IsCancelled = false")
	}
	if _, err := a.Get(pfx+"/x", WithCancellation(ct)); !errors.Is(err, ErrCancelled) {
		t.Errorf("Get with a cancelled token: %v", err)
	}
	if _, err := a.Liveliness().Get(pfx+"/**", WithCancellation(ct)); !errors.Is(err, ErrCancelled) {
		t.Errorf("liveliness Get with a cancelled token: %v", err)
	}

	// A token that is never cancelled changes nothing, and is released.
	ct2 := NewCancellationToken()
	lch, err := a.Liveliness().Get(pfx+"/**", WithCancellation(ct2))
	if err != nil {
		t.Fatal(err)
	}
	var n int
	for range lch {
		n++
	}
	if n != 1 {
		t.Errorf("liveliness get: %d replies", n)
	}
	if len(ct2.handlers) != 0 {
		t.Errorf("finished query still bound to its token")
	}
}
