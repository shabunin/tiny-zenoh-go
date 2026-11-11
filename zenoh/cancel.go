package zenoh

import (
	"errors"
	"sync"
)

// ErrCancelled is returned when a query is started with a CancellationToken
// that is already cancelled.
var ErrCancelled = errors.New("zenoh: query was cancelled")

// CancellationToken cancels running queries (zenoh's unstable
// CancellationToken): give it to Get, Querier.Get or Liveliness().Get with
// WithCancellation, possibly to several of them, and Cancel ends them all.
// Cancelling is local, nothing is sent: the queries stop delivering replies
// (consolidated replies not yet delivered are dropped, there is no timeout
// error) and complete, as if their callback had been dropped. A cancelled
// token stays cancelled; queries started with it fail with ErrCancelled.
type CancellationToken struct {
	mu        sync.Mutex
	cancelled bool
	handlers  map[uint64]func()
	nextID    uint64
	running   sync.WaitGroup // reply callbacks in progress
}

// NewCancellationToken returns a token that is not cancelled.
func NewCancellationToken() *CancellationToken {
	return &CancellationToken{handlers: map[uint64]func(){}}
}

// WithCancellation lets ct cancel the query.
func WithCancellation(ct *CancellationToken) GetOption {
	return getFn(func(o *getOptions) { o.cancel = ct })
}

// Cancel ends the queries using the token and returns once none of their
// callbacks is running any more, so it must not be called from one of them.
// Calling it again does nothing.
func (c *CancellationToken) Cancel() {
	c.mu.Lock()
	if c.cancelled {
		c.mu.Unlock()
		return
	}
	c.cancelled = true
	hs := c.handlers
	c.handlers = nil
	c.mu.Unlock()
	for _, h := range hs {
		h()
	}
	c.running.Wait()
}

// IsCancelled reports whether Cancel was called.
func (c *CancellationToken) IsCancelled() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cancelled
}

// bind ties a query to the token: cancel runs on Cancel, and the returned
// callbacks wrap cb (which stops being called once the token is cancelled)
// and done (which releases the binding). ok is false if already cancelled.
func (c *CancellationToken) bind(cb func(Reply), done func(), cancel func()) (func(Reply), func(), bool) {
	c.mu.Lock()
	if c.cancelled {
		c.mu.Unlock()
		return nil, nil, false
	}
	c.nextID++
	reg := c.nextID
	c.handlers[reg] = cancel
	c.mu.Unlock()
	wcb := func(r Reply) {
		c.mu.Lock()
		if c.cancelled {
			c.mu.Unlock()
			return
		}
		c.running.Add(1)
		c.mu.Unlock()
		defer c.running.Done()
		cb(r)
	}
	wdone := func() {
		c.mu.Lock()
		delete(c.handlers, reg) // nil map after Cancel: no-op
		c.mu.Unlock()
		if done != nil {
			done()
		}
	}
	return wcb, wdone, true
}
