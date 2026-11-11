package transport

import (
	"errors"
	"io"
	"sync"
	"time"
)

// prioQueue is how many batches a priority stream may have waiting.
const prioQueue = 16

var errPrioClosed = errors.New("transport: priority stream closed")

// prioWriter writes one priority's batches from a goroutine of its own, so
// that a stream stalled by flow control holds up only its own priority
// (zenoh-rust runs one write task per priority on such links). Batches keep
// their order within the priority.
type prioWriter struct {
	w    io.Writer
	q    chan prioItem
	stop chan struct{} // closed by close: the goroutine exits
	done chan struct{} // closed when the goroutine has exited

	mu  sync.Mutex
	err error
}

type prioItem struct {
	b    []byte
	sync chan struct{} // flush marker: closed once everything before it is written
}

func newPrioWriter(w io.Writer) *prioWriter {
	p := &prioWriter{w: w, q: make(chan prioItem, prioQueue), stop: make(chan struct{}), done: make(chan struct{})}
	go p.run()
	return p
}

func (p *prioWriter) run() {
	defer close(p.done)
	for {
		select {
		case it := <-p.q:
			if it.sync != nil {
				close(it.sync)
				continue
			}
			if _, err := p.w.Write(it.b); err != nil {
				p.mu.Lock()
				p.err = err
				p.mu.Unlock()
				return
			}
		case <-p.stop:
			return
		}
	}
}

// Write queues a copy of b, waiting while the queue is full.
func (p *prioWriter) Write(b []byte) (int, error) {
	p.mu.Lock()
	err := p.err
	p.mu.Unlock()
	if err != nil {
		return 0, err
	}
	select {
	case p.q <- prioItem{b: append([]byte(nil), b...)}:
		return len(b), nil
	case <-p.done:
		return 0, errPrioClosed
	}
}

// flush waits, at most d, until everything queued so far is written.
func (p *prioWriter) flush(d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	ch := make(chan struct{})
	select {
	case p.q <- prioItem{sync: ch}:
	case <-p.done:
		return
	case <-t.C:
		return
	}
	select {
	case <-ch:
	case <-p.done:
	case <-t.C:
	}
}

func (p *prioWriter) close() {
	select {
	case <-p.stop:
	default:
		close(p.stop)
	}
}
