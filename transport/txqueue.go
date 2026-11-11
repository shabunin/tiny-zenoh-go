//go:build !tinygo || zenoh_txqueue

package transport

import (
	"errors"
	"io"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/internal/lz4"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Transmission queue and congestion control (zenoh-rust
// io/zenoh-transport/src/common/pipeline.rs, config transport/link/tx/queue).
//
// Each link of a unicast transport gets a bounded queue of batches per
// priority, drained by a writer goroutine of its own (one per priority stream
// on links that have them), highest priority first. Send serializes messages
// into the queued batches; while the writer is busy, further messages are
// appended to the batch at the tail of their queue (adaptive batching: only
// back-pressure delays anything). When a queue is full, what Send does
// depends on the message's congestion control:
//
//   - droppable (CongestionControl::Drop, the default for data, or anything
//     sent best-effort): wait up to WaitBeforeDrop for room, then drop it.
//     Once a priority dropped a message, its droppable messages are dropped
//     straight away until a batch of it is written. A fragmented message
//     waits for each further fragment longer (doubling, up to
//     MaxWaitBeforeDropFragments in all); one abandoned midway is announced to
//     the peer by a FRAGMENT with the drop marker.
//   - blocking (Block: requests, declarations): wait up to WaitBeforeClose,
//     then give up on the peer and close the transport.
//   - block-first (BlockFirst, zenoh unstable): blocks in the background, one
//     message per priority at a time; another one arriving while the first
//     still waits gets WaitBeforeDrop for it to finish and is dropped after.
//
// Dropped messages are reported by Send as a *CongestionError.

// txQueueAvailable: this build has transmission queues.
const txQueueAvailable = true

// congestion is a message's congestion control as the queue applies it.
type congestion uint8

const (
	ccDrop congestion = iota
	ccBlock
	ccBlockFirst
)

func congestionOf(reliable bool, q protocol.QoS) congestion {
	switch {
	case !reliable:
		return ccDrop // zenoh-rust is_droppable: best effort always is
	case q.BlockFirst():
		return ccBlockFirst
	case q.Block():
		return ccBlock
	}
	return ccDrop
}

// txBuf is one batch of a queue: length prefix and header placeholders, then
// transport messages.
type txBuf struct {
	w codec.Writer
	// sealed: it ends with a FRAGMENT, which runs to the end of the batch.
	sealed bool
	// fch is the channel (chanIndex) of the FRAME that ends the batch, -1 if
	// none; fsn is that frame's SN. A further message of that channel can
	// join the frame while no later SN was taken.
	fch int
	fsn uint64
}

func chanIndex(prio protocol.Priority, reliable bool) int {
	if reliable {
		return int(prio)*2 + 1
	}
	return int(prio) * 2
}

// txQueue is the transmission queue of one link.
type txQueue struct {
	l     *tlink
	qos   bool
	size  [numPrio]int
	work  sync.Cond         // writers wait on it, with mu
	mu    sync.Mutex        // guards everything below
	space chan struct{}     // closed and replaced whenever room is made or the queue stops
	q     [numPrio][]*txBuf // batches waiting, oldest first
	used  [numPrio]int      // batches waiting or being written
	free  []*txBuf
	err   error // the queue stopped: nothing more is accepted or written

	congested [numPrio]atomic.Bool
	first     [numPrio]chan struct{} // BlockFirst token per priority
}

func newTxQueue(l *tlink, qos bool, cfg *TxQueue) *txQueue {
	q := &txQueue{l: l, qos: qos, size: cfg.Size, space: make(chan struct{})}
	if !qos {
		q.size[0] = cfg.Size[protocol.PriorityData]
	}
	q.work.L = &q.mu
	for i := range q.first {
		q.first[i] = make(chan struct{}, 1)
	}
	return q
}

// start runs the writers: one for all priorities, or one per priority
// stream.
func (q *txQueue) start() {
	if q.l.prioW == nil {
		go q.run(0, numPrio-1, q.l.link)
		return
	}
	n := 1
	if q.qos {
		n = numPrio
	}
	for i := range n {
		go q.run(i, i, q.l.prioW[i])
	}
}

// index is the queue a priority uses.
func (q *txQueue) index(prio protocol.Priority) int {
	if q.qos {
		return int(prio)
	}
	return 0
}

// run writes the batches of queues lo..hi to w, highest priority first.
func (q *txQueue) run(lo, hi int, w io.Writer) {
	var zc lz4.Compressor
	var zbuf []byte
	for {
		q.mu.Lock()
		i := -1
		for q.err == nil {
			for k := lo; k <= hi; k++ {
				if len(q.q[k]) > 0 {
					i = k
					break
				}
			}
			if i >= 0 {
				break
			}
			q.work.Wait()
		}
		if q.err != nil {
			q.mu.Unlock()
			return
		}
		b := q.q[i][0]
		q.q[i][0] = nil
		q.q[i] = q.q[i][1:]
		q.mu.Unlock()

		err := q.l.writeBatch(w, b.w.Bytes(), &zc, &zbuf)

		q.mu.Lock()
		q.used[i]--
		q.free = append(q.free, b)
		q.congested[i].Store(false)
		q.signalLocked()
		q.mu.Unlock()
		if err != nil {
			q.stop(err)
			q.l.t.dropLink(q.l, err)
			return
		}
	}
}

// signalLocked wakes everyone waiting for room.
func (q *txQueue) signalLocked() {
	close(q.space)
	q.space = make(chan struct{})
}

// stop makes the queue refuse everything from now on; the writers exit
// (one in the middle of a write finishes it).
func (q *txQueue) stop(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return
	}
	q.err = err
	q.signalLocked()
	q.work.Broadcast()
}

// flush waits, at most d, until everything queued so far is written.
func (q *txQueue) flush(d time.Duration) {
	deadline := time.Now().Add(d)
	q.mu.Lock()
	defer q.mu.Unlock()
	for q.err == nil && q.busyLocked() {
		if !q.waitLocked(deadline) {
			return
		}
	}
}

func (q *txQueue) busyLocked() bool {
	for _, n := range q.used {
		if n > 0 {
			return true
		}
	}
	return false
}

// waitLocked waits for room until deadline (zero: not at all); false once
// it passed. mu is released meanwhile.
func (q *txQueue) waitLocked(deadline time.Time) bool {
	if deadline.IsZero() {
		return false
	}
	d := time.Until(deadline)
	if d <= 0 {
		return false
	}
	ch := q.space
	t := time.NewTimer(d)
	defer t.Stop()
	q.mu.Unlock()
	defer q.mu.Lock()
	select {
	case <-ch:
		return true
	case <-t.C:
		return false
	}
}

// newBufLocked appends an empty batch to queue i.
func (q *txQueue) newBufLocked(i int) *txBuf {
	var b *txBuf
	if n := len(q.free); n > 0 {
		b, q.free = q.free[n-1], q.free[:n-1]
		b.w.Reset()
	} else {
		b = &txBuf{}
	}
	b.sealed, b.fch = false, -1
	b.w.WriteRaw(lenPlaceholder[:q.l.prefix+q.l.hdrLen()])
	q.q[i] = append(q.q[i], b)
	q.used[i]++
	return b
}

// tailLocked is the batch at the tail of queue i that more may be appended
// to, or nil.
func (q *txQueue) tailLocked(i int) *txBuf {
	if n := len(q.q[i]); n > 0 && !q.q[i][n-1].sealed {
		return q.q[i][n-1]
	}
	return nil
}

// reserveLocked finds room in queue i for n bytes: at the tail when fit
// says it fits there (nil fit: a new batch is required), else in a new
// batch. It waits until deadline (zero: not at all) and returns nil if
// there is still no room then.
func (q *txQueue) reserveLocked(i int, fit func(*txBuf) bool, deadline time.Time) (*txBuf, error) {
	for {
		if q.err != nil {
			return nil, q.err
		}
		if fit != nil {
			if b := q.tailLocked(i); b != nil && fit(b) {
				return b, nil
			}
		}
		if q.used[i] < q.size[i] {
			return q.newBufLocked(i), nil
		}
		q.work.Broadcast() // what is held back must go out to make room
		if !q.waitLocked(deadline) {
			// Room may have been made just as the wait ended.
			if q.err == nil && q.used[i] < q.size[i] {
				return q.newBufLocked(i), nil
			}
			return nil, nil
		}
	}
}

// commitLocked hands what was written to the writers and releases mu.
func (q *txQueue) commitLocked() {
	q.work.Broadcast()
	q.mu.Unlock()
}

// kick wakes the writers for what was queued without commitLocked (Send
// holds its batches back until it is done, so that its messages share them).
func (q *txQueue) kick() {
	q.mu.Lock()
	q.work.Broadcast()
	q.mu.Unlock()
}

// pushTransport queues a transport message (KEEP_ALIVE, CLOSE) on the
// Control queue, waiting at most wait for room (0: not at all); false if
// there was none.
func (q *txQueue) pushTransport(m protocol.Message, wait time.Duration) (bool, error) {
	var enc codec.Writer
	m.Encode(&enc)
	n := enc.Len()
	bs := q.l.batchSize
	var deadline time.Time
	if wait > 0 {
		deadline = time.Now().Add(wait)
	}
	i := q.index(protocol.PriorityControl)
	q.mu.Lock()
	b, err := q.reserveLocked(i, func(b *txBuf) bool { return b.w.Len()+n <= bs }, deadline)
	if b == nil {
		q.mu.Unlock()
		return false, err
	}
	b.w.WriteRaw(enc.Bytes())
	b.fch = -1
	q.l.stats.addTransport(Tx)
	q.commitLocked()
	return true, nil
}

// txChan is the sending state of one priority: its lock is held while a
// message of the priority is serialized into a queue, so messages keep their
// order and sequence numbers.
type txChan struct {
	mu  sync.Mutex
	enc codec.Writer
}

// fragWait is the deadline schedule of a message (zenoh-rust Deadline):
// set when it is first needed, at the latest after the first fragment;
// each further fragment extends it by a doubling wait, for droppable
// messages by at most max in all.
type fragWait struct {
	wait, max time.Duration
	limited   bool
	deadline  time.Time
}

func (w *fragWait) get() time.Time {
	if w.deadline.IsZero() {
		w.deadline = time.Now().Add(w.wait)
	}
	return w.deadline
}

func (w *fragWait) next() {
	w.get()
	w.wait *= 2
	if w.limited {
		if w.max <= 0 {
			return
		}
		if w.max <= w.wait {
			w.deadline = w.deadline.Add(w.max)
			w.max = 0
			return
		}
		w.max -= w.wait
	}
	w.deadline = w.deadline.Add(w.wait)
}

// sendQueued queues msgs on their links; see the comment at the top.
func (t *Unicast) sendQueued(links []*tlink, reliable bool, msgs []Encoder) error {
	var dropped []Encoder
	var kick [2]*txQueue
	queues := kick[:0]
	defer func() {
		for _, q := range queues {
			q.kick()
		}
	}()
	for _, m := range msgs {
		prio := t.txPriority(m)
		l := links[0]
		if len(links) > 1 {
			l = selectLink(links, reliable, prio)
		}
		if !slices.Contains(queues, l.txq) {
			queues = append(queues, l.txq)
		}
		ok, err := t.pushMessage(l, prio, reliable, congestionOf(reliable, protocol.QoSOf(m)), m)
		if err != nil {
			return err
		}
		if !ok {
			dropped = append(dropped, m)
		}
	}
	if dropped != nil {
		return &CongestionError{Dropped: dropped}
	}
	return nil
}

// pushMessage queues one network message on l; false if it was dropped.
func (t *Unicast) pushMessage(l *tlink, prio protocol.Priority, reliable bool, cc congestion, m Encoder) (bool, error) {
	q := l.txq
	i := q.index(prio)
	if cc == ccDrop && q.congested[i].Load() {
		return false, nil
	}
	cfg := &t.cfg.TxQueue
	if cc == ccBlockFirst {
		t0 := time.NewTimer(cfg.WaitBeforeDrop)
		select {
		case q.first[i] <- struct{}{}:
			t0.Stop()
		case <-t0.C:
			return false, nil
		}
		var enc codec.Writer
		m.Encode(&enc)
		ni, count := l.stats.netIndex(Tx, m)
		go func() {
			defer func() { <-q.first[i] }()
			c := &t.txc[prio]
			c.mu.Lock()
			ok, err := t.pushEncoded(l, q, i, prio, reliable, false, false, enc.Bytes())
			c.mu.Unlock()
			if ok && count {
				l.stats.nmsgs[ni].Add(1)
			}
			if errors.Is(err, ErrBlocked) {
				t.shutdown(err)
			}
		}()
		return true, nil
	}
	c := &t.txc[prio]
	c.mu.Lock()
	defer c.mu.Unlock()
	if cc == ccDrop && q.congested[i].Load() {
		return false, nil // congested while we waited for the lock
	}
	c.enc.Reset()
	m.Encode(&c.enc)
	ok, err := t.pushEncoded(l, q, i, prio, reliable, cc == ccDrop, true, c.enc.Bytes())
	if errors.Is(err, ErrBlocked) {
		go t.shutdown(err)
		return false, ErrClosed
	}
	if ok {
		l.stats.addNetwork(Tx, m)
	} else if cc == ccDrop {
		q.congested[i].Store(true)
	}
	return ok, err
}

// pushEncoded queues the serialized network message msg on queue i of l, in
// a FRAME or in FRAGMENTs. The caller holds t.txc[prio].mu.
// With hold, a FRAME is left for the caller to kick the writers.
func (t *Unicast) pushEncoded(l *tlink, q *txQueue, i int, prio protocol.Priority, reliable, droppable, hold bool, msg []byte) (bool, error) {
	cfg := &t.cfg.TxQueue
	w := fragWait{wait: cfg.WaitBeforeClose}
	if droppable {
		w = fragWait{wait: cfg.WaitBeforeDrop, max: cfg.MaxWaitBeforeDropFragments, limited: true}
	}
	block := func() (bool, error) {
		if droppable {
			return false, nil
		}
		return false, ErrBlocked
	}
	bs := l.batchSize
	ch := chanIndex(prio, reliable)
	si := 0
	if reliable {
		si = 1
	}
	exts := qosExts(prio)
	capacity := t.frameCapacity(l)
	if len(msg) <= capacity {
		hdr := 1 + vleLen(t.snMask) + 2
		next := t.txSN[prio][si]
		fit := func(b *txBuf) bool {
			if b.fch == ch && (b.fsn+1)&t.snMask == next {
				return b.w.Len()+len(msg) <= bs
			}
			return b.w.Len()+hdr+len(msg) <= bs
		}
		q.mu.Lock()
		b, err := q.reserveLocked(i, fit, time.Time{})
		if b == nil && err == nil {
			b, err = q.reserveLocked(i, fit, w.get())
		}
		if b == nil {
			q.mu.Unlock()
			if err != nil {
				return false, err
			}
			return block()
		}
		if b.fch == ch && (b.fsn+1)&t.snMask == next && b.w.Len()+len(msg) <= bs {
			b.w.WriteRaw(msg) // joins the frame that ends the batch
		} else {
			f := protocol.Frame{Reliable: reliable, SN: t.nextSNLocked(prio, reliable), Exts: exts, Payload: msg}
			f.Encode(&b.w)
			b.fch, b.fsn = ch, f.SN
			l.stats.addTransport(Tx)
		}
		if hold {
			q.mu.Unlock()
		} else {
			q.commitLocked()
		}
		return true, nil
	}

	// Fragments, each in a batch of its own.
	first := t.patch >= patchFragmentMarkers
	sent := false
	for len(msg) > 0 {
		q.mu.Lock()
		b, err := q.reserveLocked(i, nil, time.Time{})
		if b == nil && err == nil {
			b, err = q.reserveLocked(i, nil, w.get())
		}
		if b == nil {
			q.mu.Unlock()
			if err != nil {
				return false, err
			}
			if sent {
				t.abandonFragments(q, i, prio, reliable)
			}
			return block()
		}
		fexts, fcap := exts, capacity
		if first {
			fexts = append(append([]protocol.Extension(nil), exts...),
				protocol.Extension{ID: extFragFirst, Enc: protocol.ExtUnit})
			fcap--
			first = false
		}
		n := min(fcap, len(msg))
		f := protocol.Fragment{Reliable: reliable, More: n < len(msg), SN: t.nextSNLocked(prio, reliable),
			Exts: fexts, Payload: msg[:n]}
		f.Encode(&b.w)
		b.sealed = true
		l.stats.addTransport(Tx)
		q.commitLocked()
		msg = msg[n:]
		sent = true
		w.next()
	}
	return true, nil
}

// abandonFragments ends a message whose remaining fragments could not be
// queued: patched peers get a FRAGMENT with the drop marker (in a batch over
// the queue's size, as zenoh-rust's ephemeral batch); for the others, the
// skipped SN tells the message is incomplete.
func (t *Unicast) abandonFragments(q *txQueue, i int, prio protocol.Priority, reliable bool) {
	sn := t.nextSNLocked(prio, reliable)
	if t.patch < patchFragmentMarkers {
		return
	}
	exts := append(qosExts(prio), protocol.Extension{ID: extFragDrop, Enc: protocol.ExtUnit})
	q.mu.Lock()
	if q.err != nil {
		q.mu.Unlock()
		return
	}
	b := q.newBufLocked(i)
	f := protocol.Fragment{Reliable: reliable, More: true, SN: sn, Exts: exts}
	f.Encode(&b.w)
	b.sealed = true
	q.l.stats.addTransport(Tx)
	q.commitLocked()
}
