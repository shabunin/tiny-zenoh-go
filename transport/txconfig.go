package transport

import (
	"errors"
	"fmt"
	"time"
)

// TxMode selects how a unicast transport writes its batches.
type TxMode uint8

const (
	// TxDefault is TxQueued, except on TinyGo where it is TxSync.
	// TinyGo builds have queues only with the build tag zenoh_txqueue;
	// without it, every mode is TxSync.
	TxDefault TxMode = iota
	// TxQueued runs a transmission queue per link (see TxQueue).
	TxQueued
	// TxSync writes each batch from Send itself: senders wait for the link,
	// whatever their congestion control, and nothing is dropped. It needs no
	// goroutine or buffers per link (and it is what low-latency transports
	// always do).
	TxSync
)

// TxQueue configures the transmission queues of a unicast transport's links.
// Zero fields take zenoh's defaults.
type TxQueue struct {
	Mode TxMode
	// Size is how many batches each priority may hold, 1..16 (default 2);
	// without QoS a single queue has the size of PriorityData's. Batches are
	// allocated when needed.
	Size [numPrio]int
	// WaitBeforeDrop is how long a droppable message waits for room
	// (default 1ms).
	WaitBeforeDrop time.Duration
	// MaxWaitBeforeDropFragments bounds the extra waiting of the further
	// fragments of a droppable message (default 50ms).
	MaxWaitBeforeDropFragments time.Duration
	// WaitBeforeClose is how long a blocking message waits for room before
	// the transport is closed (default 5s).
	WaitBeforeClose time.Duration
	// BatchingTimeLimit is how long the writer may leave a batch that is
	// still being filled while messages keep coming (zenoh's
	// transport/link/tx/queue/batching time_limit; default 1ms, negative:
	// no batching beyond what queues while the link is busy). The writer
	// takes it as soon as no message was added since it last looked;
	// express messages close their batch.
	BatchingTimeLimit time.Duration
}

func (c *TxQueue) setDefaults() {
	if c.Mode == TxDefault {
		c.Mode = txDefaultMode
	}
	if !txQueueAvailable {
		c.Mode = TxSync
	}
	for p, n := range c.Size {
		if n <= 0 {
			c.Size[p] = 2
		} else if n > 16 {
			c.Size[p] = 16
		}
	}
	if c.WaitBeforeDrop == 0 {
		c.WaitBeforeDrop = time.Millisecond
	}
	if c.MaxWaitBeforeDropFragments == 0 {
		c.MaxWaitBeforeDropFragments = 50 * time.Millisecond
	}
	if c.WaitBeforeClose == 0 {
		c.WaitBeforeClose = 5 * time.Second
	}
	if c.BatchingTimeLimit == 0 {
		c.BatchingTimeLimit = time.Millisecond
	}
}

// ErrCongested matches a *CongestionError.
var ErrCongested = errors.New("transport: messages dropped on congestion")

// ErrBlocked closes a transport whose blocking message found no room in its
// queue for TxQueue.WaitBeforeClose.
var ErrBlocked = errors.New("transport: link blocked beyond wait_before_close")

// CongestionError is returned by Send when some of the messages were
// dropped (the others were sent).
type CongestionError struct {
	Dropped []Encoder
}

func (e *CongestionError) Error() string {
	return fmt.Sprintf("transport: %d message(s) dropped on congestion", len(e.Dropped))
}

// Is makes errors.Is(err, ErrCongested) hold.
func (e *CongestionError) Is(target error) bool { return target == ErrCongested }
