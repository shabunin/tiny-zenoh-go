//go:build tinygo && !zenoh_txqueue

package transport

import (
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Without the zenoh_txqueue tag, TinyGo builds leave the transmission
// queues out (see txqueue.go): links are always TxSync.

const txQueueAvailable = false

type txQueue struct{}

type txChan struct{}

func newTxQueue(*tlink, bool, *TxQueue) *txQueue { return nil }

func (*txQueue) start()              {}
func (*txQueue) stop(error)          {}
func (*txQueue) flush(time.Duration) {}
func (*txQueue) drained() bool       { return true }

func (*txQueue) pushTransport(protocol.Message, time.Duration) (bool, error) {
	return false, ErrClosed
}

func (t *Unicast) sendQueued([]*tlink, bool, []Encoder) error { return ErrClosed }
