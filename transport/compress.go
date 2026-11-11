package transport

import (
	"errors"
	"sync/atomic"

	"github.com/shabunin/gozenoh/internal/lz4"
)

// Batch compression (zenoh-rust io/zenoh-transport/src/common/batch.rs).
// Negotiated by the unit INIT extension 0x6; the INIT/OPEN exchange itself is
// never compressed. Afterwards each batch starts (after the stream length
// prefix) with a header byte whose bit 0 says the rest is one LZ4 block; a
// batch is sent compressed only when that is smaller. The block decompresses
// to at most the negotiated batch size.

const (
	extCompression  = 0x6
	batchCompressed = 0x01
)

var errDecompress = errors.New("transport: bad compressed batch")

type compressState struct {
	c     lz4.Compressor
	txBuf []byte // compressed batch being sent
	rxBuf []byte // decompressed batch being handled

	txCompressed, rxCompressed atomic.Uint64
}

func (l *tlink) enableCompression(on bool) {
	if on {
		l.compress = true
		l.lz = &compressState{}
	}
}

// compressLocked fills in the header byte of the batch b (prefix, header,
// body) and returns the batch to send: b itself or its compressed form.
func (l *tlink) compressBatch(b []byte) []byte {
	h := l.prefix
	body := b[h+1:]
	b[h] = 0
	if len(body) < 32 { // keep-alives and the like: not worth trying
		return b
	}
	z := l.lz
	out := append(z.txBuf[:0], b[:h]...)
	out = append(out, batchCompressed)
	out = z.c.Compress(out, body)
	z.txBuf = out
	if len(out) >= len(b) {
		return b
	}
	z.txCompressed.Add(1)
	return out
}

// decompress strips the batch header and, if set, inflates the body.
func (l *tlink) decompress(batch []byte) ([]byte, error) {
	if len(batch) == 0 {
		return nil, errDecompress
	}
	if batch[0]&batchCompressed == 0 {
		return batch[1:], nil
	}
	z := l.lz
	out, err := lz4.Decompress(z.rxBuf[:0], batch[1:], l.batchSize)
	if err != nil {
		return nil, errors.Join(errDecompress, err)
	}
	z.rxBuf = out
	z.rxCompressed.Add(1)
	return out, nil
}

// Compression reports whether batch compression is in use on the first link.
func (t *Unicast) Compression() bool {
	l := t.firstLink()
	return l != nil && l.compress
}

// CompressedBatches returns how many batches were sent and received
// compressed (over all current links).
func (t *Unicast) CompressedBatches() (tx, rx uint64) {
	for _, l := range t.linkList() {
		if l.lz != nil {
			tx += l.lz.txCompressed.Load()
			rx += l.lz.rxCompressed.Load()
		}
	}
	return tx, rx
}
