package transport

import (
	"bytes"
	"errors"
	"io"
	"net"
	"sync"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/internal/lz4"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Fuzzing what a remote node controls: received batches (frames, fragments,
// compression, low latency) and both sides of the handshake. Anything may
// fail; nothing may panic or hang.

// -----------------------------------------------------------------------------
// Batches
// -----------------------------------------------------------------------------

func FuzzRxBatches(f *testing.F) {
	push := func(k, v string) []byte {
		var w codec.Writer
		(&protocol.Push{WireExpr: protocol.WireExpr{Suffix: k}, Body: &protocol.Put{Payload: []byte(v)}}).Encode(&w)
		return w.Bytes()
	}
	enc := func(ms ...protocol.Message) []byte {
		var w codec.Writer
		for _, m := range ms {
			m.Encode(&w)
		}
		return w.Bytes()
	}
	stream := func(batches ...[]byte) []byte {
		var b []byte
		for _, x := range batches {
			b = append(b, byte(len(x)), byte(len(x)>>8))
			b = append(b, x...)
		}
		return b
	}
	big := bytes.Repeat([]byte("0123456789"), 30)
	whole := push("a/b", string(big))
	frame := enc(&protocol.Frame{Reliable: true, SN: 1, Payload: append(push("a/b", "x"), push("c", "y")...)},
		&protocol.KeepAlive{})
	frags := stream(
		enc(&protocol.Fragment{Reliable: true, More: true, SN: 1, Payload: whole[:100]}),
		enc(&protocol.Fragment{Reliable: true, More: true, SN: 2, Payload: whole[100:200]}),
		enc(&protocol.Fragment{Reliable: true, SN: 3, Payload: whole[200:]}))
	qos := enc(&protocol.Frame{Reliable: true, SN: 1,
		Exts: []protocol.Extension{{ID: extQoS, Enc: protocol.ExtZ64, U64: 2}}, Payload: push("q", "z")})
	compressed := append([]byte{batchCompressed}, new(lz4.Compressor).Compress(nil, frame)...)

	f.Add(stream(frame), false, false, byte(0))
	f.Add(frags, false, false, byte(0))
	f.Add(stream(qos), true, false, byte(0))
	f.Add(stream(append([]byte{0}, frame...), compressed), false, true, byte(0))
	f.Add(stream(push("ll", "1"), enc(&protocol.KeepAlive{})), false, false, byte(1))
	f.Add(stream(enc(&protocol.Close{Session: true})), false, false, byte(0))

	f.Fuzz(func(t *testing.T, data []byte, qos, compress bool, mode byte) {
		lowLatency := mode&1 != 0
		if lowLatency {
			qos, compress = false, false
		}
		const batchSize = 1 << 12
		u := &Unicast{}
		u.rx.init(snMask(32), qos, 1<<16, func(m protocol.Message, _ bool) error {
			var w codec.Writer
			m.Encode(&w) // what was decoded can be re-encoded
			return nil
		})
		l := &tlink{prefix: streamPrefix, batchSize: batchSize, compress: compress, lowLatency: lowLatency}
		if compress {
			l.lz = &compressState{}
		}
		u.rxLink = l
		r := bytes.NewReader(data)
		buf := make([]byte, batchSize)
		var zbuf []byte
		for {
			batch, err := ReadBatch(r, buf)
			if err != nil {
				return
			}
			if compress {
				if batch, err = l.decompress(batch, &zbuf); err != nil {
					return
				}
				if len(batch) > batchSize {
					t.Fatalf("decompressed %d bytes > batch size %d", len(batch), batchSize)
				}
			}
			if lowLatency {
				err = u.handleLowLatency(batch)
			} else {
				err = u.handleBatch(batch)
			}
			if err != nil {
				return
			}
		}
	})
}

// -----------------------------------------------------------------------------
// Handshake
// -----------------------------------------------------------------------------

// fuzzLink plays the remote side of a handshake from a script of batch
// bodies; fix may rewrite each one just before it is read (to echo what
// we sent, like the cookie). Writes are kept.
type fuzzLink struct {
	mu      sync.Mutex
	script  [][]byte
	fix     func(i int, b []byte, sent []byte) []byte
	next    int
	pending []byte
	sent    []byte
	closed  bool
}

func (l *fuzzLink) Read(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.pending) == 0 {
		if l.closed || l.next >= len(l.script) {
			return 0, io.EOF
		}
		b := l.script[l.next]
		if l.fix != nil {
			b = l.fix(l.next, b, l.sent)
		}
		l.next++
		if len(b) > 0xffff {
			b = b[:0xffff]
		}
		l.pending = append([]byte{byte(len(b)), byte(len(b) >> 8)}, b...)
	}
	n := copy(p, l.pending)
	l.pending = l.pending[n:]
	return n, nil
}

func (l *fuzzLink) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, net.ErrClosed
	}
	l.sent = append(l.sent, p...)
	return len(p), nil
}

func (l *fuzzLink) Close() error {
	l.mu.Lock()
	l.closed = true
	l.mu.Unlock()
	return nil
}

// sentInits returns the INITs found in what was written.
func sentInits(sent []byte) []*protocol.Init {
	var out []*protocol.Init
	r := bytes.NewReader(sent)
	buf := make([]byte, 1<<16)
	for {
		b, err := ReadBatch(r, buf)
		if err != nil {
			return out
		}
		if m, err := protocol.DecodeTransport(codec.NewReader(b)); err == nil {
			if i, ok := m.(*protocol.Init); ok {
				out = append(out, i)
			}
		}
	}
}

// recLink records what goes through a real link.
type recLink struct {
	Link
	mu  sync.Mutex
	out []byte
}

func (l *recLink) Write(p []byte) (int, error) {
	l.mu.Lock()
	l.out = append(l.out, p...)
	l.mu.Unlock()
	return l.Link.Write(p)
}

func (l *recLink) batches() [][]byte {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out [][]byte
	r := bytes.NewReader(l.out)
	buf := make([]byte, 1<<16)
	for {
		b, err := ReadBatch(r, buf)
		if err != nil {
			return out
		}
		out = append(out, append([]byte(nil), b...))
	}
}

var (
	fuzzKeysOnce               sync.Once
	fuzzSrvKey, fuzzCliKey     *RSAKey
	fuzzSrvMLKey, fuzzCliMLKey *MultiLinkKey
)

func fuzzKeys(t testing.TB) {
	fuzzKeysOnce.Do(func() {
		var err error
		for _, k := range []**RSAKey{&fuzzSrvKey, &fuzzCliKey} {
			if *k, err = NewRSAKey(512); err != nil {
				t.Fatal(err)
			}
		}
		for _, k := range []**MultiLinkKey{&fuzzSrvMLKey, &fuzzCliMLKey} {
			if *k, err = NewMultiLinkKey(); err != nil {
				t.Fatal(err)
			}
		}
	})
}

const fuzzProfiles = 5

// fuzzConfigs are the accepting and opening configurations of a profile.
func fuzzConfigs(t testing.TB, profile byte) (acc, open Config) {
	fuzzKeys(t)
	z := func(b byte) protocol.ZID { id, _ := protocol.NewZID([]byte{b, 1, 2, 3}); return id }
	acc = Config{ZID: z(0xa), WhatAmI: protocol.Router}
	open = Config{ZID: z(0xb), WhatAmI: protocol.Client}
	switch profile % fuzzProfiles {
	case 0: // QoS and compression
		acc.QoS, acc.Compression = true, true
		open.QoS, open.Compression = true, true
	case 3: // everything negotiable, with authentication (replayed proofs fail)
		acc.QoS, acc.Compression = true, true
		open.QoS, open.Compression = true, true
		acc.MaxLinks, acc.MultiLinkKey = 2, fuzzSrvMLKey
		open.MaxLinks, open.MultiLinkKey = 2, fuzzCliMLKey
		acc.Auth = &Auth{Users: map[string]string{"u": "p"}, Key: fuzzSrvKey, KnownKeys: []RSAPublicKey{fuzzCliKey.Public()}}
		open.Auth = &Auth{User: "u", Password: "p", Key: fuzzCliKey}
	case 1: // low latency
		acc.LowLatency, open.LowLatency = true, true
	case 4: // user/password (a replayed answer fails)
		acc.Auth = &Auth{Users: map[string]string{"u": "p"}}
		open.Auth = &Auth{User: "u", Password: "p"}
	}
	return acc, open
}

// realHandshake runs a real handshake for a profile and returns what each
// side wrote: the opener's INIT SYN and OPEN SYN, the acceptor's INIT ACK and
// OPEN ACK.
func realHandshake(t testing.TB, profile byte) (opener, acceptor [][]byte) {
	acfg, ocfg := fuzzConfigs(t, profile)
	c1, c2 := net.Pipe()
	ro, ra := &recLink{Link: c1}, &recLink{Link: c2}
	done := make(chan *Unicast, 1)
	go func() {
		u, err := Accept(ra, acfg, Callbacks{})
		if err != nil {
			t.Error(err)
		}
		done <- u
	}()
	o, err := Open(ro, ocfg, Callbacks{})
	if err != nil {
		t.Fatal(err)
	}
	a := <-done
	opener, acceptor = ro.batches(), ra.batches()
	_ = o.Close()
	if a != nil {
		_ = a.Close()
	}
	return opener[:2], acceptor[:2]
}

func FuzzAcceptHandshake(f *testing.F) {
	for p := range byte(fuzzProfiles) {
		o, _ := realHandshake(f, p)
		f.Add(o[0], o[1], p)
	}
	f.Fuzz(func(t *testing.T, syn, osyn []byte, profile byte) {
		acfg, _ := fuzzConfigs(t, profile)
		l := &fuzzLink{script: [][]byte{syn, osyn}, fix: echoCookie}
		if u, err := Accept(l, acfg, Callbacks{}); err == nil {
			_ = u.Close()
		}
	})
}

// echoCookie puts the cookie of the INIT ACK we were sent into a decodable
// OPEN SYN, so the fuzzer gets past the cookie check.
func echoCookie(i int, b, sent []byte) []byte {
	if i != 1 {
		return b
	}
	m, err := protocol.DecodeTransport(codec.NewReader(b))
	if err != nil {
		return b
	}
	o, ok := m.(*protocol.Open)
	inits := sentInits(sent)
	if !ok || len(inits) == 0 {
		return b
	}
	o.Cookie = inits[len(inits)-1].Cookie
	var w codec.Writer
	o.Encode(&w)
	return w.Bytes()
}

func FuzzOpenHandshake(f *testing.F) {
	for p := range byte(fuzzProfiles) {
		_, a := realHandshake(f, p)
		f.Add(a[0], a[1], p)
	}
	f.Fuzz(func(t *testing.T, ack, oack []byte, profile byte) {
		_, ocfg := fuzzConfigs(t, profile)
		l := &fuzzLink{script: [][]byte{ack, oack}}
		u, err := Open(l, ocfg, Callbacks{})
		if err == nil {
			_ = u.Close()
		} else if errors.Is(err, io.ErrShortWrite) {
			t.Fatal(err)
		}
	})
}

// TestFuzzHarnessHandshakes: the unmutated seeds complete the handshake
// through the fuzz links, so mutations start from the deep end.
func TestFuzzHarnessHandshakes(t *testing.T) {
	for p := range byte(fuzzProfiles) {
		o, a := realHandshake(t, p)
		acfg, ocfg := fuzzConfigs(t, p)
		u, err := Accept(&fuzzLink{script: o, fix: echoCookie}, acfg, Callbacks{})
		if p >= 3 {
			if err == nil {
				t.Fatalf("profile %d: a replayed proof was accepted", p)
			}
		} else if err != nil {
			t.Fatalf("profile %d: accept: %v", p, err)
		}
		if u != nil {
			_ = u.Close()
		}
		if u, err = Open(&fuzzLink{script: a}, ocfg, Callbacks{}); err != nil {
			t.Fatalf("profile %d: open: %v", p, err)
		}
		_ = u.Close()
	}
}
