//go:build linux && !tinygo

package shm

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// -----------------------------------------------------------------------------
// Layouts (zenoh-shm 1.x)
// -----------------------------------------------------------------------------

const (
	posixProtocol = 0 // POSIX_PROTOCOL_ID
	shmVersion    = 2 // SHM_VERSION

	metaCount    = 32768 // headers per metadata segment
	hdrSize      = 32
	watchdogBase = metaCount * hdrSize
	metaSize     = watchdogBase + metaCount*8

	offRefcount = 0
	offInvalid  = 4 // AtomicBool; bytes 5..7 are padding, so we use the u32 word
	offGen      = 8
	offProto    = 12
	offSeg      = 16
	offChunk    = 20
	offLen      = 24

	// ShmTransportMetadata (repr(C)).
	authCountOff     = 0
	authChallengeOff = 8
	authVersionOff   = 16
	authProtocolsOff = 24
	authMaxProtocols = 256
	authCountersOff  = authProtocolsOff + 4*authMaxProtocols
	authCounters     = 762 + 2048
	authSize         = authCountersOff + 4*authCounters

	numPrio = 8

	tickEvery    = 50 * time.Millisecond // confirmator period; validator runs every 2nd tick
	disabledHold = 5 * time.Second       // keep-alive for buffers sent without handoff counters
)

// header is one chunk header in a metadata segment.
type header struct {
	seg *segment
	idx uint16
}

func (h header) off(f int) int        { return int(h.idx)*hdrSize + f }
func (h header) refcount() *uint32    { return h.seg.u32(h.off(offRefcount)) }
func (h header) generation() uint32   { return atomic.LoadUint32(h.seg.u32(h.off(offGen))) }
func (h header) invalidated() bool    { return atomic.LoadUint32(h.seg.u32(h.off(offInvalid)))&0xff != 0 }
func (h header) setInvalidated(v int) { atomic.StoreUint32(h.seg.u32(h.off(offInvalid)), uint32(v)) }

func (h header) watchdog() (*uint64, uint64) {
	return h.seg.u64(watchdogBase + int(h.idx/64)*8), 1 << (h.idx % 64)
}

func (h header) confirm() {
	w, m := h.watchdog()
	atomic.OrUint64(w, m)
}

// validate clears the watchdog bit and reports whether it was set.
func (h header) validate() bool {
	w, m := h.watchdog()
	return atomic.AndUint64(w, ^m)&m != 0
}

// confirmKey identifies one allocation of a header we keep confirmed.
type confirmKey struct {
	h   header
	gen uint32
}

// -----------------------------------------------------------------------------
// Process-wide state
// -----------------------------------------------------------------------------

// g is shared by all Managers of the process, like zenoh-rust's statics: our
// metadata segments, the watchdogs we confirm, and the segments of other
// processes we have mapped.
var g struct {
	mu       sync.Mutex
	users    int
	stop     chan struct{}
	metas    []*segment
	free     []header
	confirms map[confirmKey]int
	managers map[*Manager]struct{}

	remoteMeta map[uint64]*segment
	remoteData map[uint64]*segment
}

func acquireGlobal(m *Manager) {
	if g.users == 0 {
		g.confirms = map[confirmKey]int{}
		g.managers = map[*Manager]struct{}{}
		g.remoteMeta = map[uint64]*segment{}
		g.remoteData = map[uint64]*segment{}
		g.stop = make(chan struct{})
		go ticker(g.stop)
	}
	g.users++
	g.managers[m] = struct{}{}
}

func releaseGlobal(m *Manager) {
	delete(g.managers, m)
	g.users--
	if g.users > 0 {
		return
	}
	close(g.stop)
	for _, s := range g.metas {
		s.close()
	}
	for _, s := range g.remoteMeta {
		s.close()
	}
	for _, s := range g.remoteData {
		s.close()
	}
	g.metas, g.free, g.confirms, g.managers = nil, nil, nil, nil
	g.remoteMeta, g.remoteData = nil, nil
}

func ticker(stop chan struct{}) {
	t := time.NewTicker(tickEvery)
	defer t.Stop()
	for n := 0; ; n++ {
		select {
		case <-stop:
			return
		case <-t.C:
		}
		g.mu.Lock()
		if g.users == 0 {
			g.mu.Unlock()
			return
		}
		for k := range g.confirms {
			k.h.confirm()
		}
		if n%2 == 1 {
			now := time.Now()
			for m := range g.managers {
				m.tickLocked(now)
			}
		}
		g.mu.Unlock()
	}
}

func holdLocked(k confirmKey) {
	g.confirms[k]++
	k.h.confirm()
}

func unholdLocked(k confirmKey) {
	if n, ok := g.confirms[k]; ok {
		if n <= 1 {
			delete(g.confirms, k)
		} else {
			g.confirms[k] = n - 1
		}
	}
}

// takeHeaderLocked returns a free header of ours, adding a metadata segment
// when all are in use.
func takeHeaderLocked() (header, error) {
	if len(g.free) == 0 {
		s, err := createSegment(0xffff, metaSize)
		if err != nil {
			return header{}, err
		}
		g.metas = append(g.metas, s)
		for i := range metaCount {
			g.free = append(g.free, header{seg: s, idx: uint16(i)})
		}
	}
	h := g.free[0]
	g.free = g.free[1:]
	return h, nil
}

// reclaimLocked returns a header to the free list, bumping its generation so
// stale references to the old allocation are detected.
func reclaimLocked(h header) {
	delete(g.confirms, confirmKey{h, h.generation()})
	atomic.AddUint32(h.seg.u32(h.off(offGen)), 1)
	g.free = append(g.free, h)
}

func metaSegmentLocked(id uint64) (*segment, error) {
	for _, s := range g.metas {
		if s.id == id {
			return s, nil
		}
	}
	if s := g.remoteMeta[id]; s != nil {
		return s, nil
	}
	s, err := openSegment(id)
	if err != nil {
		return nil, err
	}
	if len(s.mem) < metaSize {
		s.close()
		return nil, errSegmentGone
	}
	g.remoteMeta[id] = s
	return s, nil
}

func dataSegmentLocked(id uint64) (*segment, error) {
	for m := range g.managers {
		if m.data.id == id {
			return m.data, nil
		}
	}
	if s := g.remoteData[id]; s != nil {
		return s, nil
	}
	s, err := openSegment(id)
	if err != nil {
		return nil, err
	}
	g.remoteData[id] = s
	return s, nil
}

// readShm copies a buffer out of shared memory and releases the reference the
// sender made for us.
func readShm(info *protocol.ShmInfo) ([]byte, error) {
	g.mu.Lock()
	if g.users == 0 {
		g.mu.Unlock()
		return nil, ErrInvalidBuffer
	}
	meta, err := metaSegmentLocked(uint64(info.MetaID))
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	if info.MetaIndex >= metaCount {
		g.mu.Unlock()
		return nil, ErrInvalidBuffer
	}
	h := header{seg: meta, idx: info.MetaIndex}
	h.confirm()
	proto := atomic.LoadUint32(h.seg.u32(h.off(offProto)))
	segID := atomic.LoadUint32(h.seg.u32(h.off(offSeg)))
	chunk := uint64(atomic.LoadUint32(h.seg.u32(h.off(offChunk))))
	length := atomic.LoadUint64(h.seg.u64(h.off(offLen)))
	if proto != posixProtocol {
		g.mu.Unlock()
		return nil, fmt.Errorf("shm: unsupported protocol %d", proto)
	}
	data, err := dataSegmentLocked(uint64(segID))
	g.mu.Unlock()
	if err != nil {
		return nil, err
	}
	// From here on we own the reference the sender counted for us.
	defer atomic.AddUint32(h.refcount(), ^uint32(0))
	valid := func() bool { return !h.invalidated() && h.generation() == info.Generation }
	if !valid() || info.DataLen > length || chunk+info.DataLen > uint64(len(data.mem)) {
		return nil, ErrInvalidBuffer
	}
	out := make([]byte, info.DataLen)
	copy(out, data.mem[chunk:chunk+info.DataLen])
	if !valid() { // reclaimed while we copied
		return nil, ErrInvalidBuffer
	}
	return out, nil
}

// -----------------------------------------------------------------------------
// Manager: our data segment, allocator and auth segment
// -----------------------------------------------------------------------------

// Manager owns a session's shared memory: the pool payloads are copied into
// for sending and the "auth" segment peers read during the handshake.
type Manager struct {
	opts Options

	// Guarded by g.mu.
	data      *segment
	free      []span // free ranges of data, sorted by offset
	busy      map[header]span
	auth      *segment
	challenge uint64
	counters  []uint16 // free handoff counters in auth
	links     map[*Link]struct{}
	closed    bool
}

type span struct{ off, size int }

// New creates a Manager with its data and auth segments.
func New(o Options) (*Manager, error) {
	o.setDefaults()
	m := &Manager{opts: o, busy: map[header]span{}, links: map[*Link]struct{}{}}
	var err error
	if m.data, err = createSegment(0xffffffff, o.PoolSize); err != nil {
		return nil, err
	}
	if m.auth, err = createSegment(0xffffffff, authSize); err != nil {
		m.data.close()
		return nil, err
	}
	m.free = []span{{0, o.PoolSize}}
	m.challenge = rand.Uint64()
	a := m.auth.mem
	binary.LittleEndian.PutUint64(a[authCountOff:], 1)
	binary.LittleEndian.PutUint64(a[authChallengeOff:], m.challenge)
	binary.LittleEndian.PutUint64(a[authVersionOff:], shmVersion)
	binary.LittleEndian.PutUint32(a[authProtocolsOff:], posixProtocol)
	for i := range authCounters {
		m.counters = append(m.counters, uint16(i))
	}
	g.mu.Lock()
	acquireGlobal(m)
	g.mu.Unlock()
	return m, nil
}

// Threshold is the smallest payload worth sending through shared memory.
func (m *Manager) Threshold() int { return m.opts.Threshold }

// Close releases the Manager's segments. Buffers still referenced by other
// processes stay valid for them: they hold their own mappings.
func (m *Manager) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil
	}
	m.closed = true
	for l := range m.links {
		l.closeLocked()
	}
	m.gcLocked()
	m.data.close()
	m.auth.close()
	releaseGlobal(m)
	return nil
}

// NewLink returns the SHM state for one transport (its handshake extension).
func (m *Manager) NewLink() *Link { return &Link{m: m} }

func (m *Manager) allocSpanLocked(size int) (int, bool) {
	for i, s := range m.free {
		if s.size < size {
			continue
		}
		off := s.off
		if s.size == size {
			m.free = append(m.free[:i], m.free[i+1:]...)
		} else {
			m.free[i] = span{s.off + size, s.size - size}
		}
		return off, true
	}
	return 0, false
}

func (m *Manager) freeSpanLocked(sp span) {
	i := sort.Search(len(m.free), func(i int) bool { return m.free[i].off > sp.off })
	m.free = append(m.free, span{})
	copy(m.free[i+1:], m.free[i:])
	m.free[i] = sp
	// Merge with the following and preceding ranges.
	if i+1 < len(m.free) && m.free[i].off+m.free[i].size == m.free[i+1].off {
		m.free[i].size += m.free[i+1].size
		m.free = append(m.free[:i+1], m.free[i+2:]...)
	}
	if i > 0 && m.free[i-1].off+m.free[i-1].size == m.free[i].off {
		m.free[i-1].size += m.free[i].size
		m.free = append(m.free[:i], m.free[i+1:]...)
	}
}

// allocLocked takes a chunk of n bytes and a header describing it, with our
// own reference (refcount 1, confirmed).
func (m *Manager) allocLocked(n int) (header, span, protocol.ShmInfo, error) {
	size := (n + 7) &^ 7
	off, ok := m.allocSpanLocked(size)
	if !ok {
		m.gcLocked()
		if off, ok = m.allocSpanLocked(size); !ok {
			return header{}, span{}, protocol.ShmInfo{}, ErrNoMemory
		}
	}
	h, err := takeHeaderLocked()
	if err != nil {
		m.freeSpanLocked(span{off, size})
		return header{}, span{}, protocol.ShmInfo{}, err
	}
	atomic.StoreUint32(h.refcount(), 1)
	h.setInvalidated(0)
	h.validate()
	atomic.StoreUint32(h.seg.u32(h.off(offSeg)), uint32(m.data.id))
	atomic.StoreUint32(h.seg.u32(h.off(offChunk)), uint32(off))
	atomic.StoreUint64(h.seg.u64(h.off(offLen)), uint64(size))
	atomic.StoreUint32(h.seg.u32(h.off(offProto)), posixProtocol)
	sp := span{off, size}
	m.busy[h] = sp
	gen := h.generation()
	holdLocked(confirmKey{h, gen})
	return h, sp, protocol.ShmInfo{
		DataLen:    uint64(n),
		MetaID:     uint16(h.seg.id),
		MetaIndex:  h.idx,
		Generation: gen,
	}, nil
}

// gcLocked frees chunks nobody references any more, or whose holders stopped
// confirming them (crashed readers).
func (m *Manager) gcLocked() {
	for h, sp := range m.busy {
		if atomic.LoadUint32(h.refcount()) == 0 || h.invalidated() {
			delete(m.busy, h)
			m.freeSpanLocked(sp)
			reclaimLocked(h)
		}
	}
}

// tickLocked is the 100 ms validator: invalidate unconfirmed buffers, collect
// garbage, and drop handoff references the receivers have acknowledged.
func (m *Manager) tickLocked(now time.Time) {
	for h := range m.busy {
		if !h.validate() {
			h.setInvalidated(1)
		}
	}
	m.gcLocked()
	for l := range m.links {
		l.pollLocked(now)
	}
}

func (m *Manager) counter(id uint16) *uint32 { return m.auth.u32(authCountersOff + 4*int(id)) }

// -----------------------------------------------------------------------------
// Link: per-transport negotiation and handoff
// -----------------------------------------------------------------------------

// Link is the SHM state of one transport. It implements the transport's
// handshake extension; once the handshake succeeded with SHM on both sides,
// Active reports true and Wrap/Resolve move payloads through shared memory.
type Link struct {
	m *Manager

	// Guarded by g.mu.
	peer        *segment // the peer's auth segment
	peerOffered bool
	txIDs       []uint16 // our counters the peer decrements (nil: disabled)
	txLeased    bool
	rxIDs       []uint16 // the peer's counters we decrement (nil: disabled)
	rxSet       bool
	active      bool
	closed      bool
	pending     [numPrio][]confirmKey // sent, not yet acknowledged
	timed       []timedHold
}

type timedHold struct {
	k        confirmKey
	deadline time.Time
}

func (*Link) ExtID() byte { return ExtID }

// Active reports whether SHM was negotiated on this transport.
func (l *Link) Active() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return l.active && !l.closed
}

func zbufExt(b []byte) *protocol.Extension {
	return &protocol.Extension{ID: ExtID, Enc: protocol.ExtZBuf, Buf: b}
}

// peerChallenge reads the challenge from the peer's auth segment.
func (l *Link) peerChallenge() uint64 {
	return binary.LittleEndian.Uint64(l.peer.mem[authChallengeOff:])
}

// openPeer maps the peer's auth segment, checking its layout.
func (l *Link) openPeer(id uint64) {
	s, err := openSegment(id)
	if err != nil {
		return
	}
	n := binary.LittleEndian.Uint64(s.mem[authCountOff:])
	if len(s.mem) < authSize || n > authMaxProtocols ||
		binary.LittleEndian.Uint64(s.mem[authVersionOff:]) != shmVersion {
		s.close()
		return
	}
	supported := false
	for i := range int(n) {
		if binary.LittleEndian.Uint32(s.mem[authProtocolsOff+4*i:]) == posixProtocol {
			supported = true
		}
	}
	if !supported {
		s.close()
		return
	}
	l.peer = s
}

// leaseTx reserves our per-priority counters (none on unreliable links,
// where a lost message would never be acknowledged).
func (l *Link) leaseTx(reliable bool) error {
	l.txLeased = true
	if !reliable {
		return nil
	}
	m := l.m
	if len(m.counters) < numPrio {
		return errors.New("shm: no free handoff counters")
	}
	l.txIDs = append([]uint16(nil), m.counters[:numPrio]...)
	m.counters = m.counters[numPrio:]
	for _, id := range l.txIDs {
		atomic.StoreUint32(m.counter(id), 0)
	}
	return nil
}

func encodeCounters(w *codec.Writer, ids []uint16) {
	if ids == nil {
		_ = w.WriteByte(0)
		return
	}
	_ = w.WriteByte(1)
	for _, id := range ids {
		w.WriteZInt(uint64(id))
	}
}

func decodeCounters(r *codec.Reader) ([]uint16, error) {
	b, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if b == 0 {
		return nil, nil
	}
	ids := make([]uint16, numPrio)
	for i := range ids {
		v, err := r.ReadZInt()
		if err != nil {
			return nil, err
		}
		if v >= authCounters {
			return nil, fmt.Errorf("shm: invalid counter index %d", v)
		}
		ids[i] = uint16(v)
	}
	return ids, nil
}

// InitSyn offers our auth segment.
func (l *Link) InitSyn() *protocol.Extension {
	g.mu.Lock()
	defer g.mu.Unlock()
	if l.m.closed {
		return nil
	}
	w := codec.NewWriter()
	w.WriteZInt(l.m.auth.id)
	return zbufExt(w.Bytes())
}

// InitAck checks that the responder read our challenge and maps its segment.
func (l *Link) InitAck(e *protocol.Extension) {
	g.mu.Lock()
	defer g.mu.Unlock()
	l.peerOffered = e != nil
	if e == nil || l.m.closed {
		return
	}
	r := codec.NewReader(e.Buf)
	ch, err := r.ReadZInt()
	if err != nil {
		return
	}
	seg, err := r.ReadZInt()
	if err != nil || seg > 0xffffffff || ch != l.m.challenge {
		return
	}
	l.openPeer(seg)
}

// OpenSyn proves we read the responder's segment and announces our counters.
func (l *Link) OpenSyn(reliable bool) *protocol.Extension {
	g.mu.Lock()
	defer g.mu.Unlock()
	if l.peer == nil || l.m.closed {
		return nil
	}
	if err := l.leaseTx(reliable); err != nil {
		return nil
	}
	w := codec.NewWriter()
	w.WriteZInt(l.peerChallenge())
	encodeCounters(w, l.txIDs)
	return zbufExt(w.Bytes())
}

// OpenAck activates SHM when the responder accepted.
func (l *Link) OpenAck(e *protocol.Extension) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e == nil || !l.peerOffered {
		return nil
	}
	if l.peer == nil || !l.txLeased {
		return errors.New("shm: unexpected SHM acknowledgement")
	}
	ids, err := decodeCounters(codec.NewReader(e.Buf))
	if err != nil {
		return fmt.Errorf("shm: OPEN ACK: %w", err)
	}
	l.rxIDs, l.rxSet = ids, true
	l.activateLocked()
	return nil
}

// AcceptInitSyn maps the initiator's auth segment.
func (l *Link) AcceptInitSyn(e *protocol.Extension) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e == nil || l.m.closed {
		return
	}
	seg, err := codec.NewReader(e.Buf).ReadZInt()
	if err != nil || seg > 0xffffffff {
		return
	}
	l.openPeer(seg)
}

// AcceptInitAck echoes the initiator's challenge and offers our segment.
func (l *Link) AcceptInitAck() *protocol.Extension {
	g.mu.Lock()
	defer g.mu.Unlock()
	if l.peer == nil || l.m.closed {
		return nil
	}
	w := codec.NewWriter()
	w.WriteZInt(l.peerChallenge())
	w.WriteZInt(l.m.auth.id)
	return zbufExt(w.Bytes())
}

// AcceptOpenSyn checks the initiator read our challenge.
func (l *Link) AcceptOpenSyn(e *protocol.Extension) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if e == nil || l.peer == nil {
		return nil
	}
	r := codec.NewReader(e.Buf)
	ch, err := r.ReadZInt()
	if err != nil {
		return fmt.Errorf("shm: OPEN SYN: %w", err)
	}
	if ch != l.m.challenge {
		return errors.New("shm: challenge mismatch: refusing the SHM proposal")
	}
	ids, err := decodeCounters(r)
	if err != nil {
		return fmt.Errorf("shm: OPEN SYN: %w", err)
	}
	l.rxIDs, l.rxSet = ids, true
	return nil
}

// AcceptOpenAck announces our counters and activates SHM.
func (l *Link) AcceptOpenAck(reliable bool) *protocol.Extension {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !l.rxSet || l.m.closed {
		return nil
	}
	if err := l.leaseTx(reliable); err != nil {
		return nil
	}
	w := codec.NewWriter()
	encodeCounters(w, l.txIDs)
	l.activateLocked()
	return zbufExt(w.Bytes())
}

func (l *Link) activateLocked() {
	l.active = true
	l.m.links[l] = struct{}{}
}

// Close releases the link's counters and held buffers.
func (l *Link) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	l.closeLocked()
}

func (l *Link) closeLocked() {
	if l.closed {
		return
	}
	l.closed = true
	l.active = false
	delete(l.m.links, l)
	if g.confirms != nil {
		for p := range l.pending {
			for _, k := range l.pending[p] {
				unholdLocked(k)
			}
			l.pending[p] = nil
		}
		for _, t := range l.timed {
			unholdLocked(t.k)
		}
	}
	l.timed = nil
	if l.txIDs != nil && !l.m.closed {
		l.m.counters = append(l.m.counters, l.txIDs...)
	}
	l.txIDs = nil
	if l.peer != nil {
		l.peer.close()
		l.peer = nil
	}
}

// pollLocked releases the references of buffers the receiver acknowledged by
// decrementing our counters.
func (l *Link) pollLocked(now time.Time) {
	for p, ids := 0, l.txIDs; ids != nil && p < numPrio; p++ {
		outstanding := int(int32(atomic.LoadUint32(l.m.counter(ids[p]))))
		// A misbehaving peer may decrement more than we sent: clamp.
		done := min(len(l.pending[p])-outstanding, len(l.pending[p]))
		if done <= 0 {
			continue
		}
		for _, k := range l.pending[p][:done] {
			unholdLocked(k)
		}
		l.pending[p] = append(l.pending[p][:0], l.pending[p][done:]...)
	}
	i := 0
	for ; i < len(l.timed) && now.After(l.timed[i].deadline); i++ {
		unholdLocked(l.timed[i].k)
	}
	l.timed = l.timed[i:]
}

// Buf is a buffer in a Manager's shared memory, made by Alloc: write the
// payload into Bytes, then publish it (zenoh.WithShmBuf). Peers on this
// host that negotiated SHM get a reference to it instead of a copy; the
// others get the bytes. Don't change the bytes once published: receivers
// may still be reading them. Close releases the buffer (it may be closed
// right after publishing: the memory is reused only once every receiver
// is done with it).
type Buf struct {
	m    *Manager
	h    header
	info protocol.ShmInfo
	mem  []byte

	closed bool // guarded by g.mu
}

// Alloc takes an n-byte buffer from the Manager's pool (garbage-collecting
// buffers nobody holds any more if needed). It fails with ErrNoMemory when
// the pool has no room.
func (m *Manager) Alloc(n int) (*Buf, error) {
	if n <= 0 {
		return nil, errors.New("shm: buffer size must be positive")
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if m.closed {
		return nil, ErrInvalidBuffer
	}
	h, sp, info, err := m.allocLocked(n)
	if err != nil {
		return nil, err
	}
	return &Buf{m: m, h: h, info: info, mem: m.data.mem[sp.off : sp.off+n : sp.off+n]}, nil
}

// Bytes is the buffer's memory (shared: valid until Close, and until the
// Manager is closed).
func (b *Buf) Bytes() []byte { return b.mem }

// Len is the buffer's size.
func (b *Buf) Len() int { return len(b.mem) }

// Close releases our reference to the buffer.
func (b *Buf) Close() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	atomic.AddUint32(b.h.refcount(), ^uint32(0))
	if g.confirms != nil {
		unholdLocked(confirmKey{b.h, b.info.Generation})
	}
}

// Wrap copies payload into shared memory for the peer of this link. The
// result's Parts replace the message payload; call Sent once the message was
// handed to the transport.
func (l *Link) Wrap(payload []byte, prio protocol.Priority) (*Wrapped, error) {
	g.mu.Lock()
	if !l.active || l.closed || l.m.closed {
		g.mu.Unlock()
		return nil, ErrInvalidBuffer
	}
	h, sp, info, err := l.m.allocLocked(len(payload))
	if err != nil {
		g.mu.Unlock()
		return nil, err
	}
	b := &Buf{m: l.m, h: h, info: info, mem: l.m.data.mem[sp.off : sp.off+len(payload)]}
	g.mu.Unlock()

	copy(b.mem, payload)

	w, err := l.WrapBuf(b, prio)
	if err != nil {
		b.Close()
		return nil, err
	}
	w.sent = b.Close // the receiver holds its own reference
	return w, nil
}

// WrapBuf gives the peer of this link a reference to b, without copying.
// The result's Parts replace the message payload (Sent has nothing to do).
func (l *Link) WrapBuf(b *Buf, prio protocol.Priority) (*Wrapped, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !l.active || l.closed || b.closed || b.m.closed {
		return nil, ErrInvalidBuffer
	}
	k := confirmKey{b.h, b.info.Generation}
	atomic.AddUint32(b.h.refcount(), 1) // the receiver's reference
	holdLocked(k)                       // kept confirmed until the receiver has it
	if l.txIDs != nil {
		p := int(prio) & (numPrio - 1)
		atomic.AddUint32(l.m.counter(l.txIDs[p]), 1)
		l.pending[p] = append(l.pending[p], k)
	} else {
		l.timed = append(l.timed, timedHold{k, time.Now().Add(disabledHold)})
	}
	info := b.info
	return &Wrapped{Parts: []protocol.PayloadPart{{Shm: &info}}}, nil
}

// Resolve assembles a payload received in parts, reading shared-memory
// references and acknowledging each to the sender.
func (l *Link) Resolve(parts []protocol.PayloadPart, prio protocol.Priority) ([]byte, error) {
	var out []byte
	var firstErr error
	for _, p := range parts {
		if p.Shm == nil {
			out = append(out, p.Raw...)
			continue
		}
		b, err := readShm(p.Shm)
		g.mu.Lock()
		if l.rxIDs != nil && l.peer != nil {
			atomic.AddUint32(l.peer.u32(authCountersOff+4*int(l.rxIDs[int(prio)&(numPrio-1)])), ^uint32(0))
		}
		g.mu.Unlock()
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(parts) == 1 {
			return b, nil
		}
		out = append(out, b...)
	}
	return out, firstErr
}
