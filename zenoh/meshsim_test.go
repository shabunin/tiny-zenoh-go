package zenoh

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/internal/leaktest"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// The mesh simulation runs a router mesh in one process: routers linked
// over loopback TCP through proxies that can reset or blackhole each link,
// client hosts publishing numbered samples to each other and holding
// liveliness tokens. Phases (forming, every link cut, random flaps, a
// router restart) are each followed by checks that every flow comes back,
// that a quiet window has no duplicates and no gaps, and that every host
// sees every token. It is the lab's scale/recut scenario (lab/run.sh)
// small enough for go test.
//
//	ZENOH_MESHSIM_SEED=n   replay a topology and its flaps
//	ZENOH_MESHSIM=large    also run TestMeshSimLarge (7x7 grid, 8 hosts)

func TestMeshSim(t *testing.T) {
	runMeshSim(t, meshSimConfig{rows: 4, cols: 4, chords: 3, hosts: 4, rounds: 5})
}

func TestMeshSimLarge(t *testing.T) {
	if os.Getenv("ZENOH_MESHSIM") != "large" {
		t.Skip("set ZENOH_MESHSIM=large")
	}
	runMeshSim(t, meshSimConfig{rows: 7, cols: 7, chords: 8, hosts: 8, rounds: 8})
}

type meshSimConfig struct {
	rows, cols int
	chords     int // random extra links on top of the grid
	hosts      int
	rounds     int // random flap rounds
}

const (
	simLease    = time.Second
	simRate     = 20 * time.Millisecond
	simConverge = 20 * time.Second
	simQuiet    = 2 * time.Second
)

// ---- links

type simLinkState int

const (
	simUp simLinkState = iota
	simDown
	simHole
)

// simLink forwards TCP connections to a router. Down closes every
// connection and refuses new ones (a peer reset); hole stalls traffic both
// ways without losing a byte and leaves new connections hanging (a pulled
// cable); up forwards again.
type simLink struct {
	a, b   int // a dials b
	ln     net.Listener
	target string

	mu    sync.Mutex
	state simLinkState
	wake  chan struct{}
	conns []net.Conn
}

func newSimLink(t *testing.T, a, b int, target string) *simLink {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	l := &simLink{a: a, b: b, ln: ln, target: target, wake: make(chan struct{})}
	t.Cleanup(func() {
		_ = ln.Close()
		l.set(simDown)
	})
	go func() {
		for {
			in, err := ln.Accept()
			if err != nil {
				return
			}
			go l.serve(in)
		}
	}()
	return l
}

func (l *simLink) locator() string { return "tcp/" + l.ln.Addr().String() }

func (l *simLink) set(st simLinkState) {
	l.mu.Lock()
	l.state = st
	close(l.wake)
	l.wake = make(chan struct{})
	var conns []net.Conn
	if st == simDown {
		conns, l.conns = l.conns, nil
	}
	l.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}

func (l *simLink) track(c net.Conn) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.state == simDown {
		return false
	}
	l.conns = append(l.conns, c)
	return true
}

// waitUp blocks while the link is a hole; it reports whether it is up.
func (l *simLink) waitUp() bool {
	for {
		l.mu.Lock()
		st, w := l.state, l.wake
		l.mu.Unlock()
		switch st {
		case simUp:
			return true
		case simDown:
			return false
		}
		<-w
	}
}

func (l *simLink) serve(in net.Conn) {
	if !l.track(in) || !l.waitUp() {
		_ = in.Close()
		return
	}
	out, err := net.DialTimeout("tcp", l.target, 2*time.Second)
	if err != nil {
		_ = in.Close()
		return
	}
	if !l.track(out) {
		_ = in.Close()
		_ = out.Close()
		return
	}
	go l.pipe(out, in)
	go l.pipe(in, out)
}

func (l *simLink) pipe(dst, src net.Conn) {
	defer dst.Close()
	defer src.Close()
	buf := make([]byte, 32<<10)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			if !l.waitUp() {
				return
			}
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// ---- hosts

type simHost struct {
	name string
	idx  int
	at   int // router index
	s    *Session
	tok  *LivelinessToken
	seq  atomic.Uint64 // last sequence number published

	mu   sync.Mutex
	got  []map[uint64]int // per source: seq -> copies received
	last []uint64         // per source: highest seq received
}

// ---- the simulation

type meshSim struct {
	t       *testing.T
	cfg     meshSimConfig
	rng     *rand.Rand
	n       int
	edges   [][2]int
	links   []*simLink
	listen  []string   // per router: host:port it listens on
	connect [][]string // per router: locators of its outgoing links
	routers []*Session
	hosts   []*simHost
	gone    map[int]bool // routers closed on purpose
	stop    chan struct{}
	pubs    sync.WaitGroup
}

func runMeshSim(t *testing.T, cfg meshSimConfig) {
	leaktest.Check(t)
	seed := uint64(time.Now().UnixNano())
	if v := os.Getenv("ZENOH_MESHSIM_SEED"); v != "" {
		seed, _ = strconv.ParseUint(v, 10, 64)
	}
	t.Logf("seed %d (replay with ZENOH_MESHSIM_SEED=%d)", seed, seed)
	m := &meshSim{t: t, cfg: cfg, rng: rand.New(rand.NewPCG(seed, seed^0x5eed)), gone: map[int]bool{}, stop: make(chan struct{})}
	m.topology()
	m.openRouters()
	m.openHosts()
	t.Cleanup(m.stopPublishing)

	start := time.Now()
	m.converge("forming")
	t.Logf("mesh of %d routers, %d links, %d hosts formed in %v", m.n, len(m.edges), len(m.hosts),
		time.Since(start).Round(time.Millisecond))
	m.quiet("formed")

	// Every link cut (blackholed) for longer than the lease, then restored.
	m.setAll(simHole)
	time.Sleep(3 * simLease)
	m.setAll(simUp)
	m.converge("mass cut")
	m.quiet("after mass cut")

	// Random flaps: a third of the links reset or blackholed, each for
	// between 0.2 and 2.5 leases, overlapping.
	for r := 1; r <= cfg.rounds; r++ {
		var wg sync.WaitGroup
		var desc []string
		for _, l := range m.links {
			if m.rng.IntN(3) != 0 {
				continue
			}
			st := simDown
			if m.rng.IntN(2) == 0 {
				st = simHole
			}
			delay := time.Duration(m.rng.Int64N(int64(simLease / 2)))
			dur := time.Duration(float64(simLease) * (0.2 + 2.3*m.rng.Float64()))
			kind := map[simLinkState]string{simDown: "reset", simHole: "hole"}[st]
			if m.rng.IntN(3) == 0 {
				// A blip: reset and back within one tree computation (zenoh
				// recomputes at most every 100 ms), the flap zenoh-rust
				// 1.10.2 doesn't recover from.
				st, kind = simDown, "blip"
				dur = time.Duration(10+m.rng.IntN(70)) * time.Millisecond
			}
			desc = append(desc, fmt.Sprintf("r%d-r%d %s %v+%v", l.a+1, l.b+1, kind,
				delay.Round(time.Millisecond), dur.Round(time.Millisecond)))
			wg.Add(1)
			go func() {
				defer wg.Done()
				time.Sleep(delay)
				l.set(st)
				time.Sleep(dur)
				l.set(simUp)
			}()
		}
		wg.Wait()
		t.Logf("flaps round %d: %s", r, strings.Join(desc, ", "))
		m.converge(fmt.Sprintf("flaps round %d", r))
		m.quiet(fmt.Sprintf("after flaps round %d", r))
	}

	// A router without hosts goes away and comes back on the same address.
	if i := m.hostless(); i >= 0 {
		_ = m.routers[i].Close()
		m.gone[i] = true
		time.Sleep(2 * simLease)
		m.converge(fmt.Sprintf("r%d gone", i+1))
		m.routers[i] = m.openRouter(i)
		delete(m.gone, i)
		m.converge(fmt.Sprintf("r%d back", i+1))
		m.quiet(fmt.Sprintf("after r%d restart", i+1))
	}
}

// topology: a rows x cols grid plus random chords. Edge (a, b) has a < b:
// router a dials router b.
func (m *meshSim) topology() {
	rows, cols := m.cfg.rows, m.cfg.cols
	m.n = rows * cols
	have := map[[2]int]bool{}
	add := func(a, b int) {
		if a > b {
			a, b = b, a
		}
		if a == b || have[[2]int{a, b}] {
			return
		}
		have[[2]int{a, b}] = true
		m.edges = append(m.edges, [2]int{a, b})
	}
	for i := 0; i < m.n; i++ {
		if (i+1)%cols != 0 {
			add(i, i+1)
		}
		if i+cols < m.n {
			add(i, i+cols)
		}
	}
	for c := 0; c < m.cfg.chords; {
		before := len(m.edges)
		add(m.rng.IntN(m.n), m.rng.IntN(m.n))
		if len(m.edges) > before {
			c++
		}
	}
	sort.Slice(m.edges, func(i, j int) bool {
		if m.edges[i][0] != m.edges[j][0] {
			return m.edges[i][0] < m.edges[j][0]
		}
		return m.edges[i][1] < m.edges[j][1]
	})
}

// openRouters opens them from the last: each dials only higher-numbered
// routers, which then already listen.
func (m *meshSim) openRouters() {
	m.routers = make([]*Session, m.n)
	m.listen = make([]string, m.n)
	m.connect = make([][]string, m.n)
	for i := m.n - 1; i >= 0; i-- {
		for _, e := range m.edges {
			if e[0] == i {
				l := newSimLink(m.t, e[0], e[1], m.listen[e[1]])
				m.links = append(m.links, l)
				m.connect[i] = append(m.connect[i], l.locator())
			}
		}
		m.routers[i] = m.openRouter(i)
		m.listen[i] = strings.TrimPrefix(m.routers[i].Locators()[0], "tcp/")
	}
}

func (m *meshSim) openRouter(i int) *Session {
	listen := "tcp/127.0.0.1:0"
	if m.listen[i] != "" {
		listen = "tcp/" + m.listen[i]
	}
	var s *Session
	var err error
	// Coming back on the same port can race with the old socket's close.
	for try := 0; try < 20; try++ {
		s, err = Open(Config{Mode: ModeRouter, ZID: m.zid(i), Listen: []string{listen}, Connect: m.connect[i],
			Lease: simLease, Scouting: ScoutingConfig{Disable: true}})
		if err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		m.t.Fatalf("router r%d: %v", i+1, err)
	}
	m.t.Cleanup(func() { _ = s.Close() })
	return s
}

// zid is router i's fixed ZID (as in the lab: r1 is e00001), kept across a
// restart.
func (m *meshSim) zid(i int) ZID {
	z, err := protocol.ParseZID(fmt.Sprintf("e%05x", i+1))
	if err != nil {
		m.t.Fatal(err)
	}
	return z
}

// notWhole lists what keeps the mesh from being whole: a router without a
// live link to a neighbour, a graph that differs from the topology, trees
// still to be recomputed.
func (m *meshSim) notWhole() []string {
	adj := make([]map[string]bool, m.n)
	name := map[string]string{}
	for i := range adj {
		adj[i] = map[string]bool{}
		name[zkey(m.zid(i))] = fmt.Sprintf("r%d", i+1)
	}
	for _, e := range m.edges {
		if m.gone[e[0]] || m.gone[e[1]] {
			continue
		}
		adj[e[0]][zkey(m.zid(e[1]))] = true
		adj[e[1]][zkey(m.zid(e[0]))] = true
	}
	want := m.n - len(m.gone)
	var out []string
	for i, r := range m.routers {
		if m.gone[i] {
			continue
		}
		r.mu.Lock()
		for _, mesh := range r.meshes {
			live := map[string]bool{}
			for f := range mesh.net.links {
				live[zkey(f.zid)] = true
			}
			for z := range adj[i] {
				if !live[z] {
					out = append(out, fmt.Sprintf("r%d has no link to %s", i+1, name[z]))
				}
			}
			nodes := 0
			for _, nd := range mesh.net.nodes {
				if nd == nil {
					continue
				}
				nodes++
				j := -1
				for k := 0; k < m.n; k++ {
					if zkey(m.zid(k)) == zkey(nd.zid) {
						j = k
					}
				}
				if j < 0 || j == i { // ourselves: links tracked above
					continue
				}
				for z := range adj[j] {
					if _, ok := nd.links[z]; !ok {
						out = append(out, fmt.Sprintf("r%d's graph lacks r%d-%s", i+1, j+1, name[z]))
					}
				}
			}
			if nodes != want {
				out = append(out, fmt.Sprintf("r%d's graph has %d routers, not %d", i+1, nodes, want))
			}
			if mesh.treesPending {
				out = append(out, fmt.Sprintf("r%d recomputing its trees", i+1))
			}
		}
		r.mu.Unlock()
	}
	return out
}

// openHosts attaches clients to spread-out routers: corners first, then
// random ones.
func (m *meshSim) openHosts() {
	rows, cols := m.cfg.rows, m.cfg.cols
	at := []int{0, m.n - 1, cols - 1, (rows - 1) * cols, (rows/2)*cols + cols/2}
	used := map[int]bool{}
	var spots []int
	for _, r := range at {
		if !used[r] && len(spots) < m.cfg.hosts {
			used[r] = true
			spots = append(spots, r)
		}
	}
	for len(spots) < m.cfg.hosts {
		if r := m.rng.IntN(m.n); !used[r] {
			used[r] = true
			spots = append(spots, r)
		}
	}
	for i, r := range spots {
		m.hosts = append(m.hosts, &simHost{name: fmt.Sprintf("h%d", i+1), idx: i, at: r})
	}
	for _, h := range m.hosts {
		h.got = make([]map[uint64]int, len(m.hosts))
		for i := range h.got {
			h.got[i] = map[uint64]int{}
		}
		h.last = make([]uint64, len(m.hosts))
		h.s = clientAt(m.t, "tcp/"+m.listen[h.at])
		if _, err := h.s.DeclareSubscriber("meshsim/*", func(s Sample) { m.received(h, s) }); err != nil {
			m.t.Fatal(err)
		}
		tok, err := h.s.Liveliness().DeclareToken("meshsimtok/" + h.name)
		if err != nil {
			m.t.Fatal(err)
		}
		h.tok = tok
	}
	for _, h := range m.hosts {
		p, err := h.s.DeclarePublisher("meshsim/" + h.name)
		if err != nil {
			m.t.Fatal(err)
		}
		m.pubs.Add(1)
		go func() {
			defer m.pubs.Done()
			buf := make([]byte, 8)
			tick := time.NewTicker(simRate)
			defer tick.Stop()
			for {
				select {
				case <-m.stop:
					return
				case <-tick.C:
				}
				binary.BigEndian.PutUint64(buf, h.seq.Add(1))
				_ = p.Put(buf)
			}
		}()
	}
}

func (m *meshSim) stopPublishing() {
	select {
	case <-m.stop:
	default:
		close(m.stop)
	}
	m.pubs.Wait()
}

func (m *meshSim) received(h *simHost, s Sample) {
	name := strings.TrimPrefix(s.KeyExpr, "meshsim/")
	src := -1
	for _, o := range m.hosts {
		if o.name == name {
			src = o.idx
		}
	}
	if src < 0 || len(s.Payload) != 8 {
		m.t.Errorf("%s: unexpected sample %s %x", h.name, s.KeyExpr, s.Payload)
		return
	}
	seq := binary.BigEndian.Uint64(s.Payload)
	h.mu.Lock()
	h.got[src][seq]++
	h.last[src] = max(h.last[src], seq)
	h.mu.Unlock()
}

func (m *meshSim) setAll(st simLinkState) {
	for _, l := range m.links {
		l.set(st)
	}
}

func (m *meshSim) hostless() int {
	used := map[int]bool{}
	for _, h := range m.hosts {
		used[h.at] = true
	}
	best, links := -1, 0
	for i := 0; i < m.n; i++ {
		deg := 0
		for _, e := range m.edges {
			if e[0] == i || e[1] == i {
				deg++
			}
		}
		if !used[i] && deg > links {
			best, links = i, deg
		}
	}
	return best
}

func (m *meshSim) marks() []uint64 {
	out := make([]uint64, len(m.hosts))
	for i, h := range m.hosts {
		out[i] = h.seq.Load()
	}
	return out
}

// converge waits until every host receives something every host published
// from now on, every host sees every token, and the mesh is whole again
// (flows can be back over detours while links are still rejoining).
func (m *meshSim) converge(phase string) {
	m.t.Helper()
	mark := m.marks()
	start := time.Now()
	deadline := start.Add(simConverge)
	for {
		missing := m.missingFlows(mark)
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			m.t.Fatalf("%s: flows not back after %v: %s", phase, simConverge, strings.Join(missing, ", "))
		}
		time.Sleep(50 * time.Millisecond)
	}
	flows := time.Since(start)
	for {
		missing := m.missingTokens()
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			m.t.Fatalf("%s: tokens missing after %v: %s", phase, simConverge, strings.Join(missing, ", "))
		}
		time.Sleep(100 * time.Millisecond)
	}
	tokens := time.Since(start)
	for {
		missing := m.notWhole()
		if len(missing) == 0 {
			break
		}
		if time.Now().After(deadline) {
			if len(missing) > 20 {
				missing = append(missing[:20], "...")
			}
			m.t.Fatalf("%s: mesh not whole after %v: %s", phase, simConverge, strings.Join(missing, ", "))
		}
		time.Sleep(50 * time.Millisecond)
	}
	m.t.Logf("%s: flows back in %v, tokens in %v, mesh whole in %v", phase, flows.Round(time.Millisecond),
		tokens.Round(time.Millisecond), time.Since(start).Round(time.Millisecond))
}

func (m *meshSim) missingFlows(mark []uint64) []string {
	var out []string
	for _, h := range m.hosts {
		h.mu.Lock()
		for src, o := range m.hosts {
			if h.last[src] <= mark[src] {
				out = append(out, o.name+"->"+h.name)
			}
		}
		h.mu.Unlock()
	}
	return out
}

func (m *meshSim) missingTokens() []string {
	var out []string
	for _, h := range m.hosts {
		replies, err := h.s.Liveliness().Get("meshsimtok/*", WithTimeout(time.Second))
		if err != nil {
			out = append(out, h.name+": "+err.Error())
			continue
		}
		seen := map[string]bool{}
		for r := range replies {
			if s, err := r.Result(); err == nil {
				seen[strings.TrimPrefix(s.KeyExpr, "meshsimtok/")] = true
			}
		}
		for _, o := range m.hosts {
			if !seen[o.name] {
				out = append(out, h.name+" lacks "+o.name)
			}
		}
	}
	return out
}

// quiet checks a window of undisturbed traffic: every sample published in
// it reaches every host exactly once.
func (m *meshSim) quiet(phase string) {
	m.t.Helper()
	from := m.marks()
	time.Sleep(simQuiet)
	to := m.marks()
	time.Sleep(500 * time.Millisecond) // the last ones in flight
	var problems []string
	for _, h := range m.hosts {
		h.mu.Lock()
		for src, o := range m.hosts {
			holes, dups := 0, 0
			var at []string // where in the window the holes are
			for seq := from[src] + 1; seq <= to[src]; seq++ {
				switch c := h.got[src][seq]; {
				case c == 0:
					holes++
					if len(at) < 5 {
						at = append(at, fmt.Sprintf("#%d", seq-from[src]))
					}
				case c > 1:
					dups += c - 1
				}
			}
			if holes > 0 || dups > 0 {
				problems = append(problems, fmt.Sprintf("%s->%s: %d of %d missing %v, %d duplicates",
					o.name, h.name, holes, to[src]-from[src], at, dups))
			}
		}
		h.mu.Unlock()
	}
	if len(problems) > 0 {
		m.t.Errorf("%s: quiet window not clean: %s", phase, strings.Join(problems, "; "))
	}
}
