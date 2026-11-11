package zenoh

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/shabunin/gozenoh/internal/codec"
	"github.com/shabunin/gozenoh/link"
	"github.com/shabunin/gozenoh/protocol"
)

// Hello describes a node found by scouting.
type Hello struct {
	ZID      ZID
	WhatAmI  WhatAmI
	Locators []string
}

// Scouting periods, as in zenoh-rust (runtime/orchestrator.rs).
const (
	scoutInitialPeriod = time.Second
	scoutMaxPeriod     = 8 * time.Second
)

// scoutSocket is a unicast UDP socket bound to one interface address. SCOUTs
// are multicast from it and HELLOs come back to it.
type scoutSocket struct {
	conn *net.UDPConn
	ip   net.IP
}

// scouter sends SCOUTs and/or answers them.
type scouter struct {
	group   *net.UDPAddr
	ucast   []scoutSocket
	mcast   *net.UDPConn // nil when not answering SCOUTs
	what    protocol.WhatAmIMatcher
	onHello func(Hello)
	answer  func(src *net.UDPAddr) *protocol.Hello // nil: don't answer

	stop chan struct{}
	once sync.Once
	wg   sync.WaitGroup
}

// scoutInterfaces returns the IPv4 addresses to scout on: those of the named
// interface, or of every up, multicast-capable, non-loopback interface.
func scoutInterfaces(name string) ([]net.IP, *net.Interface, error) {
	ifs, err := net.Interfaces()
	if err != nil {
		return nil, nil, err
	}
	var ips []net.IP
	var first *net.Interface
	for i := range ifs {
		ifi := &ifs[i]
		if name != "" {
			if ifi.Name != name {
				continue
			}
		} else if ifi.Flags&net.FlagUp == 0 || ifi.Flags&net.FlagMulticast == 0 || ifi.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, err := ifi.Addrs()
		if err != nil {
			continue
		}
		for _, a := range addrs {
			if ipn, ok := a.(*net.IPNet); ok && ipn.IP.To4() != nil {
				ips = append(ips, ipn.IP.To4())
				if first == nil {
					first = ifi
				}
			}
		}
	}
	if name != "" && len(ips) == 0 {
		return nil, nil, errors.New("zenoh: no IPv4 address on interface " + name)
	}
	return ips, first, nil
}

func newScouter(cfg ScoutingConfig, listen bool) (*scouter, error) {
	group, err := net.ResolveUDPAddr("udp4", cfg.Address)
	if err != nil {
		return nil, err
	}
	ips, ifi, err := scoutInterfaces(cfg.Interface)
	if err != nil {
		return nil, err
	}
	sc := &scouter{group: group, stop: make(chan struct{})}
	if len(ips) == 0 {
		ips = []net.IP{net.IPv4zero} // let the routing table pick
	}
	for _, ip := range ips {
		c, err := net.ListenUDP("udp4", &net.UDPAddr{IP: ip})
		if err != nil {
			continue
		}
		sc.ucast = append(sc.ucast, scoutSocket{conn: c, ip: ip})
	}
	if len(sc.ucast) == 0 {
		return nil, errors.New("zenoh: no usable scouting interface")
	}
	if listen {
		mc, err := listenMulticast(ifi, group)
		if err != nil && !errors.Is(err, errNoMulticastListen) {
			sc.closeSockets()
			return nil, err
		}
		sc.mcast = mc // nil on platforms that can't join groups: we only scout
	}
	return sc, nil
}

func (sc *scouter) closeSockets() {
	for _, u := range sc.ucast {
		_ = u.conn.Close()
	}
	if sc.mcast != nil {
		_ = sc.mcast.Close()
	}
}

func (sc *scouter) start() {
	if sc.onHello != nil {
		sc.wg.Add(1)
		go sc.sendLoop()
		for _, u := range sc.ucast {
			sc.wg.Add(1)
			go sc.helloLoop(u.conn)
		}
	}
	if sc.mcast != nil && sc.answer != nil {
		sc.wg.Add(1)
		go sc.responder()
	}
}

func (sc *scouter) close() {
	sc.once.Do(func() {
		close(sc.stop)
		sc.closeSockets()
	})
	sc.wg.Wait()
}

func (sc *scouter) sendLoop() {
	defer sc.wg.Done()
	var w codec.Writer
	(&protocol.Scout{Version: protocol.ProtocolVersion, What: sc.what}).Encode(&w)
	msg := w.Bytes()
	delay := scoutInitialPeriod
	for {
		for _, u := range sc.ucast {
			_, _ = u.conn.WriteToUDP(msg, sc.group)
		}
		select {
		case <-sc.stop:
			return
		case <-time.After(delay):
		}
		if delay*2 <= scoutMaxPeriod {
			delay *= 2
		}
	}
}

func (sc *scouter) helloLoop(c *net.UDPConn) {
	defer sc.wg.Done()
	buf := make([]byte, 8192)
	for {
		n, _, err := c.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-sc.stop:
				return
			default:
				continue
			}
		}
		m, err := protocol.DecodeScouting(buf[:n])
		if err != nil {
			continue
		}
		if h, ok := m.(*protocol.Hello); ok && sc.what.Matches(h.WhatAmI) {
			sc.onHello(Hello{ZID: h.ZID, WhatAmI: h.WhatAmI, Locators: h.Locators})
		}
	}
}

// responder answers SCOUTs received on the multicast group.
func (sc *scouter) responder() {
	defer sc.wg.Done()
	buf := make([]byte, 8192)
	for {
		n, src, err := sc.mcast.ReadFromUDP(buf)
		if err != nil {
			select {
			case <-sc.stop:
				return
			default:
				continue
			}
		}
		if sc.isOwn(src) {
			continue
		}
		m, err := protocol.DecodeScouting(buf[:n])
		if err != nil {
			continue
		}
		scout, ok := m.(*protocol.Scout)
		if !ok {
			continue
		}
		h := sc.answer(src)
		if h == nil || !scout.What.Matches(h.WhatAmI) {
			continue
		}
		var w codec.Writer
		h.Encode(&w)
		_, _ = sc.bestSocket(src.IP).WriteToUDP(w.Bytes(), src)
	}
}

func (sc *scouter) isOwn(src *net.UDPAddr) bool {
	for _, u := range sc.ucast {
		if la, ok := u.conn.LocalAddr().(*net.UDPAddr); ok && la.Port == src.Port &&
			(la.IP.Equal(src.IP) || la.IP.IsUnspecified()) {
			return true
		}
	}
	return false
}

// bestSocket picks the unicast socket whose address shares the longest prefix
// with ip (as zenoh-rust's get_best_match).
func (sc *scouter) bestSocket(ip net.IP) *net.UDPConn {
	best, bestN := sc.ucast[0].conn, -1
	ip4 := ip.To4()
	for _, u := range sc.ucast {
		n := 0
		for n < 4 && ip4 != nil && u.ip.To4() != nil && ip4[n] == u.ip.To4()[n] {
			n++
		}
		if n > bestN {
			best, bestN = u.conn, n
		}
	}
	return best
}

// startScouter runs peer-mode scouting for s: it answers SCOUTs with our
// locators and connects to the routers/peers that answer ours.
func startScouter(s *Session) (*scouter, error) {
	sc, err := newScouter(s.cfg.Scouting, true)
	if err != nil {
		return nil, err
	}
	sc.what = s.cfg.Scouting.Autoconnect
	if s.cfg.Mode != ModeRouter { // routers only answer scouts (zenoh-rust default)
		sc.onHello = func(h Hello) {
			if !h.ZID.Equal(s.zid) && len(h.Locators) > 0 {
				go s.connectScouted(h.ZID, h.Locators)
			}
		}
	}
	sc.answer = func(src *net.UDPAddr) *protocol.Hello {
		return &protocol.Hello{
			Version:  protocol.ProtocolVersion,
			WhatAmI:  s.cfg.Mode.whatAmI(),
			ZID:      s.zid,
			Locators: s.helloLocators(src.IP.IsLoopback()),
		}
	}
	sc.start()
	return sc, nil
}

// helloLocators lists our listen locators for a HELLO; loopback addresses are
// only advertised to scouts coming from this host's loopback.
func (s *Session) helloLocators(loopback bool) []string {
	s.mu.Lock()
	lns := append([]link.Listener(nil), s.listeners...)
	s.mu.Unlock()
	var out []string
	for _, ln := range lns {
		out = append(out, expandLocator(ln.Locator(), loopback)...)
	}
	return out
}

// expandLocator replaces an unspecified listen address (0.0.0.0, [::]) by
// the host's interface addresses.
func expandLocator(l link.Locator, loopback bool) []string {
	host, port, err := net.SplitHostPort(l.Address)
	if err != nil {
		return []string{l.String()}
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsUnspecified() {
		return []string{l.String()}
	}
	v4only := ip.To4() != nil
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		return nil
	}
	var out []string
	for _, a := range addrs {
		ipn, ok := a.(*net.IPNet)
		if !ok || ipn.IP.IsLinkLocalUnicast() || ipn.IP.IsLinkLocalMulticast() {
			continue
		}
		if ipn.IP.IsLoopback() && !loopback {
			continue
		}
		if v4only && ipn.IP.To4() == nil {
			continue
		}
		c := l
		c.Address = net.JoinHostPort(ipn.IP.String(), port)
		out = append(out, c.String())
	}
	return out
}

// -----------------------------------------------------------------------------
// Public scouting API
// -----------------------------------------------------------------------------

// Scouter is a running scout; see Scout.
type Scouter struct{ sc *scouter }

// Stop ends scouting.
func (s *Scouter) Stop() { s.sc.close() }

// Scout multicasts SCOUT messages for nodes of the given roles and calls
// handler for every HELLO received, until Stop. The same node usually answers
// several times.
func Scout(what WhatAmIMatcher, cfg ScoutingConfig, handler func(Hello)) (*Scouter, error) {
	if cfg.Address == "" {
		cfg.Address = "224.0.0.224:7446"
	}
	if what == 0 {
		what = MatchRouter | MatchPeer
	}
	sc, err := newScouter(cfg, false)
	if err != nil {
		return nil, err
	}
	sc.what = what
	sc.onHello = handler
	sc.start()
	return &Scouter{sc: sc}, nil
}

// errNoMulticastListen: the platform cannot receive on a multicast group.
var errNoMulticastListen = errors.New("zenoh: multicast receive unsupported on this platform")
