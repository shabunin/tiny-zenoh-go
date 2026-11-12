package zenoh

import (
	"sync"
	"sync/atomic"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
)

// Route caching. Working out where data on a key goes walks every face's
// subscribers and the mesh trees under s.mu; the result only changes when
// routing state does. So the routes are cached per key, and anything that
// changes what routing reads (faces coming and going, their declarations,
// link states, gateways, our own subscribers) calls routesChanged, which
// bumps s.routeGen and so retires every cached route at once.
//
// routesChanged comes after the change, never before: a route computed (under
// s.mu) while the generation is still the old one sees either the old state,
// and is retired by the bump, or the new one, and is right anyway.

// maxCachedRoutes bounds each cache; a full cache starts over.
const maxCachedRoutes = 1024

// subsIndexMin is the number of local subscribers from which the matching
// ones are cached per key instead of found by a scan.
const subsIndexMin = 8

// routesChanged retires the cached routes.
func (s *Session) routesChanged() { s.routeGen.Add(1) }

// meshHop is a neighbour router to send to, with the tree to tag data with.
type meshHop struct {
	f    *face
	node uint16
}

// dataRoute is where data on one key from one source goes.
type dataRoute struct {
	drop    bool      // unknown mesh NodeId: forward nothing
	faces   []*face   // our faces whose subscribers want it
	routers []meshHop // neighbour routers on the way to other subscribers
}

type routeKey struct {
	node uint16 // the mesh NodeId the data came with
	key  string
}

// routeCache maps keys to routes of one generation. Data with NodeId 0
// (all but what routers pass along a mesh tree) is looked up by its key
// alone, the faster map.
type routeCache[V any] struct {
	mu    sync.RWMutex
	gen   uint64
	m     map[string]V
	nodes map[routeKey]V
}

func (c *routeCache[V]) get(k routeKey, gen uint64) (v V, ok bool) {
	c.mu.RLock()
	if c.gen == gen {
		if k.node == 0 {
			v, ok = c.m[k.key]
		} else {
			v, ok = c.nodes[k]
		}
	}
	c.mu.RUnlock()
	return v, ok
}

func (c *routeCache[V]) put(k routeKey, gen uint64, v V) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if gen < c.gen {
		return // computed before a change another one was computed after
	}
	if gen > c.gen || len(c.m)+len(c.nodes) >= maxCachedRoutes {
		c.m, c.nodes = nil, nil
		c.gen = gen
	}
	if k.node == 0 {
		if c.m == nil {
			c.m = make(map[string]V)
		}
		c.m[k.key] = v
	} else {
		if c.nodes == nil {
			c.nodes = make(map[routeKey]V)
		}
		c.nodes[k] = v
	}
}

// genRoute is a route with the generation it is valid for.
type genRoute struct {
	gen uint64
	r   *dataRoute
}

// publishRoute is where our own data on key goes. slot, if not nil, is the
// publisher's own cache of it.
func (s *Session) publishRoute(key string, slot *atomic.Pointer[genRoute]) *dataRoute {
	gen := s.routeGen.Load()
	if slot != nil {
		if g := slot.Load(); g != nil && g.gen == gen {
			return g.r
		}
	}
	rk := routeKey{key: key}
	if r, ok := s.pubRoutes.get(rk, gen); ok {
		if slot != nil {
			slot.Store(&genRoute{gen, r})
		}
		return r
	}
	s.mu.Lock()
	gen = s.routeGen.Load()
	r := &dataRoute{}
	for _, f := range s.faces {
		if f.wantsDataLocked(key) {
			r.faces = append(r.faces, f)
		}
	}
	routers, _ := s.meshRouteLocked(kindSub, nil, 0, key, 0)
	for f, node := range routers {
		r.routers = append(r.routers, meshHop{f, node})
	}
	s.mu.Unlock()
	s.pubRoutes.put(rk, gen, r)
	if slot != nil {
		slot.Store(&genRoute{gen, r})
	}
	return r
}

// forwardRoute is where data on key that src brought (with mesh NodeId node)
// goes on to.
func (s *Session) forwardRoute(src *face, node uint16, key string) *dataRoute {
	rk := routeKey{node: node, key: key}
	gen := s.routeGen.Load()
	if r, ok := src.routes.get(rk, gen); ok {
		return r
	}
	s.mu.Lock()
	gen = s.routeGen.Load()
	r := &dataRoute{}
	routers, ok := s.meshRouteLocked(kindSub, src, node, key, 0)
	if !ok {
		r.drop = true
	} else {
		tree, _ := s.meshSourceLocked(src, node)
		origin := s.originOf(src, tree)
		for _, f := range s.faces {
			if f.live && f.wantsDataLocked(key) && s.forwardsLocked(src, f, origin) {
				r.faces = append(r.faces, f)
			}
		}
		for f, n := range routers {
			r.routers = append(r.routers, meshHop{f, n})
		}
	}
	s.mu.Unlock()
	src.routes.put(rk, gen, r)
	return r
}

// subsChangedLocked republishes the list of our subscribers after s.subs
// changed.
func (s *Session) subsChangedLocked() {
	list := make([]*Subscriber, 0, len(s.subs))
	for _, sub := range s.subs {
		list = append(list, sub)
	}
	s.subList.Store(&list)
	s.routesChanged()
}

// localSubs lists our subscribers matching full key (whatever their allowed
// origin), without taking s.mu.
func (s *Session) localSubs(full string, buf []*Subscriber) []*Subscriber {
	lp := s.subList.Load()
	if lp == nil {
		return buf
	}
	list := *lp
	if len(list) < subsIndexMin {
		for _, sub := range list {
			if keyexpr.Intersect(sub.key.String(), full) {
				buf = append(buf, sub)
			}
		}
		return buf
	}
	rk := routeKey{key: full}
	gen := s.routeGen.Load()
	if subs, ok := s.subRoutes.get(rk, gen); ok {
		return append(buf, subs...)
	}
	// The list is read again after the generation: it is at least as new.
	list = *s.subList.Load()
	var subs []*Subscriber
	for _, sub := range list {
		if keyexpr.Intersect(sub.key.String(), full) {
			subs = append(subs, sub)
		}
	}
	s.subRoutes.put(rk, gen, subs)
	return append(buf, subs...)
}
