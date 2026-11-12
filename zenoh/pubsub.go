package zenoh

import (
	"sync"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// dispatchSample hands a sample on full key smp.KeyExpr, published by our
// session (local) or another one, to our matching subscribers.
func (s *Session) dispatchSample(smp Sample, local bool) {
	full := smp.KeyExpr
	user, ok := s.nsStrip(full)
	if !ok {
		return // outside our namespace
	}
	smp.KeyExpr = user
	s.mu.Lock()
	var buf [8]func(Sample)
	handlers := buf[:0]
	for _, sub := range s.subs {
		if sub.origin.allows(local) && keyexpr.Intersect(sub.key.String(), full) {
			handlers = append(handlers, sub.handler)
		}
	}
	s.mu.Unlock()
	for _, h := range handlers {
		h(smp)
	}
}

// -----------------------------------------------------------------------------
// Put / Delete
// -----------------------------------------------------------------------------

func (s *Session) buildData(kind SampleKind, o *putOptions) protocol.DataBody {
	ts := o.timestamp
	if ts == nil && (o.autoTS || s.tsOn) {
		t := s.NewTimestamp()
		ts = &t
	}
	if kind == SampleKindDelete {
		return &protocol.Del{Timestamp: ts, Attachment: o.attachment, SourceInfo: o.sourceInfo}
	}
	p := &protocol.Put{Timestamp: ts, Encoding: o.encoding.wire(), Attachment: o.attachment,
		SourceInfo: o.sourceInfo}
	if o.shmBuf != nil {
		p.ShmBuf = o.shmBuf
	}
	return p
}

func (s *Session) publish(key string, we protocol.WireExpr, kind SampleKind, payload []byte, o *putOptions) error {
	if s.closed.Load() {
		return ErrSessionClosed
	}
	body := s.buildData(kind, o)
	if p, ok := body.(*protocol.Put); ok {
		p.Payload = o.payloadOf(payload)
	}
	// Deliver to this session's own matching subscribers, like zenoh-rust.
	if o.destination != LocalityRemote {
		smp := sampleFromBody(key, body, o.qos())
		smp.Payload = o.localPayload(smp.Payload)
		s.dispatchSample(smp, true)
	}
	if o.destination == LocalitySessionLocal {
		return nil
	}

	s.mu.Lock()
	var buf [8]*face
	dst := buf[:0]
	for _, f := range s.faces {
		if f.wantsDataLocked(key) {
			dst = append(dst, f)
		}
	}
	routers, _ := s.meshRouteLocked(kindSub, nil, 0, key, 0)
	for f := range routers {
		dst = append(dst, f)
	}
	s.mu.Unlock()
	s.mcastPublish(key, o.qos(), body, o.reliability == Reliable)
	msg := &protocol.Push{WireExpr: we, QoS: o.qos(), Body: body}
	var first error
	for _, f := range dst {
		if err := f.send(o.reliability == Reliable, msg); err != nil && first == nil {
			first = err
		}
	}
	if s.cfg.Mode == ModeClient {
		if first != nil && s.closed.Load() {
			return ErrSessionClosed
		}
		return first
	}
	return nil
}

// Put publishes payload on key.
func (s *Session) Put(key string, payload []byte, opts ...PutOption) error {
	k, err := s.nsKey(key)
	if err != nil {
		return err
	}
	o := defaultPutOptions()
	for _, opt := range opts {
		opt.applyPut(&o)
	}
	return s.publish(k.String(), protocol.WireExpr{Suffix: k.String()}, SampleKindPut, payload, &o)
}

// Delete publishes a deletion on key.
func (s *Session) Delete(key string, opts ...PutOption) error {
	k, err := s.nsKey(key)
	if err != nil {
		return err
	}
	o := defaultPutOptions()
	for _, opt := range opts {
		opt.applyPut(&o)
	}
	return s.publish(k.String(), protocol.WireExpr{Suffix: k.String()}, SampleKindDelete, nil, &o)
}

// -----------------------------------------------------------------------------
// Subscriber
// -----------------------------------------------------------------------------

// Subscriber receives samples published on a key expression.
type Subscriber struct {
	s       *Session
	id      uint32
	key     *keyexpr.Key // full key (namespace included)
	origin  Locality
	handler func(Sample)
	stop    chan struct{} // closed by Undeclare
	once    sync.Once
}

// KeyExpr returns the subscriber's key expression.
func (sub *Subscriber) KeyExpr() string { return sub.s.userKey(sub.key.String()) }

// remote reports whether the entity is declared to the network.
func (sub *Subscriber) remote() bool { return sub.origin != LocalitySessionLocal }

// DeclareSubscriber calls handler for every sample published on keys
// intersecting key (see WithAllowedOrigin).
func (s *Session) DeclareSubscriber(key string, handler func(Sample), opts ...SubscriberOption) (*Subscriber, error) {
	k, err := s.nsKey(key)
	if err != nil {
		return nil, err
	}
	sub := &Subscriber{s: s, id: s.entityID(), key: k, handler: handler, stop: make(chan struct{})}
	for _, o := range opts {
		o.applySub(sub)
	}
	s.mu.Lock()
	s.subs[sub.id] = sub
	if !sub.remote() {
		s.mu.Unlock()
		s.checkMatching()
		return sub, nil
	}
	err = s.declareLocalLocked(kindSub, k.String(), &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.DeclareSubscriber{ID: sub.id, WireExpr: protocol.WireExpr{Suffix: k.String()}}})
	if err != nil {
		s.mu.Lock()
		delete(s.subs, sub.id)
		s.mu.Unlock()
		return nil, err
	}
	return sub, nil
}

// DeclareSubscriberChan is like DeclareSubscriber but delivers samples on an
// unbounded channel, closed by Undeclare or when the session ends.
func (s *Session) DeclareSubscriberChan(key string, opts ...SubscriberOption) (*Subscriber, <-chan Sample, error) {
	f := newFifo[Sample]()
	sub, err := s.DeclareSubscriber(key, f.push, opts...)
	if err != nil {
		f.close()
		return nil, nil, err
	}
	go func() {
		select {
		case <-s.done:
		case <-sub.stop:
		}
		f.close()
	}()
	return sub, f.out, nil
}

// Undeclare stops the subscriber.
func (sub *Subscriber) Undeclare() error {
	s := sub.s
	s.mu.Lock()
	_, ok := s.subs[sub.id]
	delete(s.subs, sub.id)
	if !ok {
		s.mu.Unlock()
		return nil
	}
	sub.once.Do(func() { close(sub.stop) })
	if !sub.remote() {
		s.mu.Unlock()
		s.checkMatching()
		return nil
	}
	return s.declareLocalLocked(kindSub, sub.key.String(), &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.UndeclareSubscriber{ID: sub.id}})
}

// -----------------------------------------------------------------------------
// Publisher
// -----------------------------------------------------------------------------

// Publisher publishes repeatedly on one key expression. It declares a numeric
// key-expression id to every connected node so each message carries a few
// bytes of scope instead of the full key.
type Publisher struct {
	s      *Session
	key    string // full key (namespace included)
	exprID uint16
	opts   []PutOption
	base   putOptions // the defaults with opts applied
	m      *matcher
}

// DeclarePublisher declares a publisher on key; opts become its defaults.
func (s *Session) DeclarePublisher(key string, opts ...PutOption) (*Publisher, error) {
	k, err := s.nsKey(key)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.nextExpr++
	id := s.nextExpr
	s.exprMu.Lock()
	s.localExprs[id] = k.String()
	s.exprMu.Unlock()
	faces := s.faceListLocked()
	s.mu.Unlock()
	err = s.broadcast(faces, &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.DeclareKeyExpr{ID: id, WireExpr: protocol.WireExpr{Suffix: k.String()}}})
	if err != nil {
		return nil, err
	}
	o := defaultPutOptions()
	for _, opt := range opts {
		opt.applyPut(&o)
	}
	m, err := s.newMatcher(kindSub, k.String(), false, o.destination)
	if err != nil {
		return nil, err
	}
	return &Publisher{s: s, key: k.String(), exprID: id, opts: opts,
		base: withPutOptions(defaultPutOptions(), opts), m: m}, nil
}

// KeyExpr returns the publisher's key expression.
func (p *Publisher) KeyExpr() string { return p.s.userKey(p.key) }

func (p *Publisher) options(opts []PutOption) putOptions {
	if len(opts) == 0 {
		return p.base
	}
	return withPutOptions(p.base, opts)
}

// withPutOptions applies opts to o (apart, so that a put without options
// doesn't pay for o escaping into the option interfaces).
func withPutOptions(o putOptions, opts []PutOption) putOptions {
	for _, opt := range opts {
		opt.applyPut(&o)
	}
	return o
}

func (p *Publisher) wire() protocol.WireExpr {
	return protocol.WireExpr{Scope: p.exprID, Mapping: true}
}

// Put publishes payload.
func (p *Publisher) Put(payload []byte, opts ...PutOption) error {
	o := p.options(opts)
	return p.s.publish(p.key, p.wire(), SampleKindPut, payload, &o)
}

// Delete publishes a deletion.
func (p *Publisher) Delete(opts ...PutOption) error {
	o := p.options(opts)
	return p.s.publish(p.key, p.wire(), SampleKindDelete, nil, &o)
}

// Undeclare releases the publisher's key-expression id and its matching
// listeners.
func (p *Publisher) Undeclare() error {
	if ok, err := p.m.close(); !ok || err != nil {
		return err
	}
	s := p.s
	s.mu.Lock()
	s.exprMu.Lock()
	delete(s.localExprs, p.exprID)
	s.exprMu.Unlock()
	faces := s.faceListLocked()
	s.mu.Unlock()
	return s.broadcast(faces, &protocol.Declare{QoS: protocol.QoSDeclare,
		Body: &protocol.UndeclareKeyExpr{ID: p.exprID}})
}
