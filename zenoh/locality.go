package zenoh

// Locality says which entities a publication or query may reach, or which
// ones a subscriber or queryable accepts traffic from (zenoh-rust
// sample::Locality): those of the same session, remote ones, or both.
type Locality int

const (
	// LocalityAny: local and remote entities (the default).
	LocalityAny Locality = iota
	// LocalitySessionLocal: entities of the same session only. Such a
	// subscriber or queryable is not declared to the network.
	LocalitySessionLocal
	// LocalityRemote: entities of other sessions only.
	LocalityRemote
)

func (l Locality) String() string {
	switch l {
	case LocalitySessionLocal:
		return "SessionLocal"
	case LocalityRemote:
		return "Remote"
	}
	return "Any"
}

// allows reports whether traffic between our session and a local (true) or
// remote (false) peer entity is allowed.
func (l Locality) allows(local bool) bool {
	return l == LocalityAny || local == (l == LocalitySessionLocal)
}

// SubscriberOption customizes a subscriber.
type SubscriberOption interface{ applySub(*Subscriber) }

// OriginOption restricts where a subscriber or queryable takes traffic from;
// see WithAllowedOrigin.
type OriginOption struct{ l Locality }

func (o OriginOption) applySub(s *Subscriber) { s.origin = o.l }
func (o OriginOption) applyQbl(q *Queryable)  { q.origin = o.l }

// WithAllowedOrigin restricts a subscriber or queryable to publications or
// queries of its own session (LocalitySessionLocal), of other sessions
// (LocalityRemote) or both (LocalityAny, the default).
func WithAllowedOrigin(l Locality) OriginOption { return OriginOption{l} }

// DestinationOption restricts where a publication or query goes; see
// WithAllowedDestination.
type DestinationOption struct{ l Locality }

func (o DestinationOption) applyPut(p *putOptions) { p.destination = o.l }
func (o DestinationOption) applyGet(g *getOptions) { g.destination = o.l }

// WithAllowedDestination restricts a put, delete, publisher, get or querier
// to the subscribers / queryables of its own session (LocalitySessionLocal),
// of other sessions (LocalityRemote) or both (LocalityAny, the default).
// Matching status follows the same restriction. It has no effect on query
// replies.
func WithAllowedDestination(l Locality) DestinationOption { return DestinationOption{l} }
