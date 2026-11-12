package zenoh

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
	"github.com/shabunin/tiny-zenoh-go/link"
	"github.com/shabunin/tiny-zenoh-go/protocol"
	"github.com/shabunin/tiny-zenoh-go/transport"
)

// Access control (zenoh-rust zenoh/src/net/routing/interceptor/
// access_control.rs and authorization.rs, config section "access_control").
//
// Rules say which messages (put, query, declare_subscriber, ...) on which key
// expressions are allowed or denied, in which direction (ingress: received
// from a node; egress: sent to it). Subjects describe nodes by the interface
// their link uses, the common name of their TLS certificate, the user name
// they authenticated with, the link protocol and their ZID. Policies apply
// rules to subjects.
//
// A message is checked against every subject the node matches: it is
// allowed as soon as one subject allows it, and gets the default permission
// when the node matches no subject. For one subject, a deny rule whose key
// expression includes the message's key wins; otherwise the message is
// allowed if the default is allow or an allow rule includes the key.

// ACLMessage is a kind of message rules apply to.
type ACLMessage string

// Message kinds.
const (
	ACLPut                         ACLMessage = "put"
	ACLDelete                      ACLMessage = "delete"
	ACLDeclareSubscriber           ACLMessage = "declare_subscriber"
	ACLQuery                       ACLMessage = "query"
	ACLDeclareQueryable            ACLMessage = "declare_queryable"
	ACLReply                       ACLMessage = "reply"
	ACLLivelinessToken             ACLMessage = "liveliness_token"
	ACLDeclareLivelinessSubscriber ACLMessage = "declare_liveliness_subscriber"
	ACLLivelinessQuery             ACLMessage = "liveliness_query"
)

var aclMessages = []ACLMessage{ACLPut, ACLDelete, ACLDeclareSubscriber, ACLQuery, ACLDeclareQueryable,
	ACLReply, ACLLivelinessToken, ACLDeclareLivelinessSubscriber, ACLLivelinessQuery}

// ACLFlow is a message direction, seen from this node.
type ACLFlow string

// Directions.
const (
	ACLIngress ACLFlow = "ingress" // messages received from the node
	ACLEgress  ACLFlow = "egress"  // messages sent to the node
)

// ACLPermission is allow or deny.
type ACLPermission string

// Permissions.
const (
	ACLAllow ACLPermission = "allow"
	ACLDeny  ACLPermission = "deny"
)

// ACLConfig configures access control. Field names follow zenoh-rust's
// access_control config section, so a JSON copy of it decodes into this.
type ACLConfig struct {
	// DefaultPermission applies to messages no rule decides. Default deny.
	DefaultPermission ACLPermission `json:"default_permission"`
	Rules             []ACLRule     `json:"rules"`
	Subjects          []ACLSubject  `json:"subjects"`
	Policies          []ACLPolicy   `json:"policies"`
}

// ACLRule allows or denies messages on key expressions.
type ACLRule struct {
	ID         string        `json:"id"`
	Messages   []ACLMessage  `json:"messages"`
	Flows      []ACLFlow     `json:"flows"` // default both
	Permission ACLPermission `json:"permission"`
	KeyExprs   []string      `json:"key_exprs"`
}

// ACLSubject describes nodes. Each list left empty matches anything; a node
// matches if it matches one value of every non-empty list.
type ACLSubject struct {
	ID              string   `json:"id"`
	Interfaces      []string `json:"interfaces"`
	CertCommonNames []string `json:"cert_common_names"`
	Usernames       []string `json:"usernames"`
	LinkProtocols   []string `json:"link_protocols"`
	ZIDs            []string `json:"zids"`
}

// ACLPolicy applies rules to subjects (by id).
type ACLPolicy struct {
	ID       string   `json:"id,omitempty"`
	Rules    []string `json:"rules"`
	Subjects []string `json:"subjects"`
}

const nSubjProps = 5 // interface, cert common name, username, link protocol, zid

// aclSubject is one combination of subject properties; "" is a wildcard.
type aclSubject [nSubjProps]string

func (s aclSubject) matches(q aclSubject) bool {
	for i := range s {
		if s[i] != "" && s[i] != q[i] {
			return false
		}
	}
	return true
}

type aclKeys struct{ allow, deny []string }

type aclPolicy [2][]aclKeys // flow (0 ingress, 1 egress) -> message index

type aclEnforcer struct {
	allowDefault    bool
	subjects        []aclSubject // id = index
	policy          map[int]*aclPolicy
	ingress, egress bool // whether checks are needed at all in each direction
}

func flowIndex(f ACLFlow) int {
	if f == ACLEgress {
		return 1
	}
	return 0
}

func msgIndex(m ACLMessage) int {
	for i, x := range aclMessages {
		if x == m {
			return i
		}
	}
	return -1
}

// newACL compiles the configuration, validating it as zenoh-rust does.
func newACL(c *ACLConfig) (*aclEnforcer, error) {
	e := &aclEnforcer{policy: map[int]*aclPolicy{}}
	switch c.DefaultPermission {
	case ACLAllow:
		e.allowDefault = true
	case ACLDeny, "":
	default:
		return nil, fmt.Errorf("zenoh: acl: bad default permission %q", c.DefaultPermission)
	}
	if c.Rules == nil || c.Subjects == nil || c.Policies == nil {
		return nil, errors.New("zenoh: acl: rules, subjects and policies must all be given")
	}
	if len(c.Rules) == 0 || len(c.Subjects) == 0 || len(c.Policies) == 0 {
		e.ingress, e.egress = !e.allowDefault, !e.allowDefault
		return e, nil
	}
	rules := map[string]*ACLRule{}
	for i := range c.Rules {
		r := &c.Rules[i]
		if strings.TrimSpace(r.ID) == "" {
			return nil, errors.New("zenoh: acl: empty rule id")
		}
		if rules[r.ID] != nil {
			return nil, fmt.Errorf("zenoh: acl: rule id %q is repeated", r.ID)
		}
		if r.Permission != ACLAllow && r.Permission != ACLDeny {
			return nil, fmt.Errorf("zenoh: acl: rule %q: bad permission %q", r.ID, r.Permission)
		}
		for _, m := range r.Messages {
			if msgIndex(m) < 0 {
				return nil, fmt.Errorf("zenoh: acl: rule %q: unknown message %q", r.ID, m)
			}
		}
		for _, f := range r.Flows {
			if f != ACLIngress && f != ACLEgress {
				return nil, fmt.Errorf("zenoh: acl: rule %q: unknown flow %q", r.ID, f)
			}
		}
		for _, k := range r.KeyExprs {
			if err := keyexpr.Validate(k); err != nil {
				return nil, fmt.Errorf("zenoh: acl: rule %q: %w", r.ID, err)
			}
		}
		rules[r.ID] = r
	}
	ids := map[string]aclSubject{}
	combos := map[string][]int{}
	for _, s := range c.Subjects {
		if strings.TrimSpace(s.ID) == "" {
			return nil, errors.New("zenoh: acl: empty subject id")
		}
		if _, dup := combos[s.ID]; dup {
			return nil, fmt.Errorf("zenoh: acl: subject id %q is repeated", s.ID)
		}
		props := [nSubjProps][]string{s.Interfaces, s.CertCommonNames, s.Usernames, s.LinkProtocols, s.ZIDs}
		for i, vals := range props {
			for _, v := range vals {
				if strings.TrimSpace(v) == "" {
					return nil, fmt.Errorf("zenoh: acl: empty value in subject %q", s.ID)
				}
			}
			if len(vals) == 0 {
				props[i] = []string{""}
			}
		}
		for j, z := range props[4] {
			props[4][j] = strings.ToLower(z)
		}
		// Every combination of the listed values is one subject.
		var list []int
		var walk func(i int, cur aclSubject)
		walk = func(i int, cur aclSubject) {
			if i == nSubjProps {
				key := strings.Join(cur[:], "\x00")
				if _, ok := ids[key]; !ok {
					ids[key] = cur
					e.subjects = append(e.subjects, cur)
				}
				for id, x := range e.subjects {
					if x == cur {
						list = append(list, id)
						break
					}
				}
				return
			}
			for _, v := range props[i] {
				cur[i] = v
				walk(i+1, cur)
			}
		}
		walk(0, aclSubject{})
		combos[s.ID] = list
	}
	policyIDs := map[string]bool{}
	for n, p := range c.Policies {
		if p.ID != "" {
			if policyIDs[p.ID] {
				return nil, fmt.Errorf("zenoh: acl: policy id %q is repeated", p.ID)
			}
			policyIDs[p.ID] = true
		}
		if len(p.Rules) == 0 || len(p.Subjects) == 0 {
			return nil, fmt.Errorf("zenoh: acl: policy #%d has no rules or no subjects", n)
		}
		for _, sid := range p.Subjects {
			if _, ok := combos[sid]; !ok {
				return nil, fmt.Errorf("zenoh: acl: policy #%d: unknown subject %q", n, sid)
			}
		}
		for _, rid := range p.Rules {
			r := rules[rid]
			if r == nil {
				return nil, fmt.Errorf("zenoh: acl: policy #%d: unknown rule %q", n, rid)
			}
			flows := r.Flows
			if len(flows) == 0 {
				flows = []ACLFlow{ACLIngress, ACLEgress}
			}
			for _, sid := range p.Subjects {
				for _, id := range combos[sid] {
					pol := e.policy[id]
					if pol == nil {
						pol = &aclPolicy{make([]aclKeys, len(aclMessages)), make([]aclKeys, len(aclMessages))}
						e.policy[id] = pol
					}
					for _, f := range flows {
						fi := flowIndex(f)
						if e.allowDefault {
							if fi == 0 {
								e.ingress = true
							} else {
								e.egress = true
							}
						} else {
							e.ingress, e.egress = true, true
						}
						for _, m := range r.Messages {
							k := &pol[fi][msgIndex(m)]
							if r.Permission == ACLAllow {
								k.allow = append(k.allow, r.KeyExprs...)
							} else {
								k.deny = append(k.deny, r.KeyExprs...)
							}
						}
					}
				}
			}
		}
	}
	return e, nil
}

// subjectsOf lists the subject ids a node matches (zenoh-rust
// new_transport_unicast: every combination of its identities).
func (e *aclEnforcer) subjectsOf(q []aclSubject) []int {
	seen := map[int]bool{}
	var out []int
	for _, x := range q {
		for id, s := range e.subjects {
			if !seen[id] && s.matches(x) {
				seen[id] = true
				out = append(out, id)
			}
		}
	}
	return out
}

func anyIncludes(rules []string, key string) bool {
	for _, r := range rules {
		if keyexpr.Include(r, key) {
			return true
		}
	}
	return false
}

// decide is zenoh-rust policy_decision_point for one subject.
func (e *aclEnforcer) decide(subject int, flow, msg int, key string) bool {
	if len(e.policy) == 0 {
		return e.allowDefault
	}
	pol := e.policy[subject]
	if pol == nil {
		return e.allowDefault
	}
	k := pol[flow][msg]
	if anyIncludes(k.deny, key) {
		return false
	}
	return e.allowDefault || anyIncludes(k.allow, key)
}

// allowed is zenoh-rust's action(): any subject allowing wins.
func (e *aclEnforcer) allowed(subjects []int, flow, msg int, key string) bool {
	if len(subjects) == 0 {
		return e.allowDefault
	}
	for _, s := range subjects {
		if e.decide(s, flow, msg, key) {
			return true
		}
	}
	return false
}

// faceACL is the access control state of one face.
type faceACL struct {
	e        *aclEnforcer
	subjects []int
	mu       sync.Mutex
	cache    map[aclCacheKey]bool
}

type aclCacheKey struct {
	flow, msg int
	key       string
}

const aclCacheMax = 4096

func (a *faceACL) check(flow, msg int, key string) bool {
	if (flow == 0 && !a.e.ingress) || (flow == 1 && !a.e.egress) {
		return true
	}
	ck := aclCacheKey{flow, msg, key}
	a.mu.Lock()
	ok, hit := a.cache[ck]
	a.mu.Unlock()
	if hit {
		return ok
	}
	ok = a.e.allowed(a.subjects, flow, msg, key)
	a.mu.Lock()
	if len(a.cache) >= aclCacheMax {
		clear(a.cache)
	}
	a.cache[ck] = ok
	a.mu.Unlock()
	return ok
}

// setupACL works out which subjects the node behind f matches.
func (s *Session) setupACL(f *face, l transport.Link, tr *transport.Unicast) {
	if s.acl == nil {
		return
	}
	user, cn := tr.AuthUser(), link.PeerCommonName(l)
	ifaces := link.LocalInterfaces(l)
	if len(ifaces) == 0 {
		ifaces = []string{""}
	}
	var q []aclSubject
	for _, ifc := range ifaces {
		q = append(q, aclSubject{ifc, cn, user, f.proto, strings.ToLower(f.zid.String())})
	}
	f.acl = &faceACL{e: s.acl, subjects: s.acl.subjectsOf(q), cache: map[aclCacheKey]bool{}}
}

// aclKind classifies a message for access control: its message index and
// wire expression, or ok=false for messages access control lets through.
func aclKind(m any) (msg int, we *protocol.WireExpr, undecl bool, ok bool) {
	switch m := m.(type) {
	case *protocol.Push:
		if _, del := m.Body.(*protocol.Del); del {
			return msgIndex(ACLDelete), &m.WireExpr, false, true
		}
		return msgIndex(ACLPut), &m.WireExpr, false, true
	case *protocol.Request:
		return msgIndex(ACLQuery), &m.WireExpr, false, true
	case *protocol.Response:
		return msgIndex(ACLReply), &m.WireExpr, false, true
	case *protocol.Declare:
		switch d := m.Body.(type) {
		case *protocol.DeclareSubscriber:
			return msgIndex(ACLDeclareSubscriber), &d.WireExpr, false, true
		case *protocol.UndeclareSubscriber:
			return msgIndex(ACLDeclareSubscriber), &d.WireExpr, true, true
		case *protocol.DeclareQueryable:
			return msgIndex(ACLDeclareQueryable), &d.WireExpr, false, true
		case *protocol.UndeclareQueryable:
			return msgIndex(ACLDeclareQueryable), &d.WireExpr, true, true
		case *protocol.DeclareToken:
			return msgIndex(ACLLivelinessToken), &d.WireExpr, false, true
		case *protocol.UndeclareToken:
			return msgIndex(ACLLivelinessToken), &d.WireExpr, true, true
		}
	case *protocol.Interest:
		if m.Options&protocol.InterestTokens == 0 {
			return 0, nil, false, false
		}
		switch m.Mode {
		case protocol.InterestCurrent:
			return msgIndex(ACLLivelinessQuery), m.WireExpr, false, true
		case protocol.InterestFuture, protocol.InterestCurrentFuture:
			return msgIndex(ACLDeclareLivelinessSubscriber), m.WireExpr, false, true
		}
	}
	return 0, nil, false, false
}

// aclIngress reports whether message m received on f may be processed.
func (f *face) aclIngress(m protocol.Message) bool {
	a := f.acl
	if a == nil || !a.e.ingress {
		return true
	}
	msg, we, undecl, ok := aclKind(m)
	if !ok {
		return true
	}
	if we == nil {
		return false // zenoh-rust: no key expression, no access
	}
	if undecl && we.Scope == 0 && we.Suffix == "" {
		return true // undeclaring by id only: harmless if the declaration was refused
	}
	key, ok := f.resolve(*we)
	if !ok {
		return false
	}
	return a.check(0, msg, key)
}

// aclEgress drops from msgs those that may not be sent to f.
func (f *face) aclEgress(msgs []transport.Encoder) []transport.Encoder {
	a := f.acl
	if a == nil || !a.e.egress {
		return msgs
	}
	out := msgs[:0:0]
	for _, m := range msgs {
		msg, we, undecl, ok := aclKind(m)
		if !ok {
			out = append(out, m)
			continue
		}
		if we == nil {
			f.statsDrop(transport.Tx, dropACL, m)
			f.refused(m)
			continue
		}
		if undecl && we.Scope == 0 && we.Suffix == "" {
			out = append(out, m)
			continue
		}
		key, ok := f.resolveOwn(*we)
		if ok && a.check(1, msg, key) {
			out = append(out, m)
		} else {
			f.statsDrop(transport.Tx, dropACL, m)
			f.refused(m)
		}
	}
	return out
}

// resolveOwn turns a wire expression we send to f into a full key
// expression: M=1 means we declared the scope, M=0 that f did.
func (f *face) resolveOwn(we protocol.WireExpr) (string, bool) {
	if we.Scope == 0 {
		return we.Suffix, true
	}
	f.s.exprMu.RLock()
	defer f.s.exprMu.RUnlock()
	var prefix string
	var ok bool
	if we.Mapping {
		prefix, ok = f.s.localExprs[we.Scope]
	} else {
		prefix, ok = f.remoteExprs[we.Scope]
	}
	return prefix + we.Suffix, ok
}
