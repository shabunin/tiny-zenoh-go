package zenoh

import (
	"fmt"

	"github.com/shabunin/tiny-zenoh-go/keyexpr"
)

// Namespaces (zenoh-rust net/routing/namespace.rs, config "namespace"): a
// session with Config.Namespace "ns" works on "ns/<key>" for every key it is
// given (puts, subscribers, queries, replies, tokens...), and gives back keys
// with "ns/" stripped. Traffic for it on keys with nothing under the
// namespace is dropped. Inside the session everything uses the full keys;
// the conversion happens at the API.
//
// Routing between other nodes is not affected, and neither is the admin space.

func validNamespace(ns string) error {
	if ns != "" && !keyexpr.IsNonWild(ns) {
		return fmt.Errorf("zenoh: invalid namespace %q: must be a key expression without wildcards", ns)
	}
	return nil
}

// nsKey validates a user key expression and puts it in the namespace.
func (s *Session) nsKey(key string) (*keyexpr.Key, error) {
	k, err := validKey(key)
	if err != nil || s.cfg.Namespace == "" {
		return k, err
	}
	return keyexpr.New(s.cfg.Namespace + "/" + k.String())
}

// nsStrip gives the user's view of full key expression key; ok is false if
// key has nothing under the namespace.
func (s *Session) nsStrip(key string) (string, bool) {
	if s.cfg.Namespace == "" {
		return key, true
	}
	return keyexpr.StripNonWildPrefix(key, s.cfg.Namespace)
}

// userKey is nsStrip for keys known to be in the namespace (our own).
func (s *Session) userKey(key string) string {
	if k, ok := s.nsStrip(key); ok {
		return k
	}
	return key
}
