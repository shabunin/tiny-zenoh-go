package zenoh

import (
	"strconv"
	"strings"
)

// Admin space: like zenohd, a router answers "@/<zid>/router" with a JSON
// description of itself and its sessions.

func (s *Session) startAdmin() error {
	key := "@/" + s.zid.String() + "/router"
	k, err := validKey(key)
	if err != nil {
		return err
	}
	_, err = s.declareQueryable(k, func(q *Query) {
		_ = q.Reply(key, []byte(s.adminJSON()), WithEncoding(EncodingApplicationJSON))
	}, true, nil) // not in the namespace, like zenohd's admin space
	return err
}

func jsonString(v string) string { return strconv.Quote(v) } // keys/zids/locators are ASCII

func (s *Session) adminJSON() string {
	var b strings.Builder
	b.WriteString(`{"zid":` + jsonString(s.zid.String()))
	b.WriteString(`,"whatami":` + jsonString(s.cfg.Mode.String()))
	b.WriteString(`,"locators":[`)
	for i, l := range s.Locators() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(jsonString(l))
	}
	b.WriteString(`],"sessions":[`)
	for i, f := range s.faceList() {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(`{"peer":` + jsonString(f.zid.String()) + `,"whatami":` + jsonString(f.whatami.String()) + `}`)
	}
	b.WriteString(`]}`)
	return b.String()
}
