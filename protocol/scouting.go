package protocol

import (
	"errors"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// Scouting message ids. Scouting messages travel alone in a UDP datagram
// (no length prefix, no batching), usually to the multicast group
// 224.0.0.224:7446.
const (
	IDScout byte = 0x01
	IDHello byte = 0x02
)

const (
	flagScoutI = 0x08 // in the flags byte: a ZID follows
	flagHelloL = 0x20 // in the header: a locator list follows
)

// WhatAmIMatcher is a set of node roles, as carried by SCOUT. Note its bits
// differ from the WhatAmI wire values: router=1, peer=2, client=4.
type WhatAmIMatcher byte

// Matcher bits.
const (
	MatchRouter WhatAmIMatcher = 1
	MatchPeer   WhatAmIMatcher = 2
	MatchClient WhatAmIMatcher = 4
)

// Matches reports whether role w is in the set.
func (m WhatAmIMatcher) Matches(w WhatAmI) bool {
	switch w {
	case Router:
		return m&MatchRouter != 0
	case Peer:
		return m&MatchPeer != 0
	case Client:
		return m&MatchClient != 0
	}
	return false
}

// Scout asks nodes of the given roles to answer with a HELLO.
type Scout struct {
	Version byte
	What    WhatAmIMatcher
	ZID     ZID // optional (zero = absent)
	Exts    []Extension
}

func (*Scout) msgID() byte { return IDScout }

// Encode writes the SCOUT message.
func (m *Scout) Encode(w *codec.Writer) {
	hdr := IDScout
	if len(m.Exts) > 0 {
		hdr |= codec.FlagZ
	}
	_ = w.WriteByte(hdr)
	_ = w.WriteByte(m.Version)
	what := byte(m.What) & 0x07
	if m.ZID.IsZero() {
		_ = w.WriteByte(what)
	} else {
		writeZIDNibble(w, m.ZID, what|flagScoutI)
	}
	WriteExtensions(w, m.Exts)
}

// Hello announces a node and the locators it can be reached on. An empty
// locator list means "the datagram's source address".
type Hello struct {
	Version  byte
	WhatAmI  WhatAmI
	ZID      ZID
	Locators []string
	Exts     []Extension
}

func (*Hello) msgID() byte { return IDHello }

// Encode writes the HELLO message.
func (m *Hello) Encode(w *codec.Writer) {
	hdr := IDHello
	if len(m.Locators) > 0 {
		hdr |= flagHelloL
	}
	if len(m.Exts) > 0 {
		hdr |= codec.FlagZ
	}
	_ = w.WriteByte(hdr)
	_ = w.WriteByte(m.Version)
	writePackedZID(w, m.ZID, m.WhatAmI)
	if len(m.Locators) > 0 {
		w.WriteZInt(uint64(len(m.Locators)))
		for _, l := range m.Locators {
			w.WriteString(l)
		}
	}
	WriteExtensions(w, m.Exts)
}

// ErrBadScouting is returned for a malformed scouting datagram.
var ErrBadScouting = errors.New("protocol: malformed scouting message")

// DecodeScouting decodes a scouting datagram (SCOUT or HELLO).
func DecodeScouting(b []byte) (Message, error) {
	r := codec.NewReader(b)
	hdr, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	version, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	var msg Message
	switch codec.MsgID(hdr) {
	case IDScout:
		s := &Scout{Version: version}
		flags, err := r.PeekByte()
		if err != nil {
			return nil, err
		}
		if flags&flagScoutI != 0 {
			z, low, err := readZIDNibble(r)
			if err != nil {
				return nil, err
			}
			s.ZID, flags = z, low
		} else {
			_, _ = r.ReadByte()
		}
		s.What = WhatAmIMatcher(flags & 0x07)
		if hdr&codec.FlagZ != 0 {
			if s.Exts, err = ReadExtensions(r); err != nil {
				return nil, err
			}
		}
		msg = s
	case IDHello:
		h := &Hello{Version: version}
		if h.ZID, h.WhatAmI, err = readPackedZID(r); err != nil {
			return nil, err
		}
		if h.WhatAmI > Client {
			return nil, ErrBadScouting
		}
		if hdr&flagHelloL != 0 {
			n, err := r.ReadZInt()
			if err != nil {
				return nil, err
			}
			if n > uint64(r.Remaining()) {
				return nil, ErrBadScouting
			}
			for range n {
				l, err := r.ReadString()
				if err != nil {
					return nil, err
				}
				h.Locators = append(h.Locators, l)
			}
		}
		if hdr&codec.FlagZ != 0 {
			if h.Exts, err = ReadExtensions(r); err != nil {
				return nil, err
			}
		}
		msg = h
	default:
		return nil, ErrUnknownMessage
	}
	return msg, nil
}
