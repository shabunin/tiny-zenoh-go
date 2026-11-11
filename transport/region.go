package transport

import (
	"errors"
	"fmt"
	"unicode/utf8"

	"github.com/shabunin/gozenoh/protocol"
)

// Regions (zenoh 1.10). A node may announce a region name in INIT
// (init::ext::RegionName, a UTF-8 string of 1..32 bytes) so that its peer can
// sort it into a subregion; and during OPEN each side may tell the other on
// which side of it the other stands (open::ext::RemoteBound: 0 = north,
// 1 = south). Both are sent only when configured and ignored when unknown, so
// a node without regions interoperates unchanged.

const (
	extRegionName  = 0x8 // INIT SYN / INIT ACK
	extRemoteBound = 0x7 // OPEN SYN / OPEN ACK
)

// MaxRegionNameLen is the longest region name zenoh accepts, in bytes.
const MaxRegionNameLen = 32

// Bound is the side of a node another node stands on.
type Bound int8

const (
	// BoundNone: not stated (no RemoteBound extension).
	BoundNone Bound = -1
	// BoundNorth: the other node is a peer of ours or above us.
	BoundNorth Bound = 0
	// BoundSouth: the other node is below us; we are its region gateway.
	BoundSouth Bound = 1
)

func (b Bound) String() string {
	switch b {
	case BoundNorth:
		return "north"
	case BoundSouth:
		return "south"
	}
	return "none"
}

// PeerInfo describes the remote node to Config.Bound.
type PeerInfo struct {
	ZID        protocol.ZID
	WhatAmI    protocol.WhatAmI
	RegionName string // the name it announced, "" if none
}

// ValidateRegionName checks a region name as zenoh does.
func ValidateRegionName(s string) error {
	switch {
	case s == "":
		return errors.New("region name is empty")
	case len(s) > MaxRegionNameLen:
		return fmt.Errorf("region name %q is longer than %d bytes", s, MaxRegionNameLen)
	case !utf8.ValidString(s):
		return fmt.Errorf("region name %q is not valid UTF-8", s)
	}
	return nil
}

func (t *Unicast) regionNameExt() *protocol.Extension {
	if t.cfg.RegionName == "" {
		return nil
	}
	return &protocol.Extension{ID: extRegionName, Enc: protocol.ExtZBuf, Buf: []byte(t.cfg.RegionName)}
}

// readRegionName takes the peer's region name from its INIT.
func (t *Unicast) readRegionName(exts []protocol.Extension) error {
	e := findExt(exts, extRegionName)
	if e == nil {
		return nil
	}
	if e.Enc != protocol.ExtZBuf {
		return fmt.Errorf("%w: malformed region name extension", ErrHandshake)
	}
	if err := ValidateRegionName(string(e.Buf)); err != nil {
		return fmt.Errorf("%w: %w", ErrHandshake, err)
	}
	t.peerRegion = string(e.Buf)
	return nil
}

// remoteBoundExt asks Config.Bound which side of us the peer stands on and
// returns the OPEN extension that tells it (nil when unstated).
func (t *Unicast) remoteBoundExt() (*protocol.Extension, error) {
	t.ourBound = BoundNone
	if t.cfg.Bound == nil {
		return nil, nil
	}
	b, err := t.cfg.Bound(PeerInfo{ZID: t.peerZID, WhatAmI: t.peerWhatAmI, RegionName: t.peerRegion})
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrHandshake, err)
	}
	t.ourBound = b
	if b == BoundNone {
		return nil, nil
	}
	return &protocol.Extension{ID: extRemoteBound, Enc: protocol.ExtZ64, U64: uint64(b)}, nil
}

// readRemoteBound takes the side of the peer we stand on from its OPEN.
func (t *Unicast) readRemoteBound(exts []protocol.Extension) error {
	t.peerBound = BoundNone
	e := findExt(exts, extRemoteBound)
	if e == nil {
		return nil
	}
	if e.Enc != protocol.ExtZ64 || e.U64 > 1 {
		return fmt.Errorf("%w: invalid remote bound extension", ErrHandshake)
	}
	t.peerBound = Bound(e.U64)
	return nil
}

// PeerRegionName is the region name the peer announced ("" if none).
func (t *Unicast) PeerRegionName() string { return t.peerRegion }

// PeerBound is the side of the peer that the peer says we stand on
// (BoundNone if it did not say).
func (t *Unicast) PeerBound() Bound { return t.peerBound }

// OurBound is the side of us that we told the peer it stands on (BoundNone
// if we did not say).
func (t *Unicast) OurBound() Bound { return t.ourBound }
