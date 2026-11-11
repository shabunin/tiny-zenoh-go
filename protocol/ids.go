// Package protocol defines the Zenoh wire message set and its binary encoding,
// built on the primitives in internal/codec. It targets the Zenoh 1.0.0
// specification. This file holds the message identifiers and shared enums.
//
// The package is pure data + codec and TinyGo-compatible; it performs no I/O.
package protocol

import "github.com/shabunin/gozenoh/internal/codec"

// ProtocolVersion is the Zenoh wire version proposed in INIT.
//
// Per the 1.0.0 spec the current value is 0x09. It is centralized here because
// it is the single most likely value to need adjusting for interop with a
// specific zenohd/zenoh-pico build.
const ProtocolVersion byte = 0x09

// Transport message IDs (0x00–0x07).
const (
	IDOAMTransport byte = 0x00
	IDInit         byte = 0x01
	IDOpen         byte = 0x02
	IDClose        byte = 0x03
	IDKeepAlive    byte = 0x04
	IDFrame        byte = 0x05
	IDFragment     byte = 0x06
	IDJoin         byte = 0x07
)

// Network message IDs (0x19–0x1F).
const (
	IDInterest      byte = 0x19
	IDResponseFinal byte = 0x1a
	IDResponse      byte = 0x1b
	IDRequest       byte = 0x1c
	IDPush          byte = 0x1d
	IDDeclare       byte = 0x1e
	IDOAMNetwork    byte = 0x1f
)

// Data sub-message IDs (scoped to the enclosing PUSH/REQUEST/RESPONSE body).
const (
	IDPut   byte = 0x01
	IDDel   byte = 0x02
	IDQuery byte = 0x03
	IDReply byte = 0x04
	IDErr   byte = 0x05
)

// Message-specific header flags. Bit 5 is codec.Flag1, bit 6 is codec.Flag2,
// bit 7 is codec.FlagZ (extensions).
const (
	// INIT: |Z|S|A|
	flagInitA = codec.Flag1 // 0 = SYN, 1 = ACK
	flagInitS = codec.Flag2 // resolution + batch_size present

	// OPEN: |Z|T|A|
	flagOpenA = codec.Flag1 // 0 = SYN, 1 = ACK
	flagOpenT = codec.Flag2 // lease unit: 1 = seconds, 0 = milliseconds

	// CLOSE: |Z|X|S|
	flagCloseS = codec.Flag1 // 1 = close whole session, 0 = this link only

	// FRAME: |Z|X|R|
	flagFrameR = codec.Flag1 // reliable channel

	// FRAGMENT: |Z|M|R|
	flagFragR = codec.Flag1 // reliable channel
	flagFragM = codec.Flag2 // more fragments follow
)

// WhatAmI identifies a node's role, carried in the low 2 bits of the packed ZID
// byte of INIT/JOIN.
type WhatAmI byte

// WhatAmI roles.
const (
	Router WhatAmI = 0
	Peer   WhatAmI = 1
	Client WhatAmI = 2
)

func (w WhatAmI) String() string {
	switch w {
	case Router:
		return "router"
	case Peer:
		return "peer"
	case Client:
		return "client"
	default:
		return "unknown"
	}
}

// Resolution is the negotiated sequence-number / request-id resolution byte:
// bits 1:0 = frame SN resolution, bits 3:2 = request-id resolution. The default
// 0x0A selects 32-bit for both.
type Resolution byte

// DefaultResolution selects 32-bit frame-SN and request-id resolution.
const DefaultResolution Resolution = 0x0a

// FrameSNBits returns the number of bits in the frame sequence number implied by
// the resolution's low 2 bits (8, 16, 32 or 64).
func (r Resolution) FrameSNBits() uint {
	switch r & 0x03 {
	case 0:
		return 8
	case 1:
		return 16
	case 2:
		return 32
	default:
		return 64
	}
}

// Message is implemented by every decodable wire message.
type Message interface {
	// Encode writes the message (header byte and body) to w.
	Encode(w *codec.Writer)
	msgID() byte
}
