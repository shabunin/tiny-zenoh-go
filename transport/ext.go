package transport

import "github.com/shabunin/gozenoh/protocol"

// HandshakeExt runs an optional extension exchange inside the INIT/OPEN
// handshake (zenoh's shared-memory negotiation is one). A fresh value is made
// per handshake by Config.Ext; the methods for one side only are called, in
// order. Methods that return an extension may return nil to send none, and
// receive nil when the peer sent none. Returning an error aborts the
// handshake.
type HandshakeExt interface {
	// ExtID is the extension id this exchange uses in INIT and OPEN.
	ExtID() byte

	// Initiator side.
	InitSyn() *protocol.Extension
	InitAck(peer *protocol.Extension)
	OpenSyn(reliable bool) *protocol.Extension
	OpenAck(peer *protocol.Extension) error

	// Responder side.
	AcceptInitSyn(peer *protocol.Extension)
	AcceptInitAck() *protocol.Extension
	AcceptOpenSyn(peer *protocol.Extension) error
	AcceptOpenAck(reliable bool) *protocol.Extension

	// Close releases what the exchange holds; it is called when the handshake
	// fails or the established transport closes.
	Close()
}

// ReliableLink may be implemented by a PacketLink whose packets are never
// lost (a WebSocket, say): stream links are always reliable, other packet
// links are not.
type ReliableLink interface {
	Reliable() bool
}

func linkReliable(l Link) bool {
	if r, ok := l.(ReliableLink); ok {
		return r.Reliable()
	}
	_, packet := l.(PacketLink)
	return !packet
}

func findExt(exts []protocol.Extension, id byte) *protocol.Extension {
	for i := range exts {
		if exts[i].ID == id {
			return &exts[i]
		}
	}
	return nil
}

func addExt(exts []protocol.Extension, e *protocol.Extension) []protocol.Extension {
	if e == nil {
		return exts
	}
	return append(exts, *e)
}
