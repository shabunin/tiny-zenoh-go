//go:build !linux || tinygo

package shm

import (
	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// Manager is unavailable on this platform.
type Manager struct{}

// New returns ErrUnsupported on this platform.
func New(Options) (*Manager, error) { return nil, ErrUnsupported }

func (*Manager) Close() error     { return nil }
func (*Manager) Threshold() int   { return 0 }
func (m *Manager) NewLink() *Link { return &Link{} }

// Link is a per-transport SHM state; it never activates on this platform.
type Link struct{}

func (*Link) ExtID() byte                                      { return ExtID }
func (*Link) InitSyn() *protocol.Extension                     { return nil }
func (*Link) InitAck(*protocol.Extension)                      {}
func (*Link) OpenSyn(bool) *protocol.Extension                 { return nil }
func (*Link) OpenAck(*protocol.Extension) error                { return nil }
func (*Link) AcceptInitSyn(*protocol.Extension)                {}
func (*Link) AcceptInitAck() *protocol.Extension               { return nil }
func (*Link) AcceptOpenSyn(*protocol.Extension) error          { return nil }
func (*Link) AcceptOpenAck(bool) *protocol.Extension           { return nil }
func (*Link) Close()                                           {}
func (*Link) Active() bool                                     { return false }
func (*Link) Wrap([]byte, protocol.Priority) (*Wrapped, error) { return nil, ErrUnsupported }

// Resolve fails: shared memory cannot be read on this platform.
func (*Link) Resolve([]protocol.PayloadPart, protocol.Priority) ([]byte, error) {
	return nil, ErrUnsupported
}
