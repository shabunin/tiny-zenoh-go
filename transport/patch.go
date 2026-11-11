package transport

import "github.com/shabunin/tiny-zenoh-go/protocol"

// Patch levels of zenoh's unicast INIT extension 0x7 (zenoh-protocol
// transport/init.rs ext::PatchType): nodes at patch 1 or above mark the
// first FRAGMENT of each message (and announce dropped ones), so a receiver
// that missed a message's tail can tell where the next one starts.
const (
	extPatch             = 0x7
	patchFragmentMarkers = 1
	patchCurrent         = patchFragmentMarkers
)

func patchExt(level uint64) protocol.Extension {
	return protocol.Extension{ID: extPatch, Enc: protocol.ExtZ64, U64: level}
}

// negotiatePatch returns the patch level both sides support, given the
// other side's INIT extensions.
func negotiatePatch(exts []protocol.Extension) uint64 {
	e := findExt(exts, extPatch)
	if e == nil || e.Enc != protocol.ExtZ64 {
		return 0
	}
	return min(e.U64, patchCurrent)
}
