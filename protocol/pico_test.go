package protocol

import (
	"encoding/hex"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

// TestInitAckEmptyZ: zenoh-pico 1.10 listeners answer an INIT SYN without a
// patch extension with an INIT ACK whose Z flag is set but which carries no
// extension (bytes captured from zenoh-pico 1.10.2).
func TestInitAckEmptyZ(t *testing.T) {
	b, _ := hex.DecodeString("e109f143c91c6855d9bfb521eb70718260aa3e0a000800")
	m, err := DecodeTransport(codec.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	ack, ok := m.(*Init)
	if !ok || !ack.IsAck || ack.BatchSize != 2048 || len(ack.Cookie) != 0 || len(ack.Exts) != 0 {
		t.Errorf("got %+v", m)
	}
}
