package link

import (
	"bufio"
	"bytes"
	"io"
	"net"
	"testing"
)

// FuzzSerialFrame: arbitrary bytes between delimiters never crash the COBS
// and frame checks, and frames we make always come back intact.
func FuzzSerialFrame(f *testing.F) {
	f.Add([]byte{}, byte(0), []byte("hello"))
	f.Add([]byte{0x02, 0x01, 0x03}, byte(1), bytes.Repeat([]byte{0}, 300))
	f.Add([]byte{0xff, 0x01}, byte(2), bytes.Repeat([]byte{0xab}, 600))
	f.Fuzz(func(t *testing.T, raw []byte, flags byte, payload []byte) {
		if dec, err := cobsDecode(nil, raw); err == nil {
			_, _, _ = parseSerialFrame(dec)
		}
		if len(payload) > SerialMTU {
			payload = payload[:SerialMTU]
		}
		fr := appendSerialFrame(nil, flags, payload)
		if bytes.IndexByte(fr[:len(fr)-1], 0) >= 0 || fr[len(fr)-1] != 0 {
			t.Fatal("COBS left a zero inside the frame")
		}
		dec, err := cobsDecode(nil, fr[:len(fr)-1])
		if err != nil {
			t.Fatal(err)
		}
		gf, gp, err := parseSerialFrame(dec)
		if err != nil || gf != flags || !bytes.Equal(gp, payload) {
			t.Fatalf("round trip: flags %d payload %d bytes, err %v", gf, len(gp), err)
		}
	})
}

// FuzzSerialStream: a whole byte stream (noise, frames, partial frames)
// through the serial reader.
func FuzzSerialStream(f *testing.F) {
	f.Add(append(appendSerialFrame(nil, 0, []byte("x")), 1, 2, 3, 0, 0))
	f.Add(bytes.Repeat([]byte{1}, 3000))
	f.Fuzz(func(t *testing.T, stream []byte) {
		s := NewSerial(nopCloser{bytes.NewReader(stream)})
		for range s.rx {
		}
	})
}

type nopCloser struct{ io.Reader }

func (nopCloser) Write(p []byte) (int, error) { return len(p), nil }
func (nopCloser) Close() error                { return nil }

// FuzzWSRead: frames from a remote WebSocket end, after the upgrade.
func FuzzWSRead(f *testing.F) {
	frame := func(op byte, masked bool, p []byte) []byte {
		b := []byte{0x80 | op}
		m := byte(0)
		if masked {
			m = 0x80
		}
		switch {
		case len(p) < 126:
			b = append(b, m|byte(len(p)))
		default:
			b = append(b, m|126, byte(len(p)>>8), byte(len(p)))
		}
		if masked {
			b = append(b, 0, 0, 0, 0)
		}
		return append(b, p...)
	}
	f.Add(frame(wsBinary, true, []byte("batch")))
	f.Add(append(frame(wsPing, true, []byte("p")), frame(wsBinary, false, bytes.Repeat([]byte{7}, 300))...))
	f.Add(append([]byte{0x02, 0x83, 1, 2, 3, 4, 'a', 'b', 'c'}, frame(wsCont, true, []byte("d"))...))
	f.Add(frame(wsClose, true, nil))
	f.Add([]byte{0x82, 0x7f, 0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff})
	f.Fuzz(func(t *testing.T, in []byte) {
		a, b := net.Pipe()
		defer a.Close()
		defer b.Close()
		go func() { _, _ = io.Copy(io.Discard, b) }() // our pongs and closes
		w := &wsConn{c: a, br: bufio.NewReader(bytes.NewReader(in))}
		w.hsOnce.Do(func() {})
		buf := make([]byte, 512)
		for {
			if _, err := w.Read(buf); err != nil {
				return
			}
			if len(w.msg) > wsMaxMessage {
				t.Fatalf("message of %d bytes", len(w.msg))
			}
		}
	})
}
