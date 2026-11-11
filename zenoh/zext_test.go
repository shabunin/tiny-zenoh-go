package zenoh

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/zext"
)

// zenoh-ext serialization against the Python binding (Rust zenoh-ext), both
// ways: the tuple (i32, str, list[f64], dict[str, i64], bool, bytes,
// list[u8], f32).
func zextTuple(t *testing.T) []byte {
	t.Helper()
	b, err := zext.Serialize(int32(-7), "héllo", []float64{0.5, -2}, map[string]int64{"k": -9}, true,
		[]byte{0, 1}, []byte{3}, float32(1.5))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestZextRust(t *testing.T) {
	r := routerLocator(t)
	s, err := Open(Config{Connect: []string{r}})
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	t.Run("go to rust", func(t *testing.T) {
		key := uniq(t)
		p := startRust(t, r, true, "zsersub", key)
		time.Sleep(300 * time.Millisecond)
		_ = s.Put(key, zextTuple(t))
		var e peerEvent
		for e = p.next(); e.Zser == nil && e.ZserErr == nil; e = p.next() {
		}
		if e.ZserErr != nil {
			t.Fatalf("rust: %s", *e.ZserErr)
		}
		var got []any
		if err := json.Unmarshal(e.Zser, &got); err != nil {
			t.Fatal(err)
		}
		want := `[-7,"héllo",[0.5,-2],{"k":-9},true,"0001",[3],1.5]`
		if b, _ := json.Marshal(got); string(b) != want {
			t.Fatalf("rust read %s, want %s", b, want)
		}
	})
	t.Run("rust to go", func(t *testing.T) {
		key := uniq(t)
		k := newSink()
		sub, err := s.DeclareSubscriber(key, k.handler)
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Undeclare()
		time.Sleep(300 * time.Millisecond)
		startRust(t, r, false, "zserpub", key)
		b := k.wait(t, 1)[0].Payload
		// Python's dict and our map have one entry: the bytes are deterministic.
		if !bytes.Equal(b, zextTuple(t)) {
			t.Fatalf("rust wrote %s\nwe write  %s", hex.EncodeToString(b), hex.EncodeToString(zextTuple(t)))
		}
		var (
			i   int32
			str string
			fs  []float64
			m   map[string]int64
			ok  bool
			raw []byte
			u8s []byte
			f   float32
		)
		if err := zext.Deserialize(b, &i, &str, &fs, &m, &ok, &raw, &u8s, &f); err != nil {
			t.Fatal(err)
		}
		if i != -7 || str != "héllo" || len(fs) != 2 || fs[1] != -2 || m["k"] != -9 || !ok ||
			!bytes.Equal(raw, []byte{0, 1}) || !bytes.Equal(u8s, []byte{3}) || f != 1.5 {
			t.Fatalf("got %v %q %v %v %v %v %v %v", i, str, fs, m, ok, raw, u8s, f)
		}
	})
}
