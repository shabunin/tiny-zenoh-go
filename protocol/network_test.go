package protocol

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/shabunin/tiny-zenoh-go/internal/codec"
)

func netRoundTrip(t *testing.T, m Message) {
	t.Helper()
	w := codec.NewWriter()
	m.Encode(w)
	r := codec.NewReader(w.Bytes())
	got, err := DecodeNetwork(r)
	if err != nil {
		t.Fatalf("DecodeNetwork(%T) bytes % x: %v", m, w.Bytes(), err)
	}
	if r.Remaining() != 0 {
		t.Errorf("%T: %d trailing bytes", m, r.Remaining())
	}
	if !reflect.DeepEqual(m, got) {
		t.Errorf("round-trip %T:\n got  %#v\n want %#v", m, got, m)
	}
}

func mustBytes(t *testing.T, m Message, want []byte) {
	t.Helper()
	w := codec.NewWriter()
	m.Encode(w)
	if !bytes.Equal(w.Bytes(), want) {
		t.Errorf("%T bytes:\n got  % x\n want % x", m, w.Bytes(), want)
	}
}

func TestPushVectors(t *testing.T) {
	// PUSH|N to "a/b", PUT with payload "hi", no extensions.
	mustBytes(t, &Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Payload: []byte("hi")}},
		[]byte{0x3d, 0x00, 0x03, 'a', '/', 'b', 0x01, 0x02, 'h', 'i'})
	// Default QoS (0x05) is omitted even when set explicitly.
	mustBytes(t, &Push{WireExpr: WireExpr{Scope: 1}, QoS: DefaultQoS, Body: &Del{}},
		[]byte{0x1d, 0x01, 0x02})
}

func TestDeclareVectors(t *testing.T) {
	// Undeclare queryable 7 with the "null" WireExpr extension (from zenoh-rust).
	mustBytes(t, &Declare{Body: &UndeclareQueryable{ID: 7}},
		[]byte{0x1e, 0x85, 0x07, 0x5f, 0x02, 0x00, 0x00})
	// DECLARE with Control+Block QoS, as zenohd sends them: 9e 21 08 ...
	mustBytes(t, &Declare{QoS: QoSDeclare, Body: &DeclareSubscriber{ID: 1, WireExpr: WireExpr{Suffix: "k"}}},
		[]byte{0x9e, 0x21, 0x08, 0x22, 0x01, 0x00, 0x01, 'k'})
	// D_FINAL answering interest 5.
	mustBytes(t, &Declare{HasInterestID: true, InterestID: 5, Body: &DeclareFinal{}},
		[]byte{0x3e, 0x05, 0x1a})
}

func TestNetworkRoundTrips(t *testing.T) {
	zid := ZID{b: []byte{1, 2, 3, 4}}
	ts := &Timestamp{Time: 0x1234567890, ID: zid}
	src := &SourceInfo{ZID: zid, EID: 7, SN: 99}

	netRoundTrip(t, &Push{
		WireExpr: WireExpr{Scope: 3, Suffix: "/x", Mapping: true},
		QoS:      NewQoS(PriorityRealTime, true, true), Timestamp: ts, NodeID: 4,
		Body: &Put{
			Timestamp: ts, Encoding: Encoding{ID: 5, Schema: []byte("s")},
			SourceInfo: src, Attachment: []byte("att"), Payload: []byte("payload"),
		},
	})
	netRoundTrip(t, &Push{WireExpr: WireExpr{Suffix: "a"}, Body: &Del{Timestamp: ts, SourceInfo: src, Attachment: []byte{1}}})
	netRoundTrip(t, &Request{
		ID: 42, WireExpr: WireExpr{Suffix: "q/**"}, QoS: QoSRequest,
		Target: TargetAll, Budget: 3, TimeoutMS: 10000, NodeID: 2,
		Query: &Query{
			Consolidation: ConsolidationLatest, Parameters: "a=1;b=2", SourceInfo: src,
			Body:       &QueryBody{Encoding: Encoding{ID: 1}, Payload: []byte("body")},
			Attachment: []byte("att"),
		},
	})
	netRoundTrip(t, &Request{ID: 1, WireExpr: WireExpr{Suffix: "x"}, Query: &Query{}})
	netRoundTrip(t, &Response{
		RequestID: 42, WireExpr: WireExpr{Suffix: "q/1"},
		Responder: &EntityGlobalID{ZID: zid, EID: 9},
		Body:      &Reply{Body: &Put{Payload: []byte("v")}},
	})
	netRoundTrip(t, &Response{RequestID: 1, WireExpr: WireExpr{Suffix: "q"},
		Body: &Reply{Consolidation: ConsolidationNone, Body: &Del{}}})
	netRoundTrip(t, &Response{RequestID: 2, WireExpr: WireExpr{Suffix: "q"},
		Body: &Err{Encoding: Encoding{ID: 1}, Payload: []byte("boom")}})
	netRoundTrip(t, &ResponseFinal{RequestID: 42, QoS: QoSRequest})

	for _, d := range []Declaration{
		&DeclareKeyExpr{ID: 3, WireExpr: WireExpr{Suffix: "demo/ex"}},
		&UndeclareKeyExpr{ID: 3},
		&DeclareSubscriber{ID: 10, WireExpr: WireExpr{Scope: 3, Suffix: "/**", Mapping: true}},
		&UndeclareSubscriber{ID: 10},
		&UndeclareSubscriber{ID: 10, WireExpr: WireExpr{Suffix: "demo/**"}},
		&DeclareQueryable{ID: 11, WireExpr: WireExpr{Suffix: "q"}, Complete: true, Distance: 2},
		&DeclareQueryable{ID: 12, WireExpr: WireExpr{Suffix: "q"}},
		&UndeclareQueryable{ID: 11, WireExpr: WireExpr{Suffix: "q", Mapping: true}},
		&DeclareToken{ID: 13, WireExpr: WireExpr{Suffix: "alive/me"}},
		&UndeclareToken{ID: 13},
		&DeclareFinal{},
	} {
		netRoundTrip(t, &Declare{Body: d})
		netRoundTrip(t, &Declare{HasInterestID: true, InterestID: 77, QoS: QoSDeclare, NodeID: 1, Body: d})
	}

	netRoundTrip(t, &Interest{ID: 5, Mode: InterestCurrentFuture,
		Options: InterestTokens | InterestAggregate, WireExpr: &WireExpr{Suffix: "alive/**"}})
	netRoundTrip(t, &Interest{ID: 6, Mode: InterestCurrent, Options: InterestKeyExprs | InterestSubscribers})
	netRoundTrip(t, &Interest{ID: 5, Mode: InterestFinal})
	netRoundTrip(t, &OAM{ID: 1, Enc: ExtZBuf, Buf: []byte{1, 2}})
	netRoundTrip(t, &OAM{ID: 2, Enc: ExtZ64, U64: 300, QoS: QoSDeclare})
}

func TestUnknownMandatoryExtRejected(t *testing.T) {
	w := codec.NewWriter()
	// PUT with Z set and an unknown mandatory unit extension (id 7).
	_ = w.WriteByte(0x3d)
	w.WriteZInt(0)
	w.WriteString("k")
	_ = w.WriteByte(codec.Header(IDPut, codec.FlagZ))
	_ = w.WriteByte(0x17)
	w.WriteZBytes([]byte("x"))
	if _, err := DecodeNetwork(codec.NewReader(w.Bytes())); err != ErrUnknownMandatoryExt {
		t.Errorf("err = %v, want ErrUnknownMandatoryExt", err)
	}
}

func TestShmPartsRoundTrip(t *testing.T) {
	parts := []PayloadPart{
		{Raw: []byte("head")},
		{Shm: &ShmInfo{DataLen: 1 << 20, MetaID: 3, MetaIndex: 700, Generation: 9}},
	}
	netRoundTrip(t, &Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Parts: parts}})
	netRoundTrip(t, &Push{WireExpr: WireExpr{Suffix: "a/b"}, Body: &Put{Parts: parts, Attachment: []byte{1}}})
	netRoundTrip(t, &Request{ID: 1, WireExpr: WireExpr{Suffix: "q"},
		Query: &Query{Body: &QueryBody{Encoding: Encoding{ID: 4}, Parts: parts}}})
	netRoundTrip(t, &Response{RequestID: 1, WireExpr: WireExpr{Suffix: "q"}, Body: &Err{Parts: parts}})
}

func TestQoSZeroIsDefault(t *testing.T) {
	var q QoS
	if q.Priority() != PriorityData || q.Block() || q.Express() {
		t.Errorf("zero QoS should behave as default Data/Drop/!Express")
	}
	if NewQoS(PriorityData, false, false) != 0 {
		t.Errorf("NewQoS(default) should normalize to 0")
	}
}

func TestTimestampTime(t *testing.T) {
	ts := Timestamp{Time: 10<<32 | 1<<31} // 10.5 s
	if got := ts.GoTime().UnixMilli(); got != 10500 {
		t.Errorf("GoTime = %d ms, want 10500", got)
	}
}

func TestQoSControlDropRoundTrip(t *testing.T) {
	q := NewQoS(PriorityControl, false, false)
	if q == 0 || q.Priority() != PriorityControl || q.Block() || q.Express() || q.Wire() != 0 {
		t.Fatalf("NewQoS(Control) = %#x prio %d wire %#x", byte(q), q.Priority(), q.Wire())
	}
	w := codec.NewWriter()
	(&Push{WireExpr: WireExpr{Suffix: "k"}, QoS: q, Body: &Put{Payload: []byte("x")}}).Encode(w)
	m, err := DecodeNetwork(codec.NewReader(w.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if got := m.(*Push).QoS; got != q {
		t.Errorf("decoded QoS %#x, want %#x", byte(got), byte(q))
	}
	if QoS(0).Priority() != PriorityData || QoSFromWire(5) != 0 {
		t.Error("default QoS changed")
	}
}
