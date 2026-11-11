package zenoh

import (
	"encoding/json"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/shabunin/tiny-zenoh-go/protocol"
)

// icptConf is a router's interceptor and timestamping configuration.
type icptConf struct {
	ds  []DownsamplingConfig
	lp  []LowPassFilterConfig
	qos []QoSOverwriteConfig
	ts  TimestampingConfig
}

// icptRouters starts a Go router and, with ZENOH_PY, a Rust one (the
// same configuration as zenoh-rust JSON, which also checks our field names
// against its schema), and runs fn against each.
func icptRouters(t *testing.T, c icptConf, fn func(t *testing.T, router string)) {
	t.Run("go", func(t *testing.T) {
		r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
			Scouting: ScoutingConfig{Disable: true}, Downsampling: c.ds, LowPassFilter: c.lp,
			QoSOverwrite: c.qos, Timestamping: c.ts})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = r.Close() })
		fn(t, r.Locators()[0])
	})
	t.Run("rust", func(t *testing.T) {
		l := freeLocator(t)
		conf := map[string]any{"mode": "router", "listen/endpoints": []string{l},
			"scouting/multicast/enabled": false,
			"timestamping/drop_future_timestamp": c.ts.DropFutureTimestamp}
		if c.ts.Enabled != nil {
			conf["timestamping/enabled"] = *c.ts.Enabled
		}
		if c.ds != nil {
			conf["downsampling"] = c.ds
		}
		if c.lp != nil {
			conf["low_pass_filter"] = c.lp
		}
		if c.qos != nil {
			conf["qos/network"] = c.qos
		}
		startRust(t, jsonConf(conf), true, "token", "icpt/router", "60")
		fn(t, l)
	})
}

func icptClient(t *testing.T, router string) *Session {
	t.Helper()
	s, err := Open(Config{Connect: []string{router}})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func icptSub(t *testing.T, s *Session, key string) *sampleSink {
	t.Helper()
	k := newSink()
	sub, err := s.DeclareSubscriber(key, k.handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sub.Undeclare() })
	return k
}

func msgs(m ...DataMessage) []DataMessage { return m }

func TestInterceptors(t *testing.T) {
	key := uniq(t)
	yes := true
	prio := func(p Priority) *PriorityUpdate { return &PriorityUpdate{Priority: p} }
	put := msgs(MessagePut)
	conf := icptConf{
		ds: []DownsamplingConfig{{ID: "ds", Messages: put,
			Rules: []DownsamplingRule{{KeyExpr: key + "/ds/**", Freq: 2}, {KeyExpr: key + "/ds0/**", Freq: 0}}}},
		lp: []LowPassFilterConfig{{ID: "lp", Messages: msgs(MessagePut, MessageQuery, MessageReply),
			KeyExprs: []string{key + "/lp/**"}, SizeLimit: 8}},
		qos: []QoSOverwriteConfig{
			{ID: "set", Messages: put, KeyExprs: []string{key + "/qos/set"},
				Overwrite: QoSOverwrites{Priority: prio(protocol.PriorityRealTime), Express: &yes}},
			{ID: "inc", Messages: put, KeyExprs: []string{key + "/qos/inc"},
				Overwrite: QoSOverwrites{Priority: &PriorityUpdate{Increment: 2}}},
			{ID: "filter", Messages: put, KeyExprs: []string{key + "/qos/filter"}, QoS: &QoSFilter{Priority: "data_high"},
				Overwrite: QoSOverwrites{Priority: prio(protocol.PriorityInteractiveLow)}},
			{ID: "size", Messages: put, KeyExprs: []string{key + "/qos/size"}, PayloadSize: "..3",
				Overwrite: QoSOverwrites{Priority: prio(protocol.PriorityBackground)}},
			{ID: "udp", Messages: put, LinkProtocols: []string{"udp"}, KeyExprs: []string{key + "/qos/udp"},
				Overwrite: QoSOverwrites{Priority: prio(protocol.PriorityBackground)}},
		},
	}
	icptRouters(t, conf, func(t *testing.T, router string) {
		a, b := icptClient(t, router), icptClient(t, router)
		t.Run("downsampling", func(t *testing.T) {
			k := icptSub(t, b, key+"/ds$*/**")
			settle()
			for i := range 10 {
				_ = a.Put(key+"/ds/a", []byte{byte('0' + i)})
				_ = a.Put(key+"/ds0/a", []byte{byte('0' + i)})
			}
			if got := k.wait(t, 1); string(got[0].Payload) != "0" {
				t.Fatalf("first sample %q", got[0].Payload)
			}
			k.expectNone(t, 300*time.Millisecond)
			time.Sleep(300 * time.Millisecond)
			_ = a.Put(key+"/ds/a", []byte("late"))
			if got := k.wait(t, 1); string(got[1].Payload) != "late" {
				t.Fatalf("after the period: %q", got[1].Payload)
			}
		})
		t.Run("low-pass", func(t *testing.T) {
			k := icptSub(t, b, key+"/lp/**")
			settle()
			_ = a.Put(key+"/lp/a", []byte("0123456789"))
			_ = a.Put(key+"/lp/a", []byte("01234"), WithAttachment([]byte("56789")))
			_ = a.Put(key+"/lp/a", []byte("small"))
			if got := k.wait(t, 1); string(got[0].Payload) != "small" {
				t.Fatalf("got %q", got[0].Payload)
			}
			var queries atomic.Int32
			q, err := b.DeclareQueryable(key+"/lp/q", func(q *Query) {
				queries.Add(1)
				if q.Parameters() == "big" {
					_ = q.Reply(q.KeyExpr(), []byte("a big reply"))
				} else {
					_ = q.Reply(q.KeyExpr(), []byte("ok"))
				}
			})
			if err != nil {
				t.Fatal(err)
			}
			defer q.Undeclare()
			settle()
			count := func(sel string, opts ...GetOption) int {
				replies, err := collect(a.Get(sel, append(opts, WithTimeout(time.Second))...))
				if err != nil {
					t.Fatal(err)
				}
				n := 0
				for _, r := range replies {
					if r.Sample != nil {
						n++
					}
				}
				return n
			}
			if n := count(key + "/lp/q"); n != 1 {
				t.Fatalf("small reply: %d replies", n)
			}
			if n := count(key + "/lp/q?big"); n != 0 {
				t.Fatalf("big reply: %d replies", n)
			}
			before, start := queries.Load(), time.Now()
			if n := count(key+"/lp/q", WithPayload([]byte("a big query body"), Encoding{})); n != 0 || queries.Load() != before {
				t.Fatalf("big query: %d replies, queryable called %d times", n, queries.Load()-before)
			}
			// The router answers the dropped query with a final.
			if d := time.Since(start); d > 500*time.Millisecond {
				t.Errorf("dropped query took %v", d)
			}
		})
		t.Run("qos overwrite", func(t *testing.T) {
			k := icptSub(t, b, key+"/qos/**")
			settle()
			type tc struct {
				key     string
				payload string
				opts    []PutOption
				prio    Priority
				express bool
			}
			cases := []tc{
				{"set", "x", nil, protocol.PriorityRealTime, true},
				{"inc", "x", nil, protocol.PriorityBackground, false},
				{"filter", "x", []PutOption{WithPriority(protocol.PriorityDataHigh)}, protocol.PriorityInteractiveLow, false},
				{"filter", "x", nil, protocol.PriorityData, false},
				{"size", "abc", nil, protocol.PriorityBackground, false},
				{"size", "abcd", nil, protocol.PriorityData, false},
				{"udp", "x", nil, protocol.PriorityData, false},
			}
			for i, c := range cases {
				_ = a.Put(key+"/qos/"+c.key, []byte(c.payload), c.opts...)
				got := k.wait(t, 1)[i]
				if got.Priority != c.prio || got.Express != c.express {
					t.Errorf("%s %q: priority %d express %v, want %d %v", c.key, c.payload, got.Priority,
						got.Express, c.prio, c.express)
				}
			}
		})
		t.Run("timestamping", func(t *testing.T) {
			k := icptSub(t, b, key+"/ts")
			settle()
			rz := a.RoutersZID()
			if len(rz) != 1 {
				t.Fatalf("routers %v", rz)
			}
			_ = a.Put(key+"/ts", []byte("plain"))
			got := k.wait(t, 1)[0]
			if got.Timestamp == nil || !got.Timestamp.ID.Equal(rz[0]) {
				t.Fatalf("timestamp %+v, want one from the router %s", got.Timestamp, rz[0])
			}
			future := Timestamp{Time: protocol.NTP64FromTime(time.Now().Add(10 * time.Second)), ID: a.ZID()}
			_ = a.Put(key+"/ts", []byte("future"), WithTimestamp(future))
			got = k.wait(t, 1)[1]
			if got.Timestamp == nil || !got.Timestamp.ID.Equal(rz[0]) || got.Timestamp.GoTime().After(time.Now().Add(time.Second)) {
				t.Fatalf("future timestamp not replaced: %+v", got.Timestamp)
			}
			own := a.NewTimestamp()
			_ = a.Put(key+"/ts", []byte("own"), WithTimestamp(own))
			if got = k.wait(t, 1)[2]; got.Timestamp == nil || got.Timestamp.Time != own.Time || !got.Timestamp.ID.Equal(own.ID) {
				t.Fatalf("own timestamp changed: %+v", got.Timestamp)
			}
		})
	})
}

func TestTimestampingDropFuture(t *testing.T) {
	key := uniq(t)
	icptRouters(t, icptConf{ts: TimestampingConfig{DropFutureTimestamp: true}}, func(t *testing.T, router string) {
		a, b := icptClient(t, router), icptClient(t, router)
		k := icptSub(t, b, key)
		settle()
		future := Timestamp{Time: protocol.NTP64FromTime(time.Now().Add(10 * time.Second)), ID: a.ZID()}
		_ = a.Put(key, []byte("future"), WithTimestamp(future))
		_ = a.Put(key, []byte("now"))
		if got := k.wait(t, 1); string(got[0].Payload) != "now" {
			t.Fatalf("got %q", got[0].Payload)
		}
		k.expectNone(t, 200*time.Millisecond)
	})
}

func TestTimestampingDisabled(t *testing.T) {
	key := uniq(t)
	no := false
	icptRouters(t, icptConf{ts: TimestampingConfig{Enabled: &no}}, func(t *testing.T, router string) {
		a, b := icptClient(t, router), icptClient(t, router)
		k := icptSub(t, b, key)
		settle()
		_ = a.Put(key, []byte("x"))
		if got := k.wait(t, 1); got[0].Timestamp != nil {
			t.Fatalf("timestamp %+v", got[0].Timestamp)
		}
	})
}

// Flows, and faces picked by ZID and interface.
func TestInterceptorsFaces(t *testing.T) {
	key := uniq(t)
	a, b := NewZID(), NewZID()
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true},
		LowPassFilter: []LowPassFilterConfig{
			{Messages: msgs(MessagePut), KeyExprs: []string{key + "/egress/**"}, SizeLimit: 2, Flows: []ACLFlow{ACLEgress}},
			{Messages: msgs(MessagePut), KeyExprs: []string{key + "/lo/**"}, SizeLimit: 2, Interfaces: []string{"lo"}},
			{Messages: msgs(MessagePut), KeyExprs: []string{key + "/eth/**"}, SizeLimit: 2, Interfaces: []string{"no-such-if"}},
			{Messages: msgs(MessageQuery), KeyExprs: []string{key + "/egq/**"}, SizeLimit: 2, Flows: []ACLFlow{ACLEgress}},
		},
		QoSOverwrite: []QoSOverwriteConfig{{ZIDs: []string{a.String()}, Messages: msgs(MessagePut), Flows: []ACLFlow{ACLIngress},
			Overwrite: QoSOverwrites{Priority: &PriorityUpdate{Priority: protocol.PriorityBackground}}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	open := func(z ZID) *Session {
		s, err := Open(Config{Connect: r.Locators(), ZID: z})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = s.Close() })
		return s
	}
	ca, cb := open(a), open(b)
	ka, kb := icptSub(t, ca, key+"/**"), icptSub(t, cb, key+"/**")
	settle()

	// QoS overwrite on what a sends, not on what b sends.
	_ = ca.Put(key+"/q", []byte("x"))
	if got := kb.wait(t, 1); got[0].Priority != protocol.PriorityBackground {
		t.Fatalf("a's put: priority %d", got[0].Priority)
	}
	_ = cb.Put(key+"/q", []byte("x"))
	kb.wait(t, 1) // its own
	if got := ka.wait(t, 2); got[1].Priority != protocol.PriorityData {
		t.Fatalf("b's put: priority %d", got[1].Priority)
	}

	// The egress filter drops what the router sends, not its own subscriber's input.
	var routerGot atomic.Int32
	rs, err := r.DeclareSubscriber(key+"/egress/**", func(Sample) { routerGot.Add(1) })
	if err != nil {
		t.Fatal(err)
	}
	defer rs.Undeclare()
	settle()
	_ = ca.Put(key+"/egress/x", []byte("big"))
	kb.expectNone(t, 200*time.Millisecond)
	if routerGot.Load() != 1 {
		t.Fatalf("router subscriber got %d", routerGot.Load())
	}
	_ = ca.Put(key+"/lo/x", []byte("big"))
	kb.expectNone(t, 200*time.Millisecond)
	// A query dropped on its way to b's queryable ends as if b had answered.
	q, err := cb.DeclareQueryable(key+"/egq/x", func(q *Query) { _ = q.Reply(q.KeyExpr(), nil) })
	if err != nil {
		t.Fatal(err)
	}
	defer q.Undeclare()
	settle()
	start := time.Now()
	replies, err := collect(ca.Get(key+"/egq/x", WithPayload([]byte("big"), Encoding{}), WithTimeout(2*time.Second)))
	if err != nil || len(replies) != 0 || time.Since(start) > 500*time.Millisecond {
		t.Fatalf("egress-dropped query: %d replies after %v (%v)", len(replies), time.Since(start), err)
	}
	if replies, _ := collect(ca.Get(key + "/egq/x")); len(replies) != 1 {
		t.Fatalf("small query: %d replies", len(replies))
	}
	_ = ca.Put(key+"/eth/x", []byte("big"))
	if got := kb.wait(t, 1); got[len(got)-1].KeyExpr != key+"/eth/x" {
		t.Fatalf("got %q", got[len(got)-1].KeyExpr)
	}
}

func TestInterceptorsConfig(t *testing.T) {
	bad := []Config{
		{Downsampling: []DownsamplingConfig{{ID: "x", Messages: msgs(MessagePut), Rules: []DownsamplingRule{{KeyExpr: "a", Freq: 1}}},
			{ID: "x", Messages: msgs(MessagePut), Rules: []DownsamplingRule{{KeyExpr: "a", Freq: 1}}}}},
		{Downsampling: []DownsamplingConfig{{Messages: msgs("publish"), Rules: []DownsamplingRule{{KeyExpr: "a", Freq: 1}}}}},
		{Downsampling: []DownsamplingConfig{{Messages: msgs(MessagePut), Rules: []DownsamplingRule{{KeyExpr: "a", Freq: -1}}}}},
		{Downsampling: []DownsamplingConfig{{Messages: msgs(MessagePut), Rules: []DownsamplingRule{{KeyExpr: "a//b", Freq: 1}}}}},
		{LowPassFilter: []LowPassFilterConfig{{Messages: msgs(MessagePut), KeyExprs: []string{"a"}, Flows: []ACLFlow{"sideways"}}}},
		{QoSOverwrite: []QoSOverwriteConfig{{Messages: msgs(MessageReply)}}},
		{QoSOverwrite: []QoSOverwriteConfig{{Messages: msgs(MessagePut), PayloadSize: "1-2"}}},
		{QoSOverwrite: []QoSOverwriteConfig{{Messages: msgs(MessagePut), QoS: &QoSFilter{Priority: "urgent"}}}},
		{QoSOverwrite: []QoSOverwriteConfig{{Messages: msgs(MessagePut), Overwrite: QoSOverwrites{CongestionControl: "maybe"}}}},
	}
	for i, c := range bad {
		c.Listen, c.Scouting.Disable, c.Connect = []string{}, true, []string{"tcp/127.0.0.1:1"}
		c.Mode = ModePeer
		if s, err := Open(c); err == nil {
			_ = s.Close()
			t.Errorf("config %d accepted", i)
		}
	}
	var q QoSOverwriteConfig
	in := `{"messages":["put"],"overwrite":{"priority":"data_high","congestion_control":"block"},"payload_size":"10.."}`
	if err := json.Unmarshal([]byte(in), &q); err != nil || q.Overwrite.Priority.Priority != protocol.PriorityDataHigh {
		t.Fatalf("%+v %v", q, err)
	}
	if err := json.Unmarshal([]byte(`{"priority":-3}`), &q.Overwrite); err != nil || q.Overwrite.Priority.Increment != -3 ||
		q.Overwrite.Priority.Priority != 0 {
		t.Fatalf("%+v %v", q.Overwrite.Priority, err)
	}
	for _, s := range []string{`{"priority":8}`, `{"priority":"control"}`, `{"priority":1.5}`} {
		if err := json.Unmarshal([]byte(s), &q.Overwrite); err == nil {
			t.Errorf("%s accepted", s)
		}
	}
	b, _ := json.Marshal(QoSOverwrites{Priority: &PriorityUpdate{Increment: -1}})
	if !strings.Contains(string(b), `"priority":-1`) {
		t.Errorf("marshalled %s", b)
	}
	if lo, hi, err := parseRange("..3"); err != nil || lo != 0 || hi != 3 {
		t.Errorf("..3: %d %d %v", lo, hi, err)
	}
}
