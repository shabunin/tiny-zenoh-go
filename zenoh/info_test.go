package zenoh

import (
	"testing"

	"github.com/shabunin/gozenoh/transport"
)

func TestTransports(t *testing.T) {
	a, err := Open(Config{Mode: ModePeer, Listen: []string{"tcp/127.0.0.1:0"}, Scouting: ScoutingConfig{Disable: true},
		Compression: true, Auth: &transport.Auth{Users: map[string]string{"u": "p"}}})
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	b, err := Open(Config{Connect: a.Locators(), Compression: true, BatchSize: 4096,
		Auth: &transport.Auth{User: "u", Password: "p"}})
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	waitFor(t, "transport on both sides", func() bool { return len(a.Transports()) == 1 && len(b.Transports()) == 1 })
	ta, tb := a.Transports()[0], b.Transports()[0]
	if ta.ZID.String() != b.ZID().String() || ta.WhatAmI != WhatAmIClient || tb.ZID.String() != a.ZID().String() || tb.WhatAmI != WhatAmIPeer {
		t.Fatalf("ids: %+v / %+v", ta, tb)
	}
	for _, ti := range []TransportInfo{ta, tb} {
		if !ti.Compression || !ti.QoS || ti.LowLatency || ti.Links != 1 || ti.Proto != "tcp" || ti.BatchSize != 4096 {
			t.Errorf("%+v", ti)
		}
	}
	if ta.AuthUser != "u" || tb.AuthUser != "" {
		t.Errorf("auth users %q %q", ta.AuthUser, tb.AuthUser)
	}
}
