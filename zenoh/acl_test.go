package zenoh

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/shabunin/gozenoh/transport"
)

// Access control. The same configuration runs on a zenoh-rust router and on
// a Go router, with Rust clients authenticating by user name: both routers
// must let exactly the same messages through.

func aclTestConfig(pfx string) *ACLConfig {
	return &ACLConfig{
		DefaultPermission: ACLDeny,
		Rules: []ACLRule{
			{ID: "pub-a", Messages: []ACLMessage{ACLPut}, Flows: []ACLFlow{ACLIngress},
				Permission: ACLAllow, KeyExprs: []string{pfx + "/a/**"}},
			{ID: "sub-all", Messages: []ACLMessage{ACLDeclareSubscriber}, Flows: []ACLFlow{ACLIngress},
				Permission: ACLAllow, KeyExprs: []string{pfx + "/**"}},
			{ID: "recv-all", Messages: []ACLMessage{ACLPut}, Flows: []ACLFlow{ACLEgress},
				Permission: ACLAllow, KeyExprs: []string{pfx + "/**"}},
			{ID: "no-secret", Messages: []ACLMessage{ACLPut}, Flows: []ACLFlow{ACLEgress},
				Permission: ACLDeny, KeyExprs: []string{pfx + "/a/secret"}},
		},
		Subjects: []ACLSubject{{ID: "alice", Usernames: []string{"alice"}}, {ID: "bob", Usernames: []string{"bob"}}},
		Policies: []ACLPolicy{
			{Rules: []string{"pub-a"}, Subjects: []string{"alice"}},
			{Rules: []string{"sub-all", "recv-all", "no-secret"}, Subjects: []string{"bob"}},
		},
	}
}

func TestACLRouterParity(t *testing.T) {
	users := map[string]string{"alice": "a-pw", "bob": "b-pw", "mallory": "m-pw"}
	dict := filepath.Join(t.TempDir(), "users.txt")
	var lines []string
	for u, p := range users {
		lines = append(lines, u+":"+p)
	}
	if err := os.WriteFile(dict, []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	client := func(loc, user string) string {
		return authRustConf(t, map[string]any{"mode": "client", "connect/endpoints": []string{loc},
			"transport/auth/usrpwd/user": user, "transport/auth/usrpwd/password": users[user]})
	}
	scenario := func(t *testing.T, loc, pfx string) []string {
		bob := startRust(t, client(loc, "bob"), true, "sub", pfx+"/**", "2")
		// mallory matches no subject: default deny, nothing she puts passes.
		startRust(t, client(loc, "mallory"), false, "puts", pfx+"/a/m").waitExit(t)
		startRust(t, client(loc, "alice"), false, "puts",
			pfx+"/a/x", pfx+"/b/x", pfx+"/a/secret", pfx+"/a/end").waitExit(t)
		var got []string
		for range 2 {
			got = append(got, strings.TrimPrefix(bob.next().Key, pfx))
		}
		return got
	}
	want := []string{"/a/x", "/a/end"}

	t.Run("Rust", func(t *testing.T) {
		pfx := uniq(t)
		acl, _ := json.Marshal(aclTestConfig(pfx))
		var aclMap map[string]any
		_ = json.Unmarshal(acl, &aclMap)
		aclMap["enabled"] = true
		loc := freeLocator(t)
		startRust(t, authRustConf(t, map[string]any{"mode": "router", "listen/endpoints": []string{loc},
			"transport/auth/usrpwd/dictionary_file": dict, "access_control": aclMap}), true, "token", pfx+"/r", "60")
		if got := scenario(t, loc, pfx); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("rust router let through %v, want %v", got, want)
		}
	})
	t.Run("Go", func(t *testing.T) {
		pfx := uniq(t)
		r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
			Scouting: ScoutingConfig{Disable: true},
			Auth:     &transport.Auth{Users: users}, ACL: aclTestConfig(pfx)})
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		if got := scenario(t, r.Locators()[0], pfx); strings.Join(got, ",") != strings.Join(want, ",") {
			t.Errorf("go router let through %v, want %v", got, want)
		}
	})
}

func TestACLConfigErrors(t *testing.T) {
	ok := aclTestConfig("k")
	bad := []func(c *ACLConfig){
		func(c *ACLConfig) { c.Rules = nil },
		func(c *ACLConfig) { c.Rules[1].ID = c.Rules[0].ID },
		func(c *ACLConfig) { c.Rules[0].Messages = []ACLMessage{"publish"} },
		func(c *ACLConfig) { c.Rules[0].Permission = "maybe" },
		func(c *ACLConfig) { c.Rules[0].KeyExprs = []string{"a//b"} },
		func(c *ACLConfig) { c.Subjects[0].Usernames = []string{" "} },
		func(c *ACLConfig) { c.Policies[0].Subjects = []string{"nobody"} },
		func(c *ACLConfig) { c.Policies[0].Rules = nil },
		func(c *ACLConfig) { c.DefaultPermission = "open" },
	}
	if _, err := newACL(ok); err != nil {
		t.Fatalf("valid config: %v", err)
	}
	for i, mod := range bad {
		c := aclTestConfig("k")
		mod(c)
		if _, err := newACL(c); err == nil {
			t.Errorf("bad config #%d accepted", i)
		}
	}
}

// Subject matching and decisions, without network.
func TestACLDecisions(t *testing.T) {
	e, err := newACL(&ACLConfig{
		DefaultPermission: ACLAllow,
		Rules: []ACLRule{{ID: "deny-cfg", Messages: []ACLMessage{ACLPut, ACLQuery}, Permission: ACLDeny,
			KeyExprs: []string{"cfg/**"}}},
		Subjects: []ACLSubject{{ID: "untrusted", LinkProtocols: []string{"udp", "ws"}, Interfaces: []string{"eth0"}}},
		Policies: []ACLPolicy{{Rules: []string{"deny-cfg"}, Subjects: []string{"untrusted"}}},
	})
	if err != nil {
		t.Fatal(err)
	}
	put, query, sub := msgIndex(ACLPut), msgIndex(ACLQuery), msgIndex(ACLDeclareSubscriber)
	ws := e.subjectsOf([]aclSubject{{"eth0", "", "", "ws", "1"}})
	tcp := e.subjectsOf([]aclSubject{{"eth0", "", "", "tcp", "1"}})
	if len(ws) != 1 || len(tcp) != 0 {
		t.Fatalf("subjects: ws %v tcp %v", ws, tcp)
	}
	for _, c := range []struct {
		subj      []int
		flow, msg int
		key       string
		want      bool
	}{
		{ws, 0, put, "cfg/x", false},
		{ws, 1, query, "cfg/a/b", false},
		{ws, 0, sub, "cfg/x", true}, // not a listed message
		{ws, 0, put, "data/x", true},
		{ws, 0, put, "cfg/**", false}, // the rule includes cfg/**
		{ws, 0, put, "**", true},      // ...but not ** (zenoh-rust semantics)
		{tcp, 0, put, "cfg/x", true},  // no subject: default
	} {
		if got := e.allowed(c.subj, c.flow, c.msg, c.key); got != c.want {
			t.Errorf("subjects %v flow %d msg %d %s: %v, want %v", c.subj, c.flow, c.msg, c.key, got, c.want)
		}
	}
}

// Queries through a Go router whose ACL denies queries on private/** to
// every tcp node (default allow): public queries are answered, private ones
// only get the final.
func TestACLQueries(t *testing.T) {
	pfx := uniq(t)
	r, err := Open(Config{Mode: ModeRouter, Listen: []string{"tcp/127.0.0.1:0"},
		Scouting: ScoutingConfig{Disable: true},
		ACL: &ACLConfig{
			DefaultPermission: ACLAllow,
			Rules: []ACLRule{{ID: "no-private", Messages: []ACLMessage{ACLQuery}, Flows: []ACLFlow{ACLIngress},
				Permission: ACLDeny, KeyExprs: []string{pfx + "/private/**"}}},
			Subjects: []ACLSubject{{ID: "tcp", LinkProtocols: []string{"tcp"}}},
			Policies: []ACLPolicy{{Rules: []string{"no-private"}, Subjects: []string{"tcp"}}},
		}})
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	loc := r.Locators()[0]
	srv, err := Open(Config{Connect: []string{loc}})
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()
	q, _ := srv.DeclareQueryable(pfx+"/**", func(q *Query) { _ = q.Reply(q.KeyExpr(), []byte("answer")) })
	defer q.Undeclare()
	cli, err := Open(Config{Connect: []string{loc}})
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()
	settle()
	count := func(sel string) int {
		// A denied query is dropped by the router, as zenoh-rust does: no
		// final comes back and the client times out.
		ch, err := cli.Get(sel, WithTimeout(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for rep := range ch {
			if rep.Sample != nil {
				n++
			}
		}
		return n
	}
	if n := count(pfx + "/public/x"); n != 1 {
		t.Errorf("public query: %d replies", n)
	}
	if n := count(pfx + "/private/x"); n != 0 {
		t.Errorf("private query: %d replies", n)
	}
}
