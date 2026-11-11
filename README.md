# tiny-zenoh-go

A pure-Go implementation of the [Zenoh](https://zenoh.io) protocol — no cgo, no
Rust, no C bindings. The whole stack, including the public `zenoh` API, also
compiles under [TinyGo](https://tinygo.org). The core has no dependencies
outside the standard library; the optional QUIC link (`link/quic`) uses the
pure-Go [quic-go](https://github.com/quic-go/quic-go).

> **Status: client, peer and router networks work with real Zenoh.**
> A tiny-zenoh-go session connects to `zenohd` as a client, runs as a peer talking
> directly to other peers (Go or Rust), or runs as a **router** (`gozenohd`)
> that Rust and Go clients and peers attach to — found by multicast scouting —
> and that joins router meshes with other gozenohd and `zenohd` routers.
> Pub/sub, queries/queryables and liveliness interoperate with the official
> Rust stack in every direction, over TCP, UDP, TLS (incl. mutual TLS), QUIC,
> WebSocket, Unix sockets and **serial lines** (zenoh's COBS/CRC framing, as
> zenoh-pico and zenoh-rust use it), and same-host nodes can pass payloads
> through **shared memory** (compatible with zenoh-rust's SHM). The transport
> speaks zenoh's **QoS priorities**, **LZ4 batch compression**, **multilink**
> (several links, e.g. TCP + UDP, under one session) and the **multicast
> transport** (UDP groups with `JOIN`), all checked against zenoh-rust. It runs
> on microcontrollers with TinyGo (verified on ESP32 and ESP32-S3 boards over
> serial and Wi-Fi).

Targets the **Zenoh 1.0.0 wire specification** (<https://spec.zenoh.io/spec/1.0.0/>),
protocol version `0x09`; verified against **zenohd 1.10.2**.

## Quick example

```go
s, err := zenoh.Open(zenoh.Config{Connect: []string{"tcp/127.0.0.1:7447"}})
if err != nil { log.Fatal(err) }
defer s.Close()

sub, _ := s.DeclareSubscriber("demo/**", func(smp zenoh.Sample) {
    fmt.Println(smp.KeyExpr, string(smp.Payload))
})
defer sub.Undeclare()

_ = s.Put("demo/hello", []byte("hi"), zenoh.WithEncoding(zenoh.EncodingTextPlain))

replies, _ := s.Get("demo/**?x=1", zenoh.WithTimeout(time.Second))
for r := range replies {
    if smp, err := r.Result(); err == nil {
        fmt.Println("reply", smp.KeyExpr, string(smp.Payload))
    }
}
```

Peer mode — no router needed:

```go
s, _ := zenoh.Open(zenoh.Config{Mode: zenoh.ModePeer}) // listens + scouts 224.0.0.224:7446
// or connect explicitly:
s, _ = zenoh.Open(zenoh.Config{Mode: zenoh.ModePeer, Connect: []string{"tcp/10.0.0.5:7447"},
    Listen: []string{"tcp/0.0.0.0:7448"}})

// find nodes without connecting
sc, _ := zenoh.Scout(zenoh.MatchRouter|zenoh.MatchPeer, zenoh.ScoutingConfig{}, func(h zenoh.Hello) {
    fmt.Println(h.WhatAmI, h.ZID, h.Locators)
})
defer sc.Stop()
```

A pure-Go router, standing in for `zenohd` (also builds with TinyGo):

```bash
go run ./cmd/gozenohd                     # listens on tcp/0.0.0.0:7447, answers scouting
go run ./cmd/gozenohd -l tcp/0.0.0.0:7447 -i 1234abcd --no-multicast-scouting
go run ./cmd/gozenohd -l tcp/0.0.0.0:7449 -e tcp/10.0.0.5:7447   # join a zenohd mesh
go run ./cmd/gozenohd -l udp/0.0.0.0:7447 -l tls/0.0.0.0:7448 --tls-cert srv.pem --tls-key srv.key
go run ./cmd/gozenohd -c zenoh.json5      # a zenohd JSON5 config (see below)
go run ./cmd/gozenohd --rest-http-port 8000 \
   --cfg 'plugins/storage_manager/storages/demo:{key_expr:"demo/**",volume:"memory"}'
curl -X PUT -d hello http://localhost:8000/demo/a && curl http://localhost:8000/demo/**
```

gozenohd reads zenohd's configuration files (JSON5) and command line: `-c`,
`--cfg 'path/to/key:<json5>'` (and `--cfg ':<whole config>'`),
`--rest-http-port`, `--no-timestamp`, `--adminspace-permissions`, with
explicit flags winning over the file as in zenohd. Understood sections:
`mode`, `id`, `listen`/`connect` endpoints, `scouting` (multicast
enabled/address, gossip enabled/multihop), `region_name`, `namespace`,
`queries_default_timeout`, `transport` (unicast lowlatency / max_links / qos /
compression, link tx lease / batch_size, link tls, shared_memory, auth
usrpwd), `timestamping`, `downsampling`, `low_pass_filter`, `qos/network`,
`access_control`, `gateway`, `adminspace`, `metadata` and `plugins`; others
are reported and ignored. Plugins are built in, not loaded: **`rest`**
(`plugins/rest`, zenoh's REST API: GET/POST queries as JSON, HTML or `_raw`,
server-sent events subscriptions, PUT/PATCH/DELETE, `@/local`; output checked
against zenohd's plugin) and **`storage_manager`** with the `memory` volume
(`plugins/storage`: latest value per key with timestamps, tombstones,
wildcard puts/deletes, `strip_prefix`, `complete`, `garbage_collection`,
and **`replication`**: replicas align with each other and with zenoh-rust's;
other backends are not implemented).

QUIC uses the same TLS settings; enable it with a blank import:
`import _ "github.com/shabunin/tiny-zenoh-go/link/quic"` (gozenohd already does,
`-l quic/0.0.0.0:7447 --tls-cert … --tls-key …`).

TLS for a session — or put zenoh's keys in the locator itself
(`tls/host:7447#root_ca_certificate_file=ca.pem;verify_name_on_connect=false`):

```go
s, _ := zenoh.Open(zenoh.Config{
    Connect: []string{"tls/router.example:7447"},
    TLS:     &link.TLSConfig{RootCACertificate: "ca.pem"}, // + Connect* and EnableMTLS for mTLS
})
```

**Connection events.** To react when the router goes away and comes back
(zenoh's transport / link events listeners):

```go
l, _ := s.DeclareTransportEventsListener(func(ev zenoh.TransportEvent) {
    up := ev.Kind == zenoh.SampleKindPut // Delete: closed
    fmt.Println(ev.Transport.WhatAmI, ev.Transport.ZID, up)
}, zenoh.WithHistory(true)) // first report what is already connected
defer l.Undeclare()
```

`DeclareLinkEventsListener` does the same per link (locators, MTU,
priorities, reliability). Callbacks run in order on the session's
goroutines. Builds that never declare one don't link the code in.

**Traffic statistics.** Like zenohd built with its `stats` feature, a
session counts per link the bytes, transport messages and network messages
(by priority, kind, SHM), and per transport the payload sizes of the data
crossing it and of what it dropped (access control, downsampling, low-pass,
no link):

```go
st := s.Stats()
fmt.Println(st.Tx.Bytes, st.Rx.NetworkMessages, st.Rx.Dropped)
for _, t := range st.Transports {
    fmt.Println(t.Transport.ZID, t.Rx.Messages, t.Rx.PayloadBytes)
}
fmt.Print(s.Metrics(zenoh.MetricsOptions{})) // OpenMetrics, zenohd's names
```

Routers serve the same text at `@/<zid>/router/metrics` (Prometheus-ready
with `?compression=false`; `per_transport`, `per_link`, `per_key`,
`disconnected`, `descriptors` as zenohd) and add the counters to their
admin JSON with `@/<zid>/router?_stats=true`. `Config.Stats.Filters` (zenohd
`stats/filters`) adds per-key-expression payload series. Statistics are on
by default; TinyGo builds leave them out (~40 KB of code) unless built with
`-tags zenoh_stats`.

**TLS on TinyGo.** TinyGo's `crypto/tls` is a stub, so package `tls13` is
a small TLS 1.3 client of our own: X25519, AES-128/256-GCM, server
certificates with ECDSA P-256/P-384, RSA or Ed25519 keys checked against
configured roots, client certificates (mutual TLS), KeyUpdate. Boards turn
it on for `tls/` locators with a blank import (builds without it don't carry
its ~200 KB). There are no system roots, so give the CA, as PEM bytes when
there are no files:

```go
import _ "github.com/shabunin/tiny-zenoh-go/link/tls13link"

s, _ := zenoh.Open(zenoh.Config{
    Connect: []string{"tls/192.168.1.10:7447"},
    TLS:     &link.TLSConfig{RootCACertificatePEM: caPEM}, // + ConnectCertificatePEM / ConnectPrivateKeyPEM
})
```

`link.TLS13Client(conn, locator, cfg)` runs it over a connection a
`Config.Dial` made itself. On an ESP32-S3 (240 MHz) a handshake takes
~340 ms with an ECDSA P-256 server certificate and ~460 ms with a client
certificate, and allocates ~41 KB. The binary grows by ~200 KB of flash.
Ed25519 is left out of TinyGo builds unless they use the `tls13_ed25519`
build tag: its precomputed tables cost ~78 KB of RAM. There is no TLS 1.2,
session resumption, HelloRetryRequest (servers must accept X25519) or
server side.

or embedded: `zenoh.Open(zenoh.Config{Mode: zenoh.ModeRouter})` — the router
session can publish, subscribe and serve queries itself.

## What works today

| Package          | Status | Notes |
|------------------|--------|-------|
| `internal/codec` | ✅ | VLE/zint varints (Zenoh's 9-byte u64), length-prefixed bytes/strings, header helpers. |
| `keyexpr`        | ✅ | Validation, canonical form, **match / intersect / include** with `*`, `**`, `$*`; `StripNonWildPrefix` (zenoh's namespace stripping, wildcards covering the prefix included). |
| `protocol`       | ✅ | Scouting (`SCOUT`, `HELLO`) and the full transport message set: `INIT` `OPEN` `CLOSE` `KEEP_ALIVE` `FRAME` `FRAGMENT` `JOIN`; `PUSH` `REQUEST` `RESPONSE` `RESPONSE_FINAL` `DECLARE` (keyexpr/subscriber/queryable/token + undeclares, final) `INTEREST` `OAM`; `PUT` `DEL` `QUERY` `REPLY` `ERR`; extensions (QoS, Timestamp, SourceInfo, ResponderId, attachments, target, budget, timeout). |
| `transport`      | ✅ | Unicast session over stream links (length-prefixed batches) or packet links (one batch per datagram, batch size capped at the link MTU), both sides of the handshake (`Open` and `Accept`, with duplicate/self rejection) + negotiation, stream framing, FRAME batching with SNs, FRAGMENT split/reassembly, keep-alive, lease expiry, CLOSE. **QoS** (INIT ext `QoS`/`QoSLink`, 8 priorities × reliable/best-effort SN channels, priority in FRAME/FRAGMENT); **compression** (INIT ext 0x6, batch header byte, own pure-Go LZ4 block codec in `internal/lz4`); **multilink** (`Config.MaxLinks`: INIT/OPEN ext 0x4 with zenoh's RSA-512 PKCS#1 key proof, one transport over several links with per-link batch size/compression/lease, zenoh-rust's link selection by reliability and priority range, link loss without session loss); **multicast** (`NewMulticast` over a `MulticastLink`: periodic `JOIN` with next SNs, per-sender SN windows and reassembly, sender leases, CLOSE); **low latency** (`Config.LowLatency`: INIT ext 0x5, every network message sent alone with no FRAME/SN/fragmentation, u32 LE length prefix on streams); **patch** (INIT ext 0x7: fragment start/drop markers). Imports no `net`. |
| `link`           | ✅ TCP, UDP, UDP multicast, TLS, QUIC, WS, Unix, named pipes, vsock, serial | Locator parsing incl. `?metadata` / `#config`; TCP; UDP (MTU 65487 like zenoh on Linux, one listener socket demultiplexed per remote address); UDP multicast groups `udp/224.x.x.x:port#iface=<name>;ttl=<n>` (send socket bound to the interface, own datagrams filtered); TLS with zenoh's config keys (`root_ca_certificate`, `listen_*`/`connect_*` certificate & key as `_file`/`_base64`/`_raw`, `enable_mtls`, `verify_name_on_connect`); QUIC in `link/quic` (via `link.Register`; zenoh's variants chosen by ALPN from the locator metadata: single stream, multistream with a stream per priority (default), mixed reliability with best-effort traffic in QUIC datagrams `?mixed_rel=1`, datagrams only `?rel=0`); WebSocket `ws/host:port` (own minimal RFC 6455 code, one binary message per batch); Unix sockets `unixsock-stream/<path>` (flock'd `<path>.lock`, stale sockets replaced); named pipes `unixpipe/<path>` (zenoh's FIFO handshake: flock'd `<path>_uplink` request FIFO, DEADBEEF+suffix invitations, a dedicated FIFO pair per link, `#file_mask=`); VM sockets `vsock/<cid>:<port>` (Linux AF_VSOCK, `VMADDR_CID_*` names, host↔VM or `vsock_loopback`); serial `serial/<dev>#baudrate=N` (z-serial framing: COBS, CRC-32, I/A/R connect handshake, 1500-byte frames; Linux ttys opened raw with exclusive access; `link.NewSerial` wraps any byte stream, e.g. a TinyGo `machine.UART`). |
| `tls13`          | ✅ client | TLS 1.3 client (RFC 8446) that builds with TinyGo, used by `link/tls13link`: X25519 key share, TLS_AES_128_GCM_SHA256 / TLS_AES_256_GCM_SHA384, own HKDF key schedule, record layer with concurrent Read/Write, KeyUpdate both ways (and before AES-GCM's record limit), alerts, close_notify; a minimal X.509 reader (names, validity, basicConstraints, key usage, extended key usage, unknown critical extensions rejected) and chain check against configured roots or a pinned certificate, SAN DNS (wildcards) / IP name check, validity skipped while a board's clock is unset; ECDSA P-256/P-384 verify and sign on own Montgomery field arithmetic (no allocations), RSA-PSS / PKCS #1 v1.5 via `crypto/rsa`, Ed25519; client certificates with PKCS #8, SEC 1 or PKCS #1 keys. |
| `plugins`        | ✅ rest, storage (memory) | `plugins/rest`: zenoh-plugin-rest over any session (`rest.Serve(s, ":8000")`, `rest.Handler(s)`), JSON samples with zenoh's payload-to-JSON rules. `plugins/storage`: zenoh-plugin-storage-manager's in-memory volume (`storage.NewMemory(s, storage.Config{KeyExpr: "demo/**"})`) with **replication** (`Config.Replication`: zenoh's digest/aligner protocol — XXH3 fingerprints of a log cut into intervals and sub-intervals, hot/warm/cold eras, bincode digests on `@-digest/<zid>/<config hash>`, alignment queries to `@zid/<zid>/<config hash>/aligner`, initial alignment of an empty replica). Both in gozenohd. REST is not in TinyGo builds. |
| `zext`           | ✅ | zenoh-ext **serialization** (`z_serialize` / `z_deserialize` format): fixed-width little-endian numbers incl. 128-bit, bool, LEB128 lengths, strings, bytes, sequences, maps, tuples as concatenation; `Serializer`/`Deserializer`, generic `WriteSlice`/`ReadMap`..., `Serialize(vals...)`/`Deserialize(b, ptrs...)` without reflection (TinyGo), `Marshaler`/`Unmarshaler` for structs. |
| `shm`            | ✅ Linux | zenoh's POSIX shared memory, layout-compatible with zenoh-rust 1.x (SHM protocol v2): `/dev/shm/<id>.zenoh` metadata/data/auth segments, INIT/OPEN negotiation (challenges read from each other's auth segment), per-priority handoff counters, watchdog confirmation (50 ms) and validation (100 ms), refcounts and generations, garbage collection, orphan-segment cleanup. Enabled with `Config.SharedMemory` (`gozenohd --shm`): payloads ≥ 3 KiB to SHM-capable faces go through a 16 MiB pool; received SHM payloads are copied out. Stub returning `ErrUnsupported` elsewhere and on TinyGo. |
| `zenoh`          | ✅ client + peer + router | Router mode (router meshes: link-state gossip, network graph, shortest-path trees, sourced declarations, multi-hop data/query routing; data/query forwarding, INTEREST answering with aggregated declarations kept up to date, incl. *aggregate* interests); client mode; peer mode (listeners, reconnecting connectors, multicast scouting + autoconnect, gossip scouting (peers that know only some of the others get linked to all; `Gossip`, with `Multihop` relaying link states across any number of hops), declaration push between peers, writer-side filtering, peers attached to routers); **regions** (zenoh 1.10: every node sorted into its north region or a south subregion it is the gateway of; routers gossip with their peers and don't relay inside a peer region; peers are gateways of their clients; custom subregions by role/ZID/interface/announced `RegionName` with `Gateway.South`, so a peer can be the gateway of a group of peers and a router the gateway of router subregions, each its own link-state mesh; one forwarding gateway per peer or router subregion when several serve it); `Scout`; `Session`, `Put`/`Delete`, `Publisher` (declared key-expr ids), `Subscriber` (callback or channel), `Get` with targets/consolidation/timeout/payload/attachment, `Querier` (declared queries with fixed target/consolidation/timeout; undeclare drops its running queries), **query cancellation** (`CancellationToken` + `WithCancellation` on gets, querier gets and liveliness gets, as zenoh's unstable API: local, stops replies, waits for running callbacks), matching status and matching listeners on publishers and queriers, **advanced pub/sub** (zenoh-ext compatible: `DeclareAdvancedPublisher` with cache, sequence numbers + heartbeats for miss detection, publisher detection; `DeclareAdvancedSubscriber` with history incl. late-joining publishers (`_max`, `_time` limits), in-order delivery, recovery of lost samples by gap queries / periodic queries / heartbeats, miss listeners, subscriber detection), `_anyke` replies, `_time` ranges (`ParseTimeRange`), source info on samples, `Queryable` (reply, reply-delete, reply-err, detach), liveliness tokens/subscribers (with history)/get, encodings, attachments, HLC timestamps, local delivery inside one session; **namespaces** (`Config.Namespace`: every key the session is given goes under `<ns>/`, every key it gives back loses it, traffic outside it is dropped; the admin space stays outside); **locality** (`WithAllowedOrigin` on subscribers and queryables, `WithAllowedDestination` on puts, publishers, gets and queriers: same session only, remote only, or both; session-local entities are not declared, matching status follows the destination); priorities on the wire (QoS on by default, `DisableQoS`), `Compression`, `MaxLinks` (redialed links rejoin the live transport), multicast groups in `Listen` (publications go to the group, group data reaches local subscribers); handshake authentication (`Auth`: user/password, RSA public keys); access control (`ACL`: zenoh's rules/subjects/policies, ingress and egress); **interceptors** (zenoh's `downsampling`, `low_pass_filter` and `qos/network` sections as `Downsampling`, `LowPassFilter`, `QoSOverwrite`: per-rule rate limits, size limits on payload + attachment, priority / congestion control / express overwrite filtered by key, QoS and payload size; per face by interface, link protocol and ZID, ingress and/or egress) and **timestamping** (`Timestamping`: routers stamp PUTs that cross them, future timestamps replaced or dropped); **admin space** (`AdminSpace`, on by default in router mode: `@/<zid>/<mode>` with zid/version/`Metadata`/locators/sessions (links, region, router-link weights), `/metrics` (OpenMetrics, gzip), `/linkstate/<region>` (Graphviz graphs of every mesh and peer region), `/subscriber`, `/queryable`, `/token`, `/publisher`, `/querier` `/<key>` (who declared what), `/route/successor/src/<zid>/dst/<zid>`; and in every session the session-local `@/<zid>/session/transport/unicast/<zid>[/link/<id>]` with PUT/DELETE events as transports and links come and go, also queryable through its `@adv/…` form for advanced subscribers); **connectivity** (`Transports()` incl. multicast group members, `Links()`, and zenoh's transport / link events listeners: `DeclareTransportEventsListener`, `DeclareLinkEventsListener` with `WithHistory` and `WithLinkTransport`, opened/closed as PUT/DELETE, links withdrawn before their transport; linked in only when used); **traffic statistics** (zenohd's `stats` feature: per-link bytes / transport / network messages, per-transport payload and dropped-payload histograms, per-key filters; `Stats()`, `Metrics()`, `/metrics` with zenohd's metric names and labels, `?_stats=true` admin JSON; TinyGo: `-tags zenoh_stats`). |

### Verified interop

- **tiny-zenoh-go ↔ zenohd**: pub/sub, publisher key-expr ids, 300 KB fragmented
  payloads, queries with error replies, the router's admin space, liveliness.
- **tiny-zenoh-go ↔ Rust Zenoh** (via the official Python binding through zenohd):
  Go→Rust and Rust→Go pub/sub (encoding schema, attachments, delete),
  Go `Get` → Rust queryable and Rust `get` → Go queryable (incl. error
  replies), liveliness tokens both ways (history + delete on undeclare).
- **Go peer ↔ Rust peer, no router**: either side listening/connecting,
  pub/sub, queries both ways, liveliness, and a Rust peer and a Go peer
  finding each other by multicast scouting.
- **unixpipe and vsock with zenoh-rust 1.10.2** (Rust test peer built with
  both links): Rust peer listening / Go peer dialing, Go router listening /
  Rust clients dialing; pub/sub and queries both ways.
- **Storage replication with zenoh-rust 1.10.2's storage manager**: a Go
  replica bootstraps from a zenohd one and the other way round; events only
  one side has, in the hot, warm and cold eras, deletes and wildcard
  deletes reach the other side; with and without `strip_prefix`; once
  aligned, zenohd's published digest equals the Go replica's.
- **Connectivity events with zenoh-rust 1.10.2**: a Rust peer's transport
  and link events listeners and ours report the same connection (ZID, role,
  QoS; the link's locators mirrored, MTU, streaming, priority range,
  reliability), each hearing the other leave.
- **REST plugin**: same requests answered identically by zenohd's REST
  plugin and `plugins/rest` (JSON, HTML, `_raw`, SSE, PUT/DELETE, errors).
- **Go peer attached to zenohd** alongside clients: data, queries, liveliness.
- **gozenohd routing for Rust**: the whole interop + cross suite passes with
  `ZENOH_ROUTER=go` (an in-process Go router instead of zenohd), plus
  Rust↔Rust tests where Go only routes — a Rust *declared publisher* and
  *querier* (which send nothing until the router answers their INTERESTs),
  liveliness, Rust peers and Go clients on one router, and Rust clients
  finding gozenohd by multicast scouting. A TinyGo-built gozenohd routes
  between Rust clients too.
- **Router meshes**: gozenohd ↔ gozenohd (direct and in a chain, where the
  middle router forwards for the others) and gozenohd ↔ zenohd, plus
  zenohd ↔ gozenohd ↔ zenohd, where two zenohd routers that don't know each
  other exchange pub/sub, queries and liveliness only through the Go router
  (also checked with the TinyGo-built gozenohd and Rust clients).
- **zenoh-pico 1.10.2** (the official C implementation, driven by
  `zenoh/testdata/zpico/zpico.c`): pico clients through gozenohd and through
  zenohd with Go clients on the other side, both ways: put/delete with
  encoding and attachment, declared publishers, 3000-byte payloads
  fragmented over pico's 2048-byte batches, `get` with payload, error
  replies and big replies, declared queriers, queryables (reply attachment
  echo), liveliness tokens/subscribers with history/get. Go peers ↔ pico
  peers over TCP without a router (either side listening), and on a UDP
  multicast group (JOIN transport, fragmented payloads); pico clients
  finding gozenohd by multicast scouting.
- **UDP, TLS, QUIC, WebSocket and Unix-socket links**: Rust clients ↔ Go router, Go clients ↔ Rust router,
  Go peer ↔ Rust peer (each side dialing), Go ↔ Go including 300 KB
  fragmented payloads over UDP, mutual TLS, untrusted servers rejected;
  gozenohd routing between a Rust TLS client and a Rust UDP client, and a
  Rust QUIC client on gozenohd receiving from a Rust client on zenohd via
  the router mesh.
- **Multihop gossip**: in a chain A–B–C–D where only A connects to
  peers it learns about, and nobody can dial C, A finds D only through link
  states relayed over two hops, then connects to it directly. This works
  with Rust relays and Go ends, and with Go relays and Rust ends. A
  single-hop control check confirms A never finds D.
- **Namespaces and serialization**: a Rust session with `namespace: "ns"`
  gets a plain Go put on `ns/k` as `k`, a namespaced Go put as well, and its
  publications and queryable reach a namespaced Go session under the same
  keys. A tuple `(i32, str, list[f64], dict[str, i64], bool, bytes,
  list[u8], f32)` serialized by `zext` is read by Python's `z_deserialize`,
  and the one Python's `z_serialize` writes is byte-for-byte what `zext`
  writes; zenoh-ext's own binary test vectors pass too.
- **Interceptors**: one configuration (Go structs, given to zenoh-rust as
  its JSON config sections) produces the same results on a Rust router and
  on a Go router: downsampling at 2 Hz and 0 Hz, low-pass limits on puts,
  queries and replies (a dropped query ends at once, not at the timeout),
  priority set / incremented / set only for a given priority or payload
  size range / not applied on a non-matching link protocol, express set,
  router timestamps on unstamped puts, future timestamps replaced or
  (`drop_future_timestamp`) dropped, no timestamps when disabled.
- **Admin space**: the same Go clients (subscriber, queryable, token,
  publisher, querier) attached to a Go router and to a Rust router get
  identical admin answers, ZIDs aside: entity sources, root info fields and
  session entry, link-state graphs, metrics text. A Go router meshed with a
  Rust router shows the same graph edge weight and the same number of route
  successors as the Rust one, and reports the same link weights.
- **Statistics**: against a zenoh-rust router built with its `stats`
  feature, the same traffic gives the same metric families, help texts,
  units and label sets, and the same network-message counts and payload
  histograms (per kind, priority, key space and filter); `?_stats=true`
  JSON has the same fields. Byte and transport-message counts differ by
  design: zenoh-rust packs more messages into a batch.
- **Regions**: a Rust router sorts Go peers into subregions by the names they
  announce, relays between subregions and not inside one, and refuses an
  unnamed peer. A Go peer gets "you are south of me" from a Rust peer
  gateway, and a Rust peer gets it from a Go peer gateway; traffic crosses
  both ways in each case. Rust clients reach Rust peers and each other through a Go
  peer, and through a Go peer that sits under a router (client
  publishers and queriers need the router's subscribers and queryables
  relayed). Rust peers announcing names are sorted by `gozenohd -gateway`.
  Router subregions work both ways: Go routers named `cell` form a mesh
  south of a Rust gateway router (they see it as a mesh router with "you
  are south of me", and the north router never sees them), and Rust
  routers do the same under a Go gateway; data and queries cross in
  both directions.
  With three meshed routers and a peer attached to two of them, the peer
  gets each sample once wherever it is published, as with Rust.
- **QUIC variants**: single stream, multistream, mixed reliability,
  multistream + mixed reliability, and datagrams only. Each was tested Go ↔ Go,
  with a Rust peer dialing a Go peer, and with Go dialing a Rust router.
  Traffic flowed both ways at every priority, reliable and best effort, and
  the negotiated variant was checked on our side.
- **Shared memory**: Go peer ↔ Rust peer (zenoh-rust's default SHM config):
  puts, query bodies and replies of 100 KB arrive in Rust as SHM buffers
  (`ZBytes.as_shm()`), and Rust's auto-wrapped payloads are read by Go;
  Go ↔ Go too, and SHM stays off unless both sides enable it.
- **QoS, compression, multilink, multicast** (zenoh-rust 1.10 via the Python
  binding): priorities set in Go arrive in Rust and vice versa through zenohd
  and through gozenohd; a Rust peer with `transport/unicast/compression/
  enabled` exchanges 100 KB payloads with a Go peer, compressed both ways (Rust's
  `lz4_flex` reads our blocks and we read its); a Rust peer with
  `transport/unicast/max_links: 2` dials a Go peer's TCP and UDP locators and
  both end up in one transport (the RSA challenge verified both ways); a Rust
  peer and a Go peer on the same UDP multicast group exchange publications in
  both directions with no unicast link.
- **Peer gossip**: Go peers that only know a Rust peer in the middle get
  linked to each other through its gossip, and a Rust peer and a Go peer
  that only know a Go peer get linked through ours (data then flows over
  the direct link, as peers do not route for each other); Go peers keep
  gossiping to zenohd without disturbing it.
- **Authentication and access control**: Rust clients authenticate to a Go
  peer and to gozenohd with user/password (wrong password and missing
  credentials refused), and with RSA keys (unknown key refused); a Go client
  authenticates to a Rust peer reading a zenoh `dictionary_file`. One
  `access_control` config with per-user rules (ingress puts, egress deny
  overriding an allow, a user matching no subject) gives identical results
  on a zenoh-rust router and on a Go router.
- **Querier and matching status**: Go publishers and queriers (as clients
  of zenohd or of the Go router, and as peers directly linked to a Rust
  peer) see Rust subscribers and queryables come and go; Rust publishers
  and queriers (incl. `ALL_COMPLETE` targets) see Go subscribers and
  complete queryables, through both routers and peer to peer.
- **Advanced pub/sub**: a Rust `AdvancedSubscriber` (history of late
  publishers, recovery) gets a Go `AdvancedPublisher`'s cached samples and
  recovers samples we drop on purpose from our cache, in order and without
  reported misses; a Rust heartbeat-only subscriber fetches everything up to
  our heartbeat's sequence number. A Go `AdvancedSubscriber` gets a Rust
  publisher's history (late-publisher detection) and recovers from its
  heartbeat. Through zenohd and through the Go router.
- **Low latency**: a Rust peer with `transport/unicast/lowlatency: true`
  (QoS off) exchanges publications and queries with a Go peer both ways.
- **TLS 1.3 client** (`tls13`): against Go's `crypto/tls` (ECDSA P-256 and
  P-384, RSA and Ed25519 server keys, chains through intermediates with RSA,
  ECDSA and Ed25519 signatures, both cipher suites, client certificates of
  every key type, untrusted / misnamed / expired / non-CA / wrong-usage
  certificates rejected); against rustls through zenoh routers of the
  Python binding (ECDSA and RSA-PSS server certificates, mutual TLS); and
  OpenSSL 3.5 `s_server` asking for a KeyUpdate. A Go router and a Rust
  router carry zenoh sessions dialed with it, including 500 KB payloads.
- **Microcontrollers** (TinyGo 0.44): an M5StickC (ESP32) running
  `examples/tinyserial` over its USB-UART reaches a Rust client through
  `gozenohd -l serial//dev/ttyUSB1#baudrate=115200 -e tcp/<zenohd>` (counter
  publications, queries answered by the device, LED driven by puts); an
  M5Stack Cardputer (ESP32-S3) on Wi-Fi connects to `zenohd` directly over
  TCP. Serial framing matches z-serial's byte vectors.
- **Board tests** (`examples/boardtest`): a scripted run on the device against
  a zenoh-rust harness (`host.py`) passes on both boards. It covers a querier
  with matching status, a publisher waiting for a matching subscriber,
  an advanced publisher serving a late zenoh-ext subscriber its history and
  heartbeat, an advanced subscriber fetching a Rust publisher's history, and
  liveliness tokens, gets and subscribers both ways. The M5StickC runs it over
  serial with LZ4 and user/password authentication (through `gozenohd`), using
  about 93 KB of its 176 KB heap. The Cardputer runs it over Wi-Fi three
  times: against zenohd, against `gozenohd` with LZ4 and user/password, and
  with the low-latency transport. It uses about 170–195 KB of its 303 KB heap
  after a GC. Over TLS 1.3 (`tls13`) it passes against zenohd/rustls, and
  against `gozenohd` with mutual TLS, each on its own boot. Five sessions in a
  row can run out of memory, for two reasons:
  - lneto v0.4.0, the Wi-Fi TCP/IP stack, keeps closed connections and their
    5 KB of buffers reachable from its port table.
    `examples/boardtest/cardputer/lneto-release-closed-conns.patch` fixes
    that.
  - Free heap fragments into pieces smaller than the contiguous 8 KB stack
    each new goroutine needs. The device reports
  what was negotiated (`Session.Transports()`).
- **TinyGo**: TinyGo 0.44-built binaries publish to a Rust subscriber, both
  as a client (`examples/tinyput`) and as a peer directly connected to a Rust
  peer (`examples/tinypeer`), and as a client over UDP and over WebSocket
  to a Rust router.
  TinyGo limits: no multicast listening (they scout but don't answer
  SCOUTs), no UDP *listeners* (its `ReadFromUDP` reports no source address;
  dialing UDP works), TLS as a TLS 1.3 client only (package `tls13`,
  through `link/tls13link`), no QUIC, no Unix sockets.

## Running the tests

```bash
go test ./...                    # unit tests; interop tests skip themselves

# interop with a live router
docker run -d --name zenohd -p 7447:7447 eclipse/zenoh
export ZENOH_ROUTER=tcp/127.0.0.1:7447

# cross-implementation tests also need a python with the official binding
python -m venv zpy && zpy/bin/pip install 'eclipse-zenoh~=1.10.0'
export ZENOH_PY=$PWD/zpy/bin/python

ZENOH_MULTICAST=1 go test -race -count=1 ./...   # ZENOH_MULTICAST: also test scouting + multicast groups
ZENOH_ROUTER=go ZENOH_PY=... go test ./zenoh/    # same suites, tiny-zenoh-go as the router

# zenoh-pico interop: the tests compile zenoh/testdata/zpico/zpico.c against a zenoh-pico build
git clone -b 1.10.2 https://github.com/eclipse-zenoh/zenoh-pico
cmake -S zenoh-pico -B pico-build -DBUILD_TESTING=OFF -DZ_FEATURE_UNSTABLE_API=1 && cmake --build pico-build
ZENOH_PICO=$PWD/pico-build go test -run Pico ./zenoh/

# storage replication against zenoh-rust: a zenohd running two replicated storages
# (exact command in plugins/storage/replication_test.go), then
ZENOH_REPL=tcp/127.0.0.1:17470 go test -run Replication ./plugins/storage/

# unixpipe + vsock against zenoh-rust (vsock needs the vsock_loopback module)
(cd zenoh/testdata/zrs && cargo build --release)
ZENOH_RS=$PWD/zenoh/testdata/zrs/target/release/zrs go test -run RustLink ./zenoh/

# statistics against zenoh-rust built with its "stats" feature (zenohd releases lack it)
(cd zenoh/testdata/zstatsd && cargo build --release)
ZENOHD_STATS=$PWD/zenoh/testdata/zstatsd/target/release/zstatsd go test -run StatsRust ./zenoh/

# router meshes: one or two zenohd routers not connected to each other
docker run -d --name zenohd2 --net=host eclipse/zenoh -l tcp/127.0.0.1:7448 --no-multicast-scouting
ZENOH_MESH=tcp/127.0.0.1:7447,tcp/127.0.0.1:7448 go test -run Mesh ./zenoh/

tinygo build -o /dev/null ./examples/tinyput    # TinyGo builds the full API

# on a board: see examples/boardtest/{stickc,cardputer}/main.go for the
# routers each needs, then
python examples/boardtest/host.py tcp/127.0.0.1:7447 board/stickc/serial
go run ./examples/boardtest/sim tcp/127.0.0.1:7447 board/sim/plain   # same run on the PC
go run ./examples/zprobe                        # hex-dump a raw handshake
```

## Roadmap

- [x] **M0 — Foundations.** Wire codec and key-expression engine.
- [x] **M1 — Message model & codec.** Transport, network and data messages
      plus extensions, with byte-exact vectors checked against zenohd.
- [x] **M2 — Transport & TCP link.** Handshake, batching, SNs, fragments,
      keep-alive, lease, close.
- [x] **M3 — Client session.** Pub/sub, publishers, PUT/DELETE against zenohd.
- [x] **M4 — Queries & queryables.** REQUEST/RESPONSE, consolidation,
      selectors/parameters, error replies, timeouts.
- [x] **M5 — Declarations & interests** (client side). Key-expr declarations,
      answering router INTERESTs, liveliness.
- [x] **M6a — Peer mode & scouting.** Accept-side handshake, listeners,
      multicast `SCOUT`/`HELLO`, autoconnect, peer-to-peer declaration push
      (initial interest), reconnection.
- [x] **M6b — Router.** Pure-Go router for clients and peers: forwarding,
      interest answering/propagation, query routing with finals and timeouts,
      admin space, `cmd/gozenohd`.
- [x] **M6c — Router meshes.** Router-to-router link-state (OAM gossip,
      network graph, shortest-path trees with zenoh's SipHash edge weights),
      sourced declarations, multi-hop routing, interoperating with zenohd.
- [x] **M6d — Peer gossip & multicast transport.** ✅ gossip scouting
      between peers (zenoh's default, one hop), ✅ multicast transport
      (`JOIN`), ✅ multihop gossip (`scouting/gossip/multihop`). Peer
      link-state routing (`routing/peer/mode: linkstate`) was deprecated in
      zenoh 1.10 and has no effect there, so it isn't implemented.
- [x] **M6e — Regions** (zenoh 1.10). Region name and remote-bound
      handshake extensions; region computation (auto preset and custom
      `gateway.south` filters); routing by region (gateways relay across
      regions, never back into a peer region); single-hop gossip of a
      gateway's south peers; multi-gateway de-duplication; peers as
      gateways of clients and of peer subregions; router subregions (a
      gateway router runs one link-state mesh per region and announces
      each region's entities into the others as its own).
- [x] **M7 — More links.** ✅ UDP, ✅ TLS (+ mTLS; TinyGo: own TLS 1.3
      client), ✅ QUIC (single
      stream, multistream, mixed reliability, datagrams), ✅ WebSocket,
      ✅ Unix sockets, ✅ serial, ✅ named pipes (`unixpipe`), ✅ vsock.
      Not possible with quic-go: zenoh's
      "reliable UDP" (`udp/…?rel=1`), which is QUIC with packet protection
      switched off inside quinn.
- [x] **M8 — Advanced.** ✅ shared memory (Linux), ✅ QoS negotiation &
      priorities, ✅ compression, ✅ multilink, ✅ authentication (usrpwd,
      pubkey), ✅ access control, ✅ low-latency transport, ✅ querier,
      ✅ matching status, ✅ advanced pub/sub (zenoh-ext), ✅ namespaces,
      ✅ locality (allowed origin / destination), ✅ zenoh-ext serialization,
      ✅ interceptors (downsampling, low-pass filter, QoS overwrite),
      ✅ timestamping, ✅ admin space, ✅ query cancellation, ✅ connectivity
      events, ✅ traffic statistics (`stats`).
- [x] **M9 — Router plugins & config.** ✅ zenohd JSON5 config and
      command line, ✅ REST plugin, ✅ storage manager with the memory
      volume, ✅ storage replication (digests and alignment,
      interoperable with zenoh-rust replicas). Not done: backends other
      than memory, admin-space config writes, dynamic plugin loading.

## Package layout

```
tiny-zenoh-go/
├── internal/codec/   # wire primitives (VLE, framing, headers)
├── keyexpr/          # key expressions (match/intersect/include, canon)
├── protocol/         # every message type + extensions, encode/decode
├── transport/        # unicast (QoS, compression, multilink) and multicast transports
├── internal/lz4/     # LZ4 block codec for batch compression
├── shm/              # zenoh shared memory (Linux)
├── link/             # locators, dial/listen: tcp, udp, tls, ws, unixsock-stream, unixpipe, vsock, serial;
│                     # link/quic: quic; link/tls13link: tls/ for TinyGo
├── tls13/            # TLS 1.3 client for TinyGo (X.509, ECDSA, key schedule, records)
├── zenoh/            # public API: Session (client/peer), pub/sub, queries, liveliness, Scout
├── zext/             # zenoh-ext serialization (z_serialize format)
├── plugins/          # rest (REST API), storage (storage manager, memory volume)
├── internal/json5/   # JSON5 → JSON for zenoh config files
├── internal/xxh3/    # XXH3-64 (storage replication fingerprints)
├── cmd/gozenohd/      # pure-Go router
└── examples/         # zprobe, tinycheck, tinyput, tinypeer, tinyserial,
                      # boardtest (device test agent, Rust harness, StickC/Cardputer firmware)
```

## Design principles

- **Pure Go, zero cgo.** Nothing links to the Rust/C Zenoh stacks.
- **TinyGo-friendly.** No `reflect`, no heavy `encoding/*` in the library.
- **Interop is the oracle.** Correctness is measured against real Zenoh nodes.
- **Every layer tested.** Spec examples and captured bytes are test vectors.

## Interop notes (things the spec doesn't make obvious)

- Regions (zenoh 1.10) put two optional extensions into the handshake.
  INIT carries the region name (ZBuf, id 8, 1 to 32 bytes of UTF-8). OPEN
  carries the remote bound (Z64, id 7: 0 north, 1 south), which says which
  side of the sender the receiver stands on. A node sends the bound only
  with a custom `gateway.south`. Both sides then compute the region the
  same way, and a conflict closes the transport.
- Since 1.10 a router no longer relays between two peers attached to it:
  they are in its south peer region and must link to each other, helped by
  the router's single-hop gossip. Earlier gozenohd versions relayed, which
  doubled every sample between peers that were also linked directly.
- With several gateways, zenoh-rust lets only the highest ZID among the
  gateways a peer is linked to forward that peer's traffic across.
  Gateways learn this from the peer's gossip. The rule doesn't cover
  everything: if a peer publishes to routers `a1` and `b2`, a subscriber
  on `a1` gets each sample twice (once directly, once through `b2` and the
  mesh), and tiny-zenoh-go does the same.
- A gateway router keeps a separate link-state mesh per router region (a
  south mesh flags it as a gateway in its link state). The routers south
  of it see an ordinary router; it announces into each mesh, as their
  source, what it knows from outside that mesh. Two gateways between the
  same meshes must not re-announce what they learnt from each other, or a
  declaration would keep itself alive after its source left; tiny-zenoh-go skips
  sources that are themselves nodes of the target mesh. Data
  crosses at the highest-ZID gateway of the subregion unless it entered
  the mesh network at a gateway (its own session, clients, peers), which
  then delivers it, and a router reachable through two meshes gets it
  through one.
- In the Python binding the custom gateway goes in as a whole object,
  `insert_json5("gateway", '{"south": [...]}')`; the path
  `"gateway/south"` is an "unknown key".

- The negotiated batch size **includes** the 2-byte stream length prefix:
  a batch body must be ≤ `batch_size - 2`, or zenohd drops the link.
- For the "32-bit" SN resolution the usable SN space is 28 bits (VLE packs 7
  bits per nominal byte); the initial SN must stay below `2^28`.
- Frame QoS extensions must not be sent unless QoS was negotiated. With QoS,
  each priority has its own reliable and best-effort SN spaces, all starting
  at the same initial SN; the FRAME/FRAGMENT QoS ext (Z64, mandatory, id 1)
  is omitted for the default priority Data (5).
- The network QoS byte 0x00 (priority Control, drop, not express) is a real
  value, distinct from the omitted default 0x05.
- Compression is negotiated per link (INIT unit ext 6); afterwards *every*
  batch, keep-alives and CLOSE included, starts with a header byte (bit 0:
  the rest is one LZ4 block, sent only when smaller). INIT/OPEN are never
  compressed, and a block inflates to at most the batch size.
- Multilink keys are 512-bit RSA, encoded as `zbytes(n LE) zbytes(e LE)`; the
  challenge is a u64 LE encrypted with PKCS#1 v1.5 (Go's `crypto/rsa` refuses
  keys that small, hence a math/big implementation). Links of one transport
  share its SN state; zenoh-rust ignores the initial SN of later links.
  Reliable traffic is sent on links whose locator is reliable, so with TCP +
  UDP, best-effort messages take UDP.
- Multicast: multicast `udp/` endpoints belong in `listen` (zenoh-rust
  refuses them in `connect`). A sender is ignored until its `JOIN`, which
  zenoh-rust sends every 2.5 s with patch 1 (fragments may carry
  First/Drop markers), batch size 8192 and the next SN per channel; QoS on
  multicast is off by default. Peers are told apart by source address, so a
  node's own looped-back datagrams must be filtered by its send socket's
  address, and nodes on one host must use the same interface. zenoh-rust
  sends every publication to the group regardless of subscriptions.
- zenohd lowers the batch size to 49152 and returns the lease in seconds.
- Peers don't send INTERESTs to each other on connect: both sides push all
  their subscribers/queryables/tokens and close the burst with a `D_FINAL`
  carrying interest id 0. A Rust peer's `open()` waits for that `D_FINAL`.
- Clients and peers push their declarations to a router but never get pushed
  any: they send INTERESTs (publisher → subscribers, querier → queryables,
  liveliness subscriber/get → tokens) and a declared publisher sends nothing
  until a matching subscriber is declared to it.
- Routers never send each other INTERESTs. Each router announces itself as
  the *source* of its local entities (declarations with id 0 and the
  `NodeId` extension = its index in the receiver's view of the graph),
  forwarded down the source's shortest-path tree. Data and queries carry the
  `NodeId` of the router where they entered the mesh and follow that
  router's tree. Edge weights are 100 × (1 + 0.01 × a SipHash-1-3 of the two
  ZIDs), so every router computes the same trees.
- A liveliness subscriber without history sends a *future-only* interest; a
  router still sends it an `U_TOKEN` (id 0 + key) when a token disappears.
- On UDP each batch is one datagram with no length prefix, and the batch size
  is capped at the link MTU on both sides (65487 on Linux). Frames keep their
  reliable/best-effort flag; over UDP "reliable" is just a separate SN space.
- WebSocket links aren't streamed: one binary message per batch, no length
  prefix, any URL path; zenoh-rust drops the link on text or ping frames.
- zenoh's QUIC variants are negotiated by ALPN, server preference first:
  `zenoh-ms-mr`, `zenoh-ms`, `zenoh-mr`, `zenoh`, `hq-29`. The list comes from the
  locator metadata `multistream` (default `auto`) and `mixed_rel` (default
  `0`). Single stream is framed exactly like TCP.
- Multistream: the bidirectional stream carries priority Control (the
  handshake, keep-alives). Each side opens 7 unidirectional streams at once,
  in priority order, and stream number n (from 0) carries priority n+1, so a
  node must allow 7 incoming unidirectional streams. Without QoS everything
  stays on the Control stream. Order across priorities is not kept.
- Mixed reliability: the same connection's QUIC datagrams become a second,
  best-effort link of the transport, with no handshake of its own. Batches
  there are 1150 bytes at most, and keep-alives and lease apply per link.
  `?rel=0` makes the whole link datagrams, handshake included (ALPN `hq-29`).
- zenoh-rust rejects a locator whose metadata keys aren't sorted (gossip
  fails with "failed to decode link state"), so locators are canonicalized
  when parsed.
- SHM: a PUT/ERR carrying SHM has the mandatory unit ext `0x12` and its
  payload becomes a list of slices (`zint` count; per slice a kind — 0 raw,
  1 SHM pointer — and length-prefixed bytes); a query body is preceded by unit
  ext `0x04`. A pointer is `data_len, metadata segment id, index, generation`.
  The sender bumps the chunk refcount per receiver, and keeps the watchdog
  confirmed until the receiver decrements the sender's handoff counter for
  that priority. zenoh-rust **mlocks** its pool and silently stops sending
  SHM if that fails: its 16 MiB default exceeds the common 8 MiB
  `RLIMIT_MEMLOCK`. tiny-zenoh-go's mlock is best effort.
- Serial frames are `COBS(flags | len u16 LE | payload | CRC-32 LE) 0x00`.
  The CRC is computed the reflected way but with the *unreflected*
  polynomial 0x04C11DB7 (Go: `crc32.MakeTable(0x04C11DB7)`), so it is not
  IEEE CRC-32. Exclusive tty mode (TIOCEXCL) belongs to the tty, so it must
  be cleared (TIOCNXCL) on close or the port stays busy.
- Peer gossip reuses the router link-state codec (OAM id 1). A peer keeps
  itself (psid 0, sn starting at 1) plus one node per neighbour; when a
  link comes up it sends that neighbour every node *with* zid and locators
  (and its own links only if the neighbour is a router), and forwards each
  neighbour's own state, once it carries new locators, to the others. The
  advertised locators leave out loopback addresses only when they come from
  expanding `0.0.0.0`. A received state whose locators are present makes the
  peer connect to that node (default roles: router, peer and client).
- Two nodes that discover each other at once both dial. zenoh-rust registers
  a transport only when a handshake *completes*, so if a dialer refuses its
  own link as a duplicate after OPEN, the peer has already set up that
  transport, and tearing it down can take the other, good, link with it.
  tiny-zenoh-go refuses duplicates right after INIT ACK (unless multilink needs
  the key proof from OPEN). Between two Go nodes the smaller ZID also waits
  ~1 s before dialing, else each could keep its own handshake and refuse
  the other's, leaving no link.
- Authentication is INIT/OPEN extension `0x3`, a chained list of
  sub-extensions: pubkey `0x1` (the multilink RSA exchange plus a key
  allow-list), usrpwd `0x2` (unit → z64 nonce → `zbytes(user)
  zbytes(HMAC(nonce as 8 LE bytes, password))` → unit). The HMAC is over
  **SHA3-256** (zenoh-crypto), not SHA-256. A node with usrpwd configured
  (even only its own credentials) demands credentials from every node
  dialing it. zenoh-rust 1.10's pubkey *acceptor* rejects every key when
  configured from config (its allow-list starts empty and
  `known_keys_file` is never read), so pubkey works only with Rust dialing.
- Access control matches zenoh-rust: each subject expands into the product
  of its lists; a node is checked against every subject it matches and one
  allowing subject is enough; per subject, a deny rule *including* the key
  wins, then allow if default-allow or an allow rule includes it. "Includes"
  means the rule covers the whole message key expression, so allowing
  `a/b/**` does not allow subscribing to `a/**`. Interests for tokens with
  no key expression are denied.
- zenoh-rust orders ZIDs as uhlc does: lexicographically over their 16
  little-endian bytes, not as numbers. It matters for the link-weight
  jitter (the smaller ZID is hashed first) and for electing the forwarding
  region gateway (the largest ZID); found through the admin space, where
  the two implementations' edge weights differed for half of the ZID pairs.
- Key expressions accept every character but `#` and `?`; `*` only in `*`,
  `**` and `$*`, `$` only in `$*` (zenoh-rust's rules: `south:0:peer` is a
  valid chunk).
- The admin space mirrors zenoh-rust's: JSON objects with sorted keys (they
  go through serde_json maps) except entity sources, which keep
  routers/peers/clients order; entities of a south region also list the
  node itself in its own mode (`add_north_source`); publishers and queriers
  come from the future INTERESTs of the faces we are the gateway of; the
  metrics text without statistics keeps zenoh's stray space after
  `# EOF`. Without plugins,
  `plugins` is null. Admin-space writes to `config/**` are not supported.
  The session-local space is not visible to a session with a `Namespace`.
- Statistics follow zenoh-stats: series appear on first use (counters of a
  link from its start, network-message series on their first message); a
  link's counters move to its transport (no link label) when it goes; a
  closed transport stays a minute (`?disconnected=true`, `disconnected="true"`,
  resumed if the same peer comes back), then joins label-less totals; label
  values zenoh-rust leaves unset print as `""`. The histogram `le` label
  comes right after the constant labels, as `prometheus_client` writes it.
  We count bytes as sent on the wire (zenoh-rust: outgoing batches before
  compression). A multicast group is one transport for all its senders
  (zenoh-rust splits received traffic per sender).
- Interceptors and access control form one chain, in zenoh-rust's order:
  downsampling, access control, QoS overwrite, low-pass filter. A query
  dropped on the way in is answered with a RESPONSE_FINAL, a dropped
  interest with a D_FINAL; one dropped on the way out counts as answered by
  that face, so queriers don't wait for their timeout. Downsampling uses the
  first rule whose key expression *intersects* the message key; low-pass
  and QoS overwrite rules must *include* it. A QoS overwrite applies to
  puts, deletes and queries (zenoh has no reply overwrite). zenoh-rust 1.10
  ignores negative priority increments (`if 1 >= 0` in qos_overwrite.rs);
  we apply them. Timestamping is on by default only in router mode
  (`timestamping/enabled`), stamps PUTs (not DELs) that have none and the
  node's own publications, and treats a timestamp more than 500 ms ahead
  of the node's clock (uhlc's default delta) as from the future.
- Matching status comes from the INTERESTs publishers (`KeyExprs |
  Subscribers`) and queriers (`KeyExprs | Queryables`) send their routers
  with mode CurrentFuture on their key; the router answers with aggregated
  declarations and keeps them current, and undeclaring sends a `Final`
  interest. For an `ALL_COMPLETE` querier only a *complete* queryable whose
  key *includes* the querier's key counts. A listener declared while
  matching is told so at once.
- Chunks starting with `@` are *verbatim*: `*`, `$*` and `**` never match
  them, only the identical chunk does (`a/**` does not cover `a/@adv/x`).
  zenoh-ext relies on it to keep its `@adv` traffic away from subscribers.
- Advanced pub/sub is plain zenoh traffic under `K/@adv/pub/<zid>/<eid>/<meta>`
  (`uhlc` for `<eid>` when samples are ordered by timestamp, `_` for no
  metadata): the publisher's cache is a queryable there answering
  `_sn=a..b`, `_max=n` and `_time=[..]` with the cached samples on key `K`
  (so subscribers query with `_anyke`, which lets replies use keys outside
  the query's), its liveliness token has the same key, and its heartbeat
  publishes the last sequence number as a u32 LE there. Samples carry the
  SourceInfo extension `{zid, eid, sn}`, sn counting from 0. A subscriber
  with no state that hears a heartbeat asks for everything up to it.
- Low latency (INIT ext `0x5`, both sides must ask): zenoh-rust refuses it
  together with QoS, so `Config.LowLatency` turns QoS off; messages are
  never batched or fragmented, so each must fit the batch size, and
  compression is not used.
- None of the INIT/OPEN extensions are mandatory, so an acceptor can answer
  with none of them; the INIT ACK cookie is opaque to the initiator.
- `unixpipe` and `vsock` are not in zenoh-rust's default features (neither
  zenohd's image nor the Python binding has them), so their Rust interop
  tests use a small zenoh-rust peer built with both (`zenoh/testdata/zrs`,
  `ZENOH_RS`); the unixpipe handshake is also played by hand in
  `link/unixpipe_test.go`. A non-blocking vsock connect must check the
  socket on the first poller callback: `RawConn.Write` resets write
  readiness first, so a connect that already completed gives no new edge
  (seen as sporadic 5 s dial timeouts against Rust's listener). The pipe reader opens its FIFO read-write, so
  it never sees EOF: a vanished peer is noticed by the session lease.
- INIT ext `0x7` is the *patch* level (z64, 1 today): both sides use the
  lower one, and at patch ≥ 1 the first FRAGMENT of every message carries
  the `First` marker (FRAGMENT ext `0x2`) and an abandoned message a `Drop`
  (`0x3`), so receivers resync after a loss. zenoh-pico 1.10 listeners
  answer a SYN *without* the extension with an INIT ACK whose Z flag is set
  but that has no extension, so the decoder accepts an empty list there.
- **Aggregate interests** (INTEREST option `0x80`): zenoh-pico's publishers
  and queriers ask the router for subscribers/queryables this way, and count
  only declarations whose key *equals* theirs. A router answers with one
  declaration on the interest's own key (id 0 for Current-only, else a fresh
  id, carrying the interest id) as long as anything intersecting it exists,
  and undeclares it when the last one goes; a queryable aggregate is
  complete if a complete queryable *includes* the key. Answering with the
  real keys (as for plain interests) makes pico publishers drop everything.
- Multicast group members must agree on the batch size: zenoh-rust and
  tiny-zenoh-go ignore JOINs announcing another one. Both use
  min(batch size, 8192); zenoh-pico uses 2048 (`Z_BATCH_MULTICAST_SIZE`).
- zenoh-pico quirks seen while testing (pico ↔ zenohd too, not Go-specific):
  a put with congestion control *drop* fails whenever pico's TX lock is busy
  (right after `z_open`, it is: pico sends its interests), so the stock
  `z_put` example often fails; a pico liveliness get with two or more
  matching tokens gets the same replies re-sent by zenohd 1.10.2 until it
  times out (gozenohd replies once); a pico *peer* attached to a router
  exchanges nothing with the router's clients (pico clients do).

- Storage replication is the storage manager's own protocol over plain
  pub/sub and queries. The configuration fingerprint is XXH3-64 over the
  storage key expression, the strip prefix, then the interval and the
  propagation delay in milliseconds as **u128** little-endian, the
  sub-interval count (usize) and hot/warm (u64); an event's is XXH3-64 of
  its stripped key, the NTP64 time and the 16-byte uhlc ID. Digests,
  alignment queries and replies are bincode 1 (`HashMap`s as u64 length +
  pairs, enum variants as u32, timestamps as u64 + 16 raw bytes, the
  Discovery ZID as its hex string); queries and replies carry them in
  attachments. Wildcard updates are logged under the full (unstripped)
  wildcard key. Two places where tiny-zenoh-go follows zenoh's intent
  rather than its 1.10 code (fingerprints are unaffected): a recent delete
  still in the latest-updates cache keeps an older put out (zenoh only
  consults the cache when the new event is newer), and a put overridden
  by a newer wildcard delete is deleted rather than stored with an empty
  payload.

## Spec references

- Wire primitives: <https://spec.zenoh.io/spec/1.0.0/wire/primitives.html>
- Message format: <https://spec.zenoh.io/spec/1.0.0/wire/message-format.html>
- Key expressions: <https://spec.zenoh.io/spec/1.0.0/concepts/key-expressions.html>
- Session establishment: <https://spec.zenoh.io/spec/1.0.0/session/open-accept.html>

## License

Apache License 2.0 (see [LICENSE](LICENSE)). This is a Go implementation
of [Eclipse Zenoh](https://zenoh.io), written largely by an AI model from
the Zenoh specification and the zenoh-rust sources (EPL-2.0 OR Apache-2.0,
ZettaScale Technology and the Eclipse Zenoh contributors), whose behaviour it
reproduces; see [NOTICE](NOTICE).
