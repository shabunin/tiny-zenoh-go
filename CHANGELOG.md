# Changelog

All notable changes to this project are listed here. Versions follow
[Semantic Versioning](https://semver.org); before 1.0 the API may change in
minor releases.

## Unreleased

### Changed

- `tzd` prints its plugins stopping on shutdown (`storage <name> stopped`,
  `volume <name> closed`, `REST API stopped`). Exit status: 0 after
  SIGINT/SIGTERM or `--version`/`-h`, 2 for a bad command line or
  configuration, 1 when it can't start (listen or plugin failure).

### Tests

- `tzd` end to end: every flag, bad command lines and configurations, auth
  (usrpwd, pubkey, mutual TLS), ACL, plugins and admin-space writes,
  two joined routers, and the real binary's signals. Package coverage
  58% → 88%.
- Lab `soak` scenario (`lab/run.sh soak`): hours of traffic, liveliness and
  link faults on a containerlab mesh with memory trends. 12 h on 49 tzd
  routers: every flow and token recovered after every fault, no goroutine
  or heap growth (lab/README.md).

## v0.1.1 — goroutine leak fix

### Fixed

- Channel handlers (`DeclareSubscriberChan`, channel queries, queriers and
  liveliness subscribers) no longer leak a goroutine when the reader stops
  reading: `Undeclare` (or closing the session) now ends delivery at once,
  drops samples still queued and closes the channel. Previously the
  forwarding goroutine stayed blocked on the unread channel forever (seen in
  the REST plugin's SSE handler after a client disconnected).
- `tzd`: the debug signal handler is installed by `main`, not at package
  init, so importing the package doesn't start a goroutine.

### Tests

- Every package's tests now fail if they leave goroutines behind
  (`internal/leaktest`; `LEAKTEST=off` disables, `LEAKTEST_WAIT` sets the
  grace period). Several test-only leaks fixed.
- `TestMeshSim`: an in-process 16-router mesh with chords, proxied links,
  mass cuts, flaps and router restarts, checking convergence, liveliness and
  exactly-once delivery. `ZENOH_MESHSIM=large` runs a 49-router variant;
  `ZENOH_MESHSIM_SEED` replays a run.

## v0.1.0 — first release

A pure-Go implementation of Eclipse Zenoh (wire protocol `0x09`, Zenoh 1.0
specification), interoperating with zenoh-rust / zenohd 1.10.2 and
zenoh-pico. The library and the `tzd` router also build with TinyGo.

Written by Anthropic's Claude Opus 5.5 model (with Claude Code), under the
direction of the project's maintainer, from the Zenoh specification and the
sources of [Eclipse Zenoh](https://zenoh.io) (zenoh-rust, zenoh-pico), whose
design, wire formats and behaviour it reproduces; thanks to the Zenoh
project. Not affiliated with or endorsed by the Eclipse Foundation,
ZettaScale Technology or the Eclipse Zenoh project.

Licensed `EPL-2.0 OR Apache-2.0`, the same terms as Eclipse Zenoh.

### Sessions and API

- Client, peer and router modes; multicast and gossip scouting, autoconnect,
  reconnection.
- Pub/sub (publishers, subscribers, put/delete), queries (get, queryables,
  querier, consolidation, timeouts, cancellation), liveliness tokens and
  subscribers, matching status, attachments, encodings, HLC timestamps.
- Advanced pub/sub (zenoh-ext compatible: publisher cache, sequence numbers,
  heartbeats, history, lost-sample recovery, miss listeners) and zenoh-ext
  serialization.
- Namespaces, locality, access control, interceptors (downsampling, low-pass
  filter, QoS overwrite), timestamping, admin space, connectivity events,
  traffic statistics compatible with zenohd's `stats` feature.

### Router (`tzd`)

- Router meshes with link-state routing (shortest-path trees, sourced
  declarations), peers and clients attached; zenoh 1.10 regions and gateways.
- zenohd-compatible JSON5 configuration and command line.
- Plugins compiled in: REST, storage manager with replication (interoperable
  with zenoh-rust replicas), backends memory, fs, InfluxDB 1.x/2.x and S3.
- Mesh robustness found and fixed in a containerlab lab (up to 225 routers):
  ordered control messages per neighbour, routers keep their graph slot when
  they come back, re-joins held back briefly after a link loss, and mesh data
  arriving off its tree is dropped (stops duplicate copies and loops injected
  by mis-routing neighbours).

### Transport and links

- Unicast and multicast transports: batching, fragmentation, QoS priorities,
  LZ4 compression, multilink, low latency, user/password and public-key
  authentication, transmission queues with congestion control, graceful
  close.
- Links: TCP, UDP, UDP multicast, TLS (incl. mutual TLS), QUIC (via quic-go),
  WebSocket, Unix sockets, named pipes, vsock, serial (z-serial framing);
  shared memory on Linux (compatible with zenoh-rust's SHM).

### TinyGo

- The full `zenoh` API, `tzd` and board examples build with TinyGo 0.44;
  verified on an M5StickC (ESP32, serial) and an M5Stack Cardputer
  (ESP32-S3, Wi-Fi) against zenohd and tzd.
- `tls13`: own TLS 1.3 client (incl. client certificates) for TinyGo builds.

### Known limitations

See "Project status" in README.md: not audited, no soak tests yet; not
implemented are zenoh's "reliable UDP", the RocksDB backend, dynamic plugin
loading, YAML config, IPv6 multicast scouting, zero-copy SHM receive and a
TLS server on TinyGo.
