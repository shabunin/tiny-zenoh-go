# Network lab

[containerlab](https://containerlab.dev) topologies of zenoh routers (tzd,
zenohd, or a mix of both) with real links between the containers, so that
routing can be watched while links are cut and restored. The routers and
the client hosts have no management network: they reach each other only
over the lab links.

On the lab host: docker, containerlab and passwordless sudo. Then:

```bash
./build.sh                                  # here: static tzd and zlab into image/bin
./gen.py ring --impl tzd,zenohd,tzd,zenohd --name ring-mixed
rsync -a ./ labhost:tzlab/                  # the lab directory to the lab host
# on the lab host, in tzlab/:
sudo ./run.sh image                         # the tzlab image (tzd + zlab, on alpine)
sudo ./run.sh up ring-mixed
sudo ./run.sh failover ring-mixed           # results/ring-mixed-failover/
sudo ./run.sh down ring-mixed
```

Topologies for the other scenarios (or deploy one of the saved ones in
`presets/`, see `presets/README.md`):

```bash
./gen.py ring --impl tzd,tzd,zenohd,zenohd --storage r1,r3 --name part-mixed
./gen.py gateway --impl tzd,zenohd,tzd --gossip --name gw-mixed
./gen.py line --impl tzd,zenohd,tzd --wmem 65536 --name line-mixed
./gen.py grid --impl tzd,zenohd --size 10x10 --hosts 8 --name grid100-mixed
./gen.py random --impl tzd --size 60 --degree 3 --seed 7 --hosts 8 --name rnd60-tzd
```

`gen.py` makes a ring (or a line) of routers r1..rN, each dialing the next
one, with client host c1 on r1 and c2 on the router farthest from it; the
routers use a 2 s lease. `zlab` is the traffic agent: `zlab pub` sends
numbered, timestamped samples at a fixed rate and `zlab sub` reports what
arrived (losses, duplicates, reordering, the longest silence, latency).

## Scenarios

**failover**: c1 publishes 1000 samples/s (reliable) to c2 across the ring;
twice, the busiest of r1's router links is cut for 8 s and restored.

Results on a 4-core VM, ring of 4 routers:

| routers | outage per cut | lost on restore | duplicates | reordered |
|---|---|---|---|---|
| tzd ×4 | 2.3 s, 1.6 s | 0 | 0 | 0 |
| zenohd ×4 | 1.9 s, 1.8 s | 1 | 0 | 0 |
| tzd, zenohd, tzd, zenohd | 2.0 s, 2.0 s | 0 | 0 | 0 |

A cut link is noticed when its lease (2 s) runs out; what was sent over it
meanwhile is lost (zenoh's reliability is per hop), then the routers'
link-state trees route around it. When the link comes back the traffic
moves back to it without loss or duplicates.

**partition**: a ring of 4 with replicated memory storages (on lab/store/**)
on r1 and r3. c1 publishes 200 samples/s to c2 and both hold a liveliness
token; the ring is split in two halves ({r1, r4, c1} | {r2, r3, c2}) for
11 s, each side writes to its storage (both write `x`, c2 a second later),
the ring heals, then is split again for 5 s to read each half's storage
alone.

| routers | data back after heal | liveliness | storages after heal |
|---|---|---|---|
| tzd ×4 | 1.3 s, 0.0 s | delete 2.2 s after the split, put 1.3 s after heal | converged (x = from-c2) |
| zenohd ×4 | 1.5 s, 3.5 s (+21 lost after) | same, put 1.5 s / 3.4 s after heal | converged |
| tzd, tzd, zenohd, zenohd | 1.2 s, 3.5 s | same | converged |

During the split each half reads only its own writes; after the heal both
storages hold every write and agree on the later `x`, and they still do
when split again (each one alone). How fast data is back depends on when
the dialing router retries (backoff 1 s doubling to 4 s for both).

**gateway**: routers r1 and r2 are both gateways of a peer subregion of
peers p1 and p2 (linked to each other and to both gateways), r3 is north
of them with client c1; gossip on. p1 publishes 500 samples/s to c1 and
p2, c1 to p2; after 8 s every link of the gateway that forwards is cut
for 12 s.

| r1, r2, r3 | p1 → c1 silence | c1 → p2 silence | within the cell | duplicates |
|---|---|---|---|---|
| tzd ×3 | 4.3 s | 2.8 s | 0 lost | 0 |
| zenohd ×3 | 4.3 s | 2.8 s | 1 lost | 0 |
| tzd, zenohd, tzd | 4.4 s | 2.8 s | 0 lost | 0 |

The peers are Go (zlab). The other gateway takes over once the peers tell
it they lost the first one: Go peers announce a lost link in their gossip
(zenoh-rust 1.10 peers do not, so behind zenoh-rust peers neither gateway
takes over until the lost one is back). Before that change there was no
failover at all; the lab also found a liveliness bug (a client saw its
own new token deleted through tzd) and a peer that does not listen
waiting a second before dialing a gossiped peer.

**netem**: a line c1 – r1 – r2 – r3 – c2; c1 publishes 1000 samples/s
(64 B, reliable, congestion control Drop unless said) for 10 s while the
r1–r2 link is degraded with `tc netem` on both ends, one condition at a
time. The routers' TCP send buffers are capped at 64 KB (`--wmem`): with
the kernel's default (autotuned up to 4 MB) a slow link just queues
seconds of data in the socket and no congestion control ever acts.

Missing samples (and the longest silence) per condition:

| r1–r2 link | tzd ×3 | zenohd ×3 | tzd, zenohd, tzd |
|---|---|---|---|
| clean | 0 (4 ms) | 0 (7 ms) | 0 (4 ms) |
| delay 25 ms ± 5 ms | 0, p99 37 ms | 0, p99 43 ms | 0, p99 42 ms |
| loss 1% | 0 (0.2 s) | 0 (0.2 s) | 0 (0.2 s) |
| loss 5% | 0 (0.6 s) | 0 (0.3 s) | 310 (1.4 s) |
| WAN: 25 ms ± 5 ms, loss 2% | 0, p99 98 ms | 0, p99 99 ms | 0, p99 111 ms |
| loss 20% | 2973, reset | 578 | 3932, reset |
| dark for 1 s | 374 | 101, reset | 291 |
| dark for 1.5 s | 2470, reset | 2665, reset | 2604, reset |
| 1 Mbit/s, 256 B samples (2× too many), Drop | 5678, p99 1.4 s | 5239, p99 1.5 s | 5682, p99 1.3 s |
| same, Block | 0, p99 15 s | 0, p99 13 s | 0, p99 15 s |

TCP retransmits hide light loss and short delay entirely (no sample lost,
latency up by a retransmission timeout). What is lost is lost on purpose:
under heavy loss, a blackout or a bandwidth cap the link stalls, r1's queue
fills and Drop drops (Block delivers everything, late). A blackout longer
than about 1.4 s outlives the 2 s lease: TCP's retransmission backoff (0.2,
0.4, 0.8 s, then 1.6 s) leaves the link silent past it, the transport is
reset and its queue lost; loss 20% does the same now and then. The three
behave alike; the numbers vary from run to run where TCP stalls.

The lab also showed that a publisher closing its session right after a
burst lost what was still in the socket buffers (the router's next
keep-alive met the closed socket and reset the connection): Close now
half-closes TCP/TLS/unix links after its CLOSE and waits, up to the lease,
for the other side to read it all (`zenoh.Config.CloseLinger`). A Go
publisher closing at once on the capped link with Block: 5228 of 10001
delivered before, 8973 now (the rest was still queued when the 10 s lease
ran out).

**scale**: a grid (`--size RxC`) or a random connected graph (`--size N
--degree D`) of routers with hosts h1..h8 (two opposite corners, the
centre or busiest router, the rest spread out). The idle control traffic
and the routers' memory are measured, then every host publishes 100
samples/s and subscribes to everyone's (64 flows) and holds a liveliness
token; the centre router is cut off for 10 s, then every router link at
once for 5–6 s.

| routers | RSS per router | idle control traffic per link | centre cut: worst flow | every link cut: all flows back | flows never back | liveliness after 20 s |
|---|---|---|---|---|---|---|
| 49 tzd (7x7) | 21 MiB | 241 B/s | 1.5 s | 4.9 s | 0 | complete |
| 49 zenohd | 12 MiB | 484 B/s | 1.2 s | 3.8 s | 2 | 1 token missing |
| 49 mixed | 17 MiB | 401 B/s | 1.2 s | 4.8 s | 5 | 1 token missing |
| 49 mixed, rejoin fixes | 13 MiB | 404 B/s | 1.1 s | 4.2 s | 0 | complete |
| 100 tzd (10x10) | 23 MiB | 257 B/s | 1.3 s | 6.0 s | 0 | complete |
| 100 zenohd | 12 MiB | 512 B/s | 0 | 4.2 s | 3 | 1 token missing |
| 100 mixed | 18 MiB | 400 B/s | 0 | 6.4 s | 1 | complete |
| 60 mixed, random | 17 MiB | 381 B/s | 1.5 s | 3.9 s | 1 | 2 tokens missing |
| 225 tzd (15x15), cut 8 s | 16 MiB | 256 B/s | 1.5 s | 7.4 s, 8.3 s (2 runs) | 0 | complete |

(Flows never back: still nothing 20 s after the restore; zenohd's flows
and tokens come back later or not before the agents end.) The hosts are Go
clients (zlab), which matters where the centre router is their own: h3
reconnects (see below).

(RSS includes the mapped binary, about 9 MiB for tzd and 10 MiB for
zenohd, shared between the containers; see the memory section below. The
225-router row is after the fixes below; the others before the last two.)

The scale runs found five bugs, all fixed:

- a client whose router went away for longer than the lease shut its
  session down instead of reconnecting (zenoh-rust clients reconnect);
- after every link was cut and restored, tzd routers held data published
  after the reconnection up to 8 s and delivered it in a burst: each
  router recomputed all its trees for every link state, under the session
  lock (seen with goroutine dumps: `docker kill -s USR1` on a tzd
  container). The trees now cost 15–30x less (adjacency built once per
  computation) and are recomputed 100 ms after a change, once per burst,
  as zenoh-rust does; 100 routers came back in 20.9 s before, 6.0 s now;
- in mixed meshes the burst left up to 478k duplicates (now 3.8k);
- a 15x15 grid never settled and froze the VM: handling a link state
  cost O(N^2) (nodes looked up by scanning all of them, for every link of
  every state, stale duplicates included, and each of those rescheduled
  the trees), and each tree's next hops cost O(N^2) more. The routers fell
  behind, leases expired, the churn fed itself (load past 1000) and memory
  ran out. Now nodes are indexed, stale lists are dropped at once, next
  hops take O(N) per tree: one update at 225 routers 0.83 ms -> 25 us, the
  144-router lab forms at load 5 instead of 115 (zenohd: 47);
- every router that left the mesh and came back took a new slot in the
  link-state graph, so the trees (N x N) grew with every mass reconnect:
  60 MiB of heap on the 225-grid's centre after three. Slots are reused
  (as zenoh-rust's StableGraph does); two runs in a row now leave the
  routers at 16 MiB.

In a mixed mesh (as with zenohd alone), a flow sometimes stayed broken
after every link was cut and restored: 0 to 5 of 64 in repeated runs,
never with tzd alone. **recut** (below) found why: whole subtrees of the
mesh had lost one router's subscribers and tokens for good. zenoh-rust
sends a router the declarations it lacks only when that router becomes a
new child of a tree; it tells children apart by graph slot and recomputes
the trees at most every 100 ms. A router whose link went down and came
back within one computation keeps its slot, is not new, and never gets
back what it dropped with the link. That happens all the time after a
mass cut: both zenohd and tzd redial at once, and zenohd also adds a new
outbound link to a transport it has not expired yet (the lease ran out
at the other end), which closes soon after and takes the new link along.
Seen with `--trace` (tzd's `ZENOH_MESH_TRACE`) and zenohd's debug log:
r23 (tzd) got r16 (zenohd) back 5 ms after losing it, and r16 never sent
it its subscribers again. tzd now

- refuses a new transport from a neighbour router for 500 ms after the
  link to it went down; the dialer retries a second later, once the
  trees went without the link;
- tells its own trees' children apart by link as well as ZID, so a
  child that flapped between two computations gets everything again;
- gives a router that comes back the graph slot (our NodeId for it) it
  had: zenoh-rust neighbours never forget a router's old NodeId and map
  it back by the first one that names it (Network::add_node), so a
  renumbered router could have its data routed along another router's
  tree.

recut on 7x7 grids, 4 to 10 rounds each:

| mesh | rounds with entries lost, before | after |
|---|---|---|
| 49 tzd | 0 of 4 | |
| 49 zenohd | 4 of 4 | |
| 49 mixed | 8 of 8 | 2 of 20 |

and scale on 49 mixed: 0 flows never back (5 before), liveliness
complete. Both losses left came from a zenohd parent (r22 to r23, r2/r8
to r9), which only zenoh-rust can fix. On 10x10 grids zenoh-rust's side
dominates: pure zenohd loses entries in 5 of 5 rounds, mixed in 6 of 9,
pure tzd in none of 19; zenohd also logs declarations it does not pass on
("Error propagating sub ...: cannot get index of ...") and link states it
cannot map.

On 10x10 grids with tzd alone, a flow sometimes stayed broken with every
router's tables complete: h6 (r32) to h2 (r100), each time. `STOP=1`
left the mesh broken to look at, and on r32's tree r44 sent the data to
r54 and r54 back to r44, for good: the routers' link-state graphs
disagreed (20 lacked the link r36-r46, 3 lacked r99-r100, the rest had
both), so their trees did. tzd built link states under the session lock
and sent them after releasing it, so two goroutines could send theirs to
a neighbour in the wrong order. A state naming a router by a NodeId
whose mapping (the message with its ZID) was still on its way lost that
link at the receiver, which nonetheless kept it as the newest state and
passed it on; the right copy came after with the same sequence number,
as old news. Every control message to a face now goes through its own
queue, filled under the lock and sent in order (outbox, face.enqueue).
The mesh trace showed 107 link states naming unmapped NodeIds in the
first two rounds before the fix, and none in 12 rounds after; with the fix
there were no broken flows and no missing entries in 12 of 12 rounds
(it broke by round 3 in each run before), and every router's graph was
the grid. On 10x10 mixed after the fix, zenohd logged "Received LinkState
... with unknown node mapping" only about zenohd senders (25 times in 8
rounds); flows still broke in 4 of 8 rounds (entries lost twice), and at
the end every router's graph was the grid, so what is left there is
not this.

One run of scale on 100 mixed also had a data loop on one router's tree
after the mass restore (1.7 M duplicates; two reruns: 27 k, 28 k). With
the fix above, three reruns had no such loop but still 17 k, 56 k and 71 k
duplicates. Following one router's data (h5 at r19) through every tzd
router's per-neighbour counters: copies came back into the mesh from zenohd
routers (r18, and r70 into r69), tagged with trees on which the tzd router
receiving them does not have the sender as its parent, then went on to
every subscriber again; r55 (tzd) and its zenohd neighbours r54 and r56
bounced data between them at about 5,000 messages/s each way. Every graph
was the grid and no tree looped. zenohd maps a neighbour's NodeIds to its
own through a table it refreshes, when it adds a router back, only for the
first NodeId that ever named it, so data can go on the wrong tree for as
long as the link lives; pure zenohd
10x10 had 85 k duplicates against tzd's 3.5 k. tzd now drops mesh data
that comes from a neighbour that is not its parent on the data's tree, or
that comes back to its root (meshOffTreeLocked). scale on 100, three runs
each:

| mesh | duplicates | flows never back | all flows back after the restore |
|---|---|---|---|
| mixed, before | 17 k, 56 k, 71 k | 5, 1, 4 | 4.7, 4.6, 4.6 s |
| mixed, with the check | 2.1 k, 4.8 k, 0.2 k | 3, 5, 2 | 4.6, 4.6, 4.6 s |
| tzd, with the check (one run) | 11 | 0 | 4.6 s |

The flows never back in mixed meshes are zenohd's (new children told
apart by slot, and a redial joined to a stale transport; both above). Runs 2 and 3 start from the mesh the run before left,
broken flows included.

**recut**: `ROUNDS=n` (default 4) times, every router link is cut for 5 s
and restored while every host publishes to every other and holds a
token; 20 s later h1 dumps every router's subscriber and token tables
(`@/*/router/{subscriber,token}/lab/**`). The summary lists the flows
still broken and, per router, the hosts' routers missing from its
tables; `SUCC=1` also dumps the next hops on the hosts' routers' trees
and walks each broken flow's path. `STOP=1` stops cutting after the first
round that leaves a flow broken (from the subscribers' per-second ticks)
and keeps the traffic going 15 min more, to look at the mesh as it is;
`run.sh recut-summary TOPO` prints the summary of the last run again.
gen.py gives grid/random routers fixed ZIDs (r22 is e00016) so the dumps
read.

What the lab host takes: a 4-core VM with 16 GB runs 225 tzd routers and
10 hosts in about 3 GB, deploying in 90 s; idle, the routers take 0.1% CPU
each, and the traffic above keeps 3 of the 4 cores busy. Memory per node
is nearly flat with the mesh size (below), so RAM would allow well over
500; CPU while a mesh forms and the deploy time (containerlab creates
about 3 nodes per second) set the practical limit first. `run.sh up`
starts a watchdog that kills the lab when the host's available memory
drops below `GUARD_MIB` (default 2000).

**Memory, tzd vs zenohd** (idle grids, per router; anon = the process's
own memory, what each router really costs; host = the host's memory taken
by the whole lab per router, kernel and the 8 host agents included):

| routers | tzd anon | zenohd anon | tzd host | zenohd host |
|---|---|---|---|---|
| 25 | 3.3 MiB | 1.2 MiB | 10.0 MiB | 8.1 MiB |
| 49 | 5.3 MiB | 1.5 MiB | 11.2 MiB | 7.0 MiB |
| 100 | 6.7 MiB | 2.0 MiB | 12.6 MiB | 7.6 MiB |
| 144 | 6.7 MiB | 2.5 MiB | 12.9 MiB | 7.7 MiB |
| 225 | 7.3 MiB | | 13.0 MiB | |

A tzd router costs about 5 MiB more than a zenohd one, and that is the Go
runtime rather than tzd's state: on 144 routers the live heap is 2–4 MiB
(the link-state graph and trees under 0.5 MiB), and the rest is the GC's
4 MiB minimum heap target with freed heap the runtime keeps a while, GC
metadata and goroutine stacks. A lone idle tzd takes 1.5 MiB. `docker
kill -s USR2` on a tzd container prints its Go memory figures and writes
a heap profile (`/tmp/tzd-heap-1.pprof`, for `go tool pprof`).
