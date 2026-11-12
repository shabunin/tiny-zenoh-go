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

Topologies for the other scenarios:

```bash
./gen.py ring --impl tzd,tzd,zenohd,zenohd --storage r1,r3 --name part-mixed
./gen.py gateway --impl tzd,zenohd,tzd --gossip --name gw-mixed
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
| zenohd ×4 | 1.6 s, 3.4 s (+21 lost after) | same, put 1.5 s / 3.4 s after heal | converged |
| tzd, tzd, zenohd, zenohd | 1.4 s, 3.4 s | same | converged |

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
