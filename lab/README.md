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
