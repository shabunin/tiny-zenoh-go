#!/usr/bin/env python3
"""Generate a containerlab topology of zenoh routers (tzd or zenohd) and
client hosts, with its router configs and addressing.

    gen.py ring  --impl tzd,zenohd,tzd,zenohd --name ring-mixed
    gen.py line  --impl tzd,tzd,tzd --name line-tzd

Routers r1..rN are linked in a ring (or a line); client host c1 hangs off
r1 and c2 off the router farthest from it. Nodes have no management
network: they only reach each other over the lab links, which run.sh cuts
and degrades. Output goes to topos/<name>/: topo.clab.yml, cfg/*.json5 and
addrs.txt ("node iface cidr" lines run.sh applies after deploy) and
lab.env (shell variables for the scenarios).
"""
import argparse
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("shape", choices=["ring", "line"])
ap.add_argument("--impl", required=True, help="comma-separated tzd|zenohd per router")
ap.add_argument("--name", required=True)
ap.add_argument("--lease", type=int, default=2000, help="transport lease (ms)")
ap.add_argument("--tzlab-image", default="tzlab:latest")
ap.add_argument("--zenohd-image", default="eclipse/zenoh:1.10.2")
ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "topos"))
a = ap.parse_args()

impls = a.impl.split(",")
n = len(impls)
for i in impls:
    if i not in ("tzd", "zenohd"):
        raise SystemExit(f"unknown implementation {i!r}")
routers = [f"r{i+1}" for i in range(n)]

# Router-router links: subnet 10.0.<k>.0/24, .1 on the first end.
pairs = [(i, i + 1) for i in range(n - 1)]
if a.shape == "ring" and n > 2:
    pairs.append((n - 1, 0))
nexti = {r: 1 for r in routers}  # next free ethN per node
links, addrs = [], []
connect = {r: [] for r in routers}
linkvars = []
for k, (i, j) in enumerate(pairs, start=1):
    ri, rj = routers[i], routers[j]
    ei, ej = f"eth{nexti[ri]}", f"eth{nexti[rj]}"
    nexti[ri] += 1
    nexti[rj] += 1
    links.append((f"{ri}:{ei}", f"{rj}:{ej}"))
    addrs += [(ri, ei, f"10.0.{k}.1/24"), (rj, ej, f"10.0.{k}.2/24")]
    connect[ri].append(f"tcp/10.0.{k}.2:7447")  # each link dialed from one end
    linkvars.append(f"LINK_{ri}_{rj}='{ri}:{ei} {rj}:{ej}'")

# Clients: c1 on r1, c2 on the router farthest from r1.
far = routers[n // 2] if a.shape == "ring" else routers[-1]
clients = {}
for c, r, k in (("c1", routers[0], 101), ("c2", far, 102)):
    er = f"eth{nexti[r]}"
    nexti[r] += 1
    links.append((f"{c}:eth1", f"{r}:{er}"))
    addrs += [(c, "eth1", f"10.0.{k}.2/24"), (r, er, f"10.0.{k}.1/24")]
    clients[c] = f"tcp/10.0.{k}.1:7447"

d = os.path.join(a.out, a.name)
os.makedirs(os.path.join(d, "cfg"), exist_ok=True)
for r, impl in zip(routers, impls):
    cfg = {
        "mode": "router",
        "listen": {"endpoints": ["tcp/0.0.0.0:7447"]},
        "connect": {"endpoints": connect[r]},
        "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": False}},
        "transport": {"link": {"tx": {"lease": a.lease, "keep_alive": 4}}},
    }
    with open(os.path.join(d, "cfg", f"{r}.json5"), "w") as f:
        json.dump(cfg, f, indent=2)

y = [f"name: {a.name}", "topology:", "  nodes:"]
for r, impl in zip(routers, impls):
    y.append(f"    {r}:")
    y.append("      kind: linux")
    y.append("      network-mode: none")
    y.append(f"      labels: {{impl: {impl}}}")
    y.append(f"      binds: [cfg/{r}.json5:/cfg/{r}.json5:ro]")
    if impl == "tzd":
        y.append(f"      image: {a.tzlab_image}")
        y.append(f"      cmd: tzd -config /cfg/{r}.json5")
    else:
        y.append(f"      image: {a.zenohd_image}")
        y.append(f"      cmd: -c /cfg/{r}.json5")
for c in clients:
    y += [f"    {c}:", "      kind: linux", "      network-mode: none",
          f"      image: {a.tzlab_image}", "      cmd: sleep infinity"]
y.append("  links:")
for x, z in links:
    y.append(f'    - endpoints: ["{x}", "{z}"]')
with open(os.path.join(d, "topo.clab.yml"), "w") as f:
    f.write("\n".join(y) + "\n")
with open(os.path.join(d, "addrs.txt"), "w") as f:
    f.writelines(f"{nd} {ifc} {cidr}\n" for nd, ifc, cidr in addrs)
with open(os.path.join(d, "lab.env"), "w") as f:
    f.write(f"LAB={a.name}\nROUTERS='{' '.join(routers)}'\nIMPLS='{' '.join(impls)}'\n")
    f.write(f"C1_EP={clients['c1']}\nC2_EP={clients['c2']}\nFAR={far}\nLEASE_MS={a.lease}\n")
    f.writelines(v + "\n" for v in linkvars)
print(d)
