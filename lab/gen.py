#!/usr/bin/env python3
"""Generate a containerlab topology of zenoh routers (tzd or zenohd) and
hosts running the zlab agent, with the router configs and addressing.

    gen.py ring    --impl tzd,zenohd,tzd,zenohd --name ring-mixed
    gen.py line    --impl tzd,tzd,tzd --name line-tzd
    gen.py gateway --impl tzd,tzd,zenohd --name gw-tzd

ring/line: routers r1..rN in a ring (or a line), host c1 on r1 and c2 on
the router farthest from it.

gateway: routers r1 (ZID a1) and r2 (b2) are both gateways of the peer
subregion of peers p1 and p2, which are linked to each other and to both
gateways; r3 (c3) is north of them with host c1. r1, r2, r3 are a
triangle mesh. --impl gives r1, r2, r3.

Nodes have no management network: they only reach each other over the
lab links, which run.sh cuts and degrades. Each link is dialed from one
end. Output goes to topos/<name>/: topo.clab.yml, cfg/*.json5, addrs.txt
("node iface cidr" lines run.sh applies after deploy) and lab.env (shell
variables for the scenarios: LINK_a_b, <HOST>_EPS, ...).
"""
import argparse
import json
import os

ap = argparse.ArgumentParser()
ap.add_argument("shape", choices=["ring", "line", "gateway"])
ap.add_argument("--impl", required=True, help="comma-separated tzd|zenohd per router")
ap.add_argument("--name", required=True)
ap.add_argument("--lease", type=int, default=2000, help="transport lease (ms)")
ap.add_argument("--storage", default="", help="comma-separated routers with a replicated "
                "memory storage on lab/store/**")
ap.add_argument("--gossip", action="store_true", help="gossip scouting on the routers")
ap.add_argument("--wmem", type=int, default=0, help="routers' TCP send buffer limit in bytes "
                "(net.ipv4.tcp_wmem max; default: the kernel's, autotuned up to 4 MB)")
ap.add_argument("--tzlab-image", default="tzlab:latest")
ap.add_argument("--zenohd-image", default="eclipse/zenoh:1.10.2")
ap.add_argument("--out", default=os.path.join(os.path.dirname(os.path.abspath(__file__)), "topos"))
a = ap.parse_args()

impls = a.impl.split(",")
for i in impls:
    if i not in ("tzd", "zenohd"):
        raise SystemExit(f"unknown implementation {i!r}")
n = len(impls)
routers = [f"r{i+1}" for i in range(n)]
ids = {}
hosts = {}  # host -> {"id":..., "listen": bool}
edges = []  # (dialer, listener)

if a.shape in ("ring", "line"):
    edges = [(routers[i], routers[i + 1]) for i in range(n - 1)]
    if a.shape == "ring" and n > 2:
        edges.append((routers[-1], routers[0]))
    far = routers[n // 2] if a.shape == "ring" else routers[-1]
    hosts = {"c1": {}, "c2": {}}
    edges += [("c1", routers[0]), ("c2", far)]
else:
    if n != 3:
        raise SystemExit("gateway: --impl gives r1, r2, r3")
    far = "r3"
    ids = {"r1": "a1", "r2": "b2", "r3": "c3"}
    edges = [("r1", "r2"), ("r1", "r3"), ("r2", "r3")]
    hosts = {"p1": {"mode": "peer", "listen": True}, "p2": {"mode": "peer"}, "c1": {}}
    edges += [("p1", "r1"), ("p1", "r2"), ("p2", "r1"), ("p2", "r2"), ("p2", "p1"), ("c1", "r3")]

# Addressing: link k is 10.0.k.0/24, the dialer .2 and the listener .1.
nexti = {}
links, addrs, linkvars = [], [], []
connect = {x: [] for x in routers + list(hosts)}
for k, (d, l) in enumerate(edges, start=1):
    ed, el = f"eth{nexti.get(d, 1)}", f"eth{nexti.get(l, 1)}"
    nexti[d], nexti[l] = nexti.get(d, 1) + 1, nexti.get(l, 1) + 1
    links.append((f"{d}:{ed}", f"{l}:{el}"))
    addrs += [(d, ed, f"10.0.{k}.2/24"), (l, el, f"10.0.{k}.1/24")]
    connect[d].append(f"tcp/10.0.{k}.1:7447")
    linkvars.append(f"LINK_{d}_{l}='{d}:{ed} {l}:{el}'")

storages = [s for s in a.storage.split(",") if s]
d = os.path.join(a.out, a.name)
os.makedirs(os.path.join(d, "cfg"), exist_ok=True)
for r, impl in zip(routers, impls):
    cfg = {
        "mode": "router",
        "listen": {"endpoints": ["tcp/0.0.0.0:7447"]},
        "connect": {"endpoints": connect[r]},
        "scouting": {"multicast": {"enabled": False}, "gossip": {"enabled": a.gossip}},
        "transport": {"link": {"tx": {"lease": a.lease, "keep_alive": 4}}},
    }
    if r in ids:
        cfg["id"] = ids[r]
    if r in storages:
        cfg["plugins"] = {"storage_manager": {"storages": {"lab": {
            "key_expr": "lab/store/**", "volume": "memory",
            "replication": {"interval": 1, "sub_intervals": 5, "hot": 6, "warm": 30,
                            "propagation_delay": 250}}}}}
    with open(os.path.join(d, "cfg", f"{r}.json5"), "w") as f:
        json.dump(cfg, f, indent=2)

y = [f"name: {a.name}", "topology:", "  nodes:"]
for r, impl in zip(routers, impls):
    y += [f"    {r}:", "      kind: linux", "      network-mode: none",
          f"      labels: {{impl: {impl}}}",
          f"      binds: [cfg/{r}.json5:/cfg/{r}.json5:ro]"]
    if a.wmem:
        y += [f'      sysctls: {{net.ipv4.tcp_wmem: "4096 16384 {a.wmem}"}}']
    if impl == "tzd":
        y += [f"      image: {a.tzlab_image}", f"      cmd: tzd -config /cfg/{r}.json5"]
    else:
        y += [f"      image: {a.zenohd_image}", f"      cmd: -c /cfg/{r}.json5"]
for h in hosts:
    y += [f"    {h}:", "      kind: linux", "      network-mode: none",
          f"      image: {a.tzlab_image}", "      cmd: sleep infinity"]
y.append("  links:")
for x, z in links:
    y.append(f'    - endpoints: ["{x}", "{z}"]')
with open(os.path.join(d, "topo.clab.yml"), "w") as f:
    f.write("\n".join(y) + "\n")
with open(os.path.join(d, "addrs.txt"), "w") as f:
    f.writelines(f"{nd} {ifc} {cidr}\n" for nd, ifc, cidr in addrs)
with open(os.path.join(d, "lab.env"), "w") as f:
    f.write(f"LAB={a.name}\nSHAPE={a.shape}\nROUTERS='{' '.join(routers)}'\n")
    f.write(f"IMPLS='{' '.join(impls)}'\nFAR={far}\nLEASE_MS={a.lease}\n")
    f.write(f"STORAGES='{' '.join(storages)}'\n")
    for h, o in hosts.items():
        args = [f"-e {e}" for e in connect[h]]
        if o.get("mode") == "peer":
            args.append("-mode peer")
        if o.get("listen"):
            args.append("-l tcp/0.0.0.0:7447")
        if o.get("id"):
            args.append(f"-id {o['id']}")
        f.write(f"{h.upper()}_ARGS='{' '.join(args)}'\n")
    f.writelines(v + "\n" for v in linkvars)
print(d)
