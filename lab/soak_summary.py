#!/usr/bin/env python3
"""Summaries of a run.sh soak run (results/TOPO-soak).

    soak_summary.py --window results/T-soak/w003 results/T-soak   one window's line
    soak_summary.py results/T-soak                                the whole run

Per window: the flows (every host to every host) that delivered, the
samples lost (holes; expected around the link cuts, zenoh is reliable per
hop), duplicates and reordering (expected none), the flows still broken in
the last 30 s of the window, the liveliness tokens missing when its traffic ends.

Whole run: the windows, then memory: every router's resident memory over
time (a least-squares slope after the warm-up, the first hour or the first
quarter of a shorter run), the tzd routers' heap and goroutines (SIGUSR2
lines in logs/), the host's available memory.
"""
import glob
import json
import math
import os
import re
import statistics
import sys
from collections import defaultdict
from datetime import datetime


def params(out):
    minutes, wmin, rate, seed, *hosts = open(f"{out}/params").read().split()
    return int(minutes), int(wmin), int(rate), int(seed), hosts


def window(w, out):
    _, wmin, rate, _, hosts = params(out)
    ws = wmin * 60
    name = os.path.basename(w)
    flows = ok = lost = dups = reord = 0
    broken, missing, errors = [], [], []
    for s in hosts:
        try:
            sub = json.load(open(f"{w}/sub-{s}.json"))
        except (OSError, ValueError) as e:
            errors.append(f"sub-{s}: {e.__class__.__name__}")
            continue
        for p in hosts:
            flows += 1
            f = sub["keys"].get(f"lab/soak/{p}")
            if f is None:
                broken.append(f"{p}->{s} (nothing)")
                continue
            lost += f["lost"]
            dups += f["duplicates"]
            reord += f["reordered"]
            # the pubs start 3 s after the subs and run ws seconds
            tail = f["per_second"][ws + 3 - 30: ws + 3]
            if sum(tail) < rate * len(tail) // 2:
                broken.append(f"{p}->{s} ({sum(tail)}/{rate * len(tail)} in the last 30 s)")
            else:
                ok += 1
    for h in hosts:
        # The tokens alive when the pubs stop (ws s after the agents start):
        # the agents end one after the other, so their final sets differ.
        try:
            evs = [json.loads(l) for l in open(f"{w}/alive-{h}.jsonl") if l.startswith("{")]
        except (OSError, ValueError) as e:
            errors.append(f"alive-{h}: {e.__class__.__name__}")
            continue
        if not evs or evs[-1].get("event") != "end":
            errors.append(f"alive-{h}: no end")
            continue
        alive = set()
        for e in evs:
            if e["t"] > ws:
                break
            if e["event"] == "put":
                alive.add(e["key"])
            elif e["event"] == "delete":
                alive.discard(e["key"])
        miss = [p for p in hosts if f"lab/alive/{p}" not in alive]
        if miss:
            missing.append(f"{h} lacks {','.join(miss)}")
    line = f"{name}: flows {ok}/{flows} ok, lost {lost}, duplicates {dups}, reordered {reord}"
    if broken:
        line += f"; BROKEN {len(broken)}: " + ", ".join(broken[:8]) + (" ..." if len(broken) > 8 else "")
    if missing:
        line += "; TOKENS MISSING: " + "; ".join(missing)
    if errors:
        line += "; AGENT OUTPUT MISSING: " + ", ".join(errors)
    bad = bool(broken or missing or errors or dups)
    return line, bad, (ok, flows, lost, dups, reord)


def slope_per_hour(pts):
    """Least-squares slope of (unix time, value) points, per hour."""
    if len(pts) < 3:
        return 0.0
    ts = [t for t, _ in pts]
    vs = [v for _, v in pts]
    mt, mv = statistics.fmean(ts), statistics.fmean(vs)
    den = sum((t - mt) ** 2 for t in ts)
    return 0.0 if den == 0 else sum((t - mt) * (v - mv) for t, v in pts) / den * 3600


def near(pts, t, span=600):
    """Median of the points within span seconds of t."""
    vs = [v for u, v in pts if abs(u - t) <= span / 2]
    return statistics.median(vs) if vs else float("nan")


def mib(k):
    return k / 1024


def memory(out):
    series = defaultdict(list)  # (node, metric) -> [(t, v)]
    for line in open(f"{out}/mem.tsv"):
        f = line.split()
        if len(f) == 4:
            try:
                series[(f[1], f[2])].append((int(f[0]), float(f[3])))
            except ValueError:
                pass
    if not series:
        print("memory: no samples")
        return
    t_all = [t for pts in series.values() for t, _ in pts]
    t0, t1 = min(t_all), max(t_all)
    warm = t0 + min(3600, (t1 - t0) / 4)
    hours = (t1 - t0) / 3600
    print(f"\nmemory: {hours:.1f} h sampled; warm-up until +{(warm - t0) / 60:.0f} min excluded from the trends")

    rss = {n: [p for p in pts if p[0] >= warm] for (n, m), pts in series.items() if m == "rss_kib"}
    rss = {n: pts for n, pts in rss.items() if pts}
    if rss:
        marks = [warm] + [warm + h * 3600 for h in range(1, int((t1 - warm) // 3600) + 1)] + [t1]
        sums = []
        for m in sorted(set(marks)):
            vals = [v for v in (near(p, m) for p in rss.values()) if not math.isnan(v)]
            sums.append(f"+{(m - t0) / 3600:.1f}h {mib(sum(vals)):.0f}")
        print("  routers' RSS, all together (MiB):", ", ".join(sums))
        rows = []
        for n, pts in rss.items():
            a, b = near(pts, warm), near(pts, t1)
            rows.append((slope_per_hour(pts) / 1024, n, mib(a), mib(b), mib(max(v for _, v in pts))))
        rows = [r for r in rows if not math.isnan(r[2]) and not math.isnan(r[3])] or rows
        rows.sort(reverse=True)
        sl = [r[0] for r in rows]
        print(f"  RSS slope per router (MiB/h): median {statistics.median(sl):+.3f}, max {sl[0]:+.3f} ({rows[0][1]}), "
              f"min {sl[-1]:+.3f} ({rows[-1][1]})")
        print("  most growth:", ", ".join(f"{n} {a:.1f}->{b:.1f} (max {m:.1f}, {s:+.2f}/h)" for s, n, a, b, m in rows[:5]))
        suspects = [f"{n} {s:+.2f} MiB/h" for s, n, *_ in rows if s > 0.5]
        print("  RSS growing > 0.5 MiB/h:", ", ".join(suspects) if suspects else "none")

    # tzd SIGUSR2 lines: "<ts> mem: sys X MiB; heap alloc A inuse I ...; goroutines G; gcs N"
    rx = re.compile(r"^(\S+) mem: sys ([\d.]+) MiB; heap alloc ([\d.]+) inuse ([\d.]+).* goroutines (\d+); gcs (\d+)")
    heap, gor = {}, {}
    for f in sorted(glob.glob(f"{out}/logs/r*.log")):
        n = os.path.basename(f)[:-4]
        hp, gp = [], []
        for line in open(f, errors="replace"):
            m = rx.match(line)
            if not m:
                continue
            ts = datetime.fromisoformat(m.group(1)[:26].rstrip("Z") + "+00:00").timestamp()
            if ts < warm:
                continue
            hp.append((ts, float(m.group(4))))
            gp.append((ts, float(m.group(5))))
        if hp:
            heap[n], gor[n] = hp, gp
    if heap:
        hs = sorted(((slope_per_hour(p), n) for n, p in heap.items()), reverse=True)
        print(f"  tzd heap in use (MiB), {len(heap)} routers: median {statistics.median(v for p in heap.values() for _, v in p):.1f}, "
              f"slope median {statistics.median(s for s, _ in hs):+.3f}/h, max {hs[0][0]:+.3f}/h ({hs[0][1]})")
        rows = []
        for n, p in gor.items():
            first = statistics.median(v for _, v in p[: max(1, len(p) // 4)])
            last = statistics.median(v for _, v in p[-max(1, len(p) // 4):])
            rows.append((last - first, n, first, last, max(v for _, v in p)))
        rows.sort(reverse=True)
        print(f"  tzd goroutines: first quarter -> last quarter (median), most growth: " +
              ", ".join(f"{n} {a:.0f}->{b:.0f} (max {m:.0f})" for _, n, a, b, m in rows[:5]))
        grow = [f"{n} +{d:.0f}" for d, n, a, *_ in rows if d > max(5, 0.1 * a)]
        print("  goroutines growing (> 10%):", ", ".join(grow) if grow else "none")
    elif glob.glob(f"{out}/logs/r*.log"):
        print("  tzd: no memory lines in logs/ after the warm-up")

    av = series.get(("host", "avail_mib"), [])
    if av:
        print(f"  host available memory (MiB): start {av[0][1]:.0f}, min {min(v for _, v in av):.0f}, end {av[-1][1]:.0f}")
    ld = series.get(("host", "load1"), [])
    if ld:
        print(f"  host load (1 min): median {statistics.median(v for _, v in ld):.2f}, max {max(v for _, v in ld):.2f}")


def main():
    a = sys.argv[1:]
    if a and a[0] == "--window":
        line, _, _ = window(a[1], a[2])
        print(line)
        return
    out = a[0]
    minutes, wmin, rate, seed, hosts = params(out)
    ws = sorted(glob.glob(f"{out}/w[0-9][0-9][0-9]"))
    print(f"soak {out}: {len(ws)} windows of {wmin} min (of {minutes // wmin} planned), "
          f"{len(hosts)} hosts at {rate}/s, seed {seed}")
    tot = [0, 0, 0, 0, 0]
    bad = []
    for w in ws:
        if not glob.glob(f"{w}/sub-*.json") or not all(os.path.getsize(f) for f in glob.glob(f"{w}/sub-*.json")):
            continue  # the window still running
        line, b, t = window(w, out)
        tot = [x + y for x, y in zip(tot, t)]
        if b:
            bad.append(line)
    ok, flows, lost, dups, reord = tot
    print(f"flows ok at the end of their window: {ok}/{flows}; lost {lost}, duplicates {dups}, reordered {reord}")
    print(f"windows with problems: {len(bad)}")
    for line in bad:
        print("  " + line)
    memory(out)


if __name__ == "__main__":
    main()
