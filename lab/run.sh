#!/bin/bash
# Containerlab scenarios for tzd and zenohd routers (run on the lab host).
#
#   run.sh image            build the tzlab image (after build.sh)
#   run.sh up TOPO          deploy topos/TOPO (or presets/TOPO) and address
#                           its links
#   run.sh down TOPO        destroy it
#   run.sh failover TOPO    ring: c1 -> c2 traffic while the busiest of r1's
#                           router links is cut and restored, twice
#   run.sh partition TOPO   ring of 4 with storages: split it in two, write
#                           on both sides, heal; liveliness, data and the
#                           storages' convergence
#   run.sh gateway TOPO     gateway shape: traffic across the two gateways
#                           of a peer subregion while the forwarding one is
#                           cut off and comes back
#   run.sh netem TOPO       line of 3: c1 -> c2 traffic while the r1-r2 link
#                           is degraded with netem (delay, jitter, loss, a
#                           blackout shorter than the lease, a bandwidth cap)
#   run.sh scale TOPO       grid/random with --hosts: idle control traffic,
#                           every host to every other with tokens, the
#                           centre router cut off, then every router link
#
# Results (the agents' JSON, an event log, a summary) go to
# results/TOPO-SCENARIO.
set -euo pipefail
cd "$(dirname "$0")"

ns() { echo "clab-$LAB-$1"; }
nsexec() { local n=$1; shift; sudo ip netns exec "$(ns "$n")" "$@"; }

load() {
	TOPO=$1
	TD=topos/$TOPO
	[ -f "$TD/lab.env" ] || TD=presets/$TOPO
	[ -f "$TD/lab.env" ] || { echo "no topology $TOPO in topos/ (gen.py) or presets/"; exit 1; }
	# shellcheck disable=SC1090
	. "$TD/lab.env"
}

addresses() {
	while read -r node ifc cidr; do
		nsexec "$node" ip addr replace "$cidr" dev "$ifc"
		nsexec "$node" ip link set "$ifc" up
	done < "$TD/addrs.txt"
}

# link_set "r1:eth1 r2:eth1" up|down: both ends.
link_set() {
	for end in $1; do
		nsexec "${end%%:*}" ip link set "${end##*:}" "$2"
	done
}

# links_of NODE: the LINK_ variables of node's links.
links_of() {
	set | sed -n "s/^\(LINK_[a-z0-9]*_[a-z0-9]*\)=.*/\1/p" | grep -E "^LINK_($1_|[a-z0-9]+_$1\$)"
}

# end_on NODE VAR: node's interface on link VAR.
end_on() {
	local end
	for end in ${!2}; do
		if [ "${end%%:*}" = "$1" ]; then echo "${end##*:}"; fi
	done
}

# tx_rate NODE VAR: bytes node sent on link VAR in one second.
tx_rate() {
	local ifc a b
	ifc=$(end_on "$1" "$2")
	a=$(nsexec "$1" cat "/sys/class/net/$ifc/statistics/tx_bytes")
	sleep 1
	b=$(nsexec "$1" cat "/sys/class/net/$ifc/statistics/tx_bytes")
	echo $((b - a))
}

# busiest_r1_link: r1's router link that sends the most (its LINK_ value).
busiest_r1_link() {
	local best="" bestn=-1 v n
	for v in $(links_of r1 | grep -E '^LINK_r[0-9]+_r[0-9]+$'); do
		n=$(tx_rate r1 "$v")
		if [ "$n" -gt "$bestn" ]; then bestn=$n; best=${!v}; fi
	done
	echo "$best"
}

T0=0
event() {
	local t
	t=$(awk -v a="$T0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.1f", b-a}')
	echo "t=${t}s $*" | tee -a "$OUT/events.log"
}

# agent NODE CMD ARGS...: zlab CMD in a host container, with the host's
# connection arguments.
agent() {
	local n=$1 cmd=$2 v
	shift 2
	v=$(echo "$n" | tr '[:lower:]' '[:upper:]')_ARGS
	# shellcheck disable=SC2086
	docker exec "$(ns "$n")" zlab "$cmd" ${!v} "$@"
}

# flow LABEL PUB SUB: summary of one publisher/subscriber pair.
flow() {
	python3 - "$1" "$OUT/$2" "$OUT/$3" <<'PY'
import json, sys
label, pf, sf = sys.argv[1:]
pub, sub = json.load(open(pf)), json.load(open(sf))
missing = pub["sent"] - sub["received"]
print(f"[{label}] sent {pub['sent']}  received {sub['received']}  missing {missing}  "
      f"duplicates {sub['duplicates']}  reordered {sub['reordered']}")
print(f"[{label}] longest silence {sub['max_silence_ms']} ms at t={sub['max_silence_at_s']:.1f}s  "
      f"latency p50 {sub['latency_ms']['p50']} ms  p99 {sub['latency_ms']['p99']} ms  "
      f"max {sub['latency_ms']['max']} ms")
for g in sub["gaps"] or []:
    print(f"[{label}]   gap: seq {g['From']}..{g['To']} ({g['To']-g['From']+1})")
print(f"[{label}] per second:", " ".join(str(x) for x in sub["per_second"]))
PY
}

failover() {
	OUT=results/$TOPO-failover
	rm -rf "$OUT" && mkdir -p "$OUT"
	T0=$(date +%s.%N)
	agent c2 sub -k lab/flow -d 52s > "$OUT/sub.json" &
	local subpid=$!
	sleep 3
	event "pub starts (1000/s for 44s)"
	agent c1 pub -k lab/flow -rate 1000 -d 44s > "$OUT/pub.json" &
	local pubpid=$! l
	sleep 5
	for _ in 1 2; do
		l=$(busiest_r1_link)
		event "cut $l (the busiest of r1's router links)" && link_set "$l" down
		sleep 8
		event "restore $l" && link_set "$l" up
		sleep 10
	done
	wait $pubpid
	event "pub done"
	wait $subpid
	flow c1-c2 pub.json sub.json | tee "$OUT/summary.txt"
}

# get NODE TAG: the storages' content as NODE sees it, to $OUT/get-TAG.json.
get() {
	agent "$1" get -k 'lab/store/**' -d 3s > "$OUT/get-$2.json"
	event "$1 reads $(cat "$OUT/get-$2.json")"
}

partition() {
	[ "$SHAPE" = ring ] && [ "$ROUTERS" = "r1 r2 r3 r4" ] && [ -n "$STORAGES" ] ||
		{ echo "partition: needs a ring of 4 with --storage"; exit 1; }
	OUT=results/$TOPO-partition
	rm -rf "$OUT" && mkdir -p "$OUT"
	local split="$LINK_r1_r2 $LINK_r3_r4"
	T0=$(date +%s.%N)
	agent c1 alive -k lab/alive/c1 -watch 'lab/alive/**' -d 88s > "$OUT/alive-c1.jsonl" &
	local a1=$!
	agent c2 alive -k lab/alive/c2 -watch 'lab/alive/**' -d 88s > "$OUT/alive-c2.jsonl" &
	local a2=$!
	agent c2 sub -k lab/flow -d 86s > "$OUT/sub.json" &
	local subpid=$!
	sleep 3
	event "pub starts (200/s for 80s)"
	agent c1 pub -k lab/flow -rate 200 -d 80s > "$OUT/pub.json" &
	local pubpid=$!
	agent c1 put -k lab/store/before -v v0
	event "c1 put lab/store/before=v0"
	sleep 4
	event "partition: cut r1-r2 and r3-r4 ({r1,r4,c1} | {r2,r3,c2})" && link_set "$split" down
	sleep 5
	agent c1 put -k lab/store/a -v A1 && event "c1 put lab/store/a=A1"
	agent c2 put -k lab/store/b -v B1 && event "c2 put lab/store/b=B1"
	agent c1 put -k lab/store/x -v from-c1 && event "c1 put lab/store/x=from-c1"
	sleep 1
	agent c2 put -k lab/store/x -v from-c2 && event "c2 put lab/store/x=from-c2 (later: wins)"
	get c1 split-c1
	get c2 split-c2
	sleep 4
	event "heal" && link_set "$split" up
	sleep 20
	get c1 healed-c1
	get c2 healed-c2
	event "split again, to read each half's storage alone" && link_set "$split" down
	sleep 5
	get c1 resplit-c1
	get c2 resplit-c2
	event "heal" && link_set "$split" up
	wait $pubpid
	event "pub done"
	wait $subpid $a1 $a2
	{
		flow c1-c2 pub.json sub.json
		python3 - "$OUT" <<'PY'
import json, sys
d = sys.argv[1]
for me, other in (("c1", "c2"), ("c2", "c1")):
    evs = [json.loads(l) for l in open(f"{d}/alive-{me}.jsonl")]
    tl = " ".join(f"{e['event']}@{e['t']:.1f}s" for e in evs
                  if e.get("key") == f"lab/alive/{other}")
    end = next(e for e in evs if e["event"] == "end")
    print(f"[liveliness] {me} saw {other}'s token: {tl}; alive at the end: {end['alive']}")
want = {"lab/store/before": "v0", "lab/store/a": "A1", "lab/store/b": "B1",
        "lab/store/x": "from-c2"}
for tag in ("split-c1", "split-c2", "healed-c1", "healed-c2", "resplit-c1", "resplit-c2"):
    got = json.load(open(f"{d}/get-{tag}.json"))
    verdict = "converged" if got == want else "differs"
    if tag.startswith("split"):
        verdict = "(during the split)"
    print(f"[storage] {tag}: {json.dumps(got, sort_keys=True)} {verdict}")
PY
	} | tee "$OUT/summary.txt"
}

gateway() {
	[ "$SHAPE" = gateway ] || { echo "gateway: needs a gateway topology"; exit 1; }
	OUT=results/$TOPO-gateway
	rm -rf "$OUT" && mkdir -p "$OUT"
	T0=$(date +%s.%N)
	# p1 (cell) -> c1 (north) and p2 (cell); c1 -> p2.
	agent c1 sub -k lab/south -d 50s > "$OUT/sub-south-c1.json" &
	local s1=$!
	agent p2 sub -k lab/south -d 50s > "$OUT/sub-south-p2.json" &
	local s2=$!
	agent p2 sub -k lab/north -d 50s > "$OUT/sub-north-p2.json" &
	local s3=$!
	sleep 4
	event "pubs start: p1 on lab/south, c1 on lab/north (500/s for 40s)"
	agent p1 pub -k lab/south -rate 500 -d 40s > "$OUT/pub-south.json" &
	local p1=$!
	agent c1 pub -k lab/north -rate 500 -d 40s > "$OUT/pub-north.json" &
	local p2=$!
	sleep 4
	local up1 up2 down1 down2
	up1=$(tx_rate r1 LINK_r1_r3)
	up2=$(tx_rate r2 LINK_r2_r3)
	down1=$(tx_rate r1 LINK_p2_r1)
	down2=$(tx_rate r2 LINK_p2_r2)
	event "bytes/s to the north: r1 $up1, r2 $up2; to p2: r1 $down1, r2 $down2"
	local fwd=r2
	[ "$up1" -gt "$up2" ] && fwd=r1
	local cut="" v
	for v in $(links_of $fwd); do cut="$cut ${!v}"; done
	event "cut every link of $fwd (the forwarding gateway)" && link_set "$cut" down
	sleep 12
	event "restore $fwd" && link_set "$cut" up
	sleep 10
	up1=$(tx_rate r1 LINK_r1_r3)
	up2=$(tx_rate r2 LINK_r2_r3)
	event "bytes/s to the north: r1 $up1, r2 $up2"
	wait $p1 $p2
	event "pubs done"
	wait $s1 $s2 $s3
	{
		flow "p1->c1 (across a gateway)" pub-south.json sub-south-c1.json
		flow "p1->p2 (within the cell)" pub-south.json sub-south-p2.json
		flow "c1->p2 (across a gateway)" pub-north.json sub-north-p2.json
	} | tee "$OUT/summary.txt"
}

# netem_set "r1:eth1 r2:eth1" ARGS...: netem with ARGS on both ends (none:
# remove it).
netem_set() {
	local end
	for end in $1; do
		if [ "$2" = none ]; then
			nsexec "${end%%:*}" tc qdisc del dev "${end##*:}" root 2>/dev/null || true
		else
			nsexec "${end%%:*}" tc qdisc replace dev "${end##*:}" root netem "${@:2}"
		fi
	done
}

# hwm NODE: the router's peak resident memory (kB).
hwm() {
	docker exec "$(ns "$1")" sh -c 'grep VmHWM /proc/1/status' | awk '{print $2}'
}

# r1_ports: r1's local ports of TCP connections to r2 (one per transport).
r1_ports() {
	nsexec r1 ss -tnH state established dst 10.0.1.1 | awk '{split($3,a,":"); print a[2]}' | sort -u | tr '\n' ' '
}

# netem_case NAME PAYLOAD BLOCK NETEM...: one flow of 10 s at 1000/s (on a
# key of its own) with the r1-r2 link degraded; NETEM "blackout SECS" cuts
# the link (100% loss) for SECS, 3 s into the flow. The publisher waits 14 s
# before closing its session, so that a backlog still on its way is measured
# as latency, not cut short by the close (which waits for it at most the
# lease, see zenoh.Config.CloseLinger).
netem_case() {
	local name=$1 size=$2 block=$3
	shift 3
	local d="$OUT/$name" ports="" bo=""
	mkdir -p "$d"
	if [ "$1" = blackout ]; then
		bo=$2
		netem_set "$LINK_r1_r2" none
	else
		netem_set "$LINK_r1_r2" "$@"
	fi
	agent c2 sub -k "lab/netem/$name" -d 28s > "$d/sub.json" &
	sleep 2
	event "$name: netem '$*', payload $size, block=$block"
	local popt=""
	[ "$block" = 1 ] && popt=-block
	# shellcheck disable=SC2086
	agent c1 pub -k "lab/netem/$name" -rate 1000 -size "$size" $popt -d 10s -linger 14s > "$d/pub.json" &
	local i
	if [ -n "$bo" ]; then
		(
			sleep 3
			netem_set "$LINK_r1_r2" loss 100% && event "$name: link dark"
			sleep "$bo"
			netem_set "$LINK_r1_r2" none && event "$name: link back"
		) &
	fi
	for i in $(seq 1 24); do
		ports="$ports $(r1_ports)"
		sleep 1
	done
	wait
	netem_set "$LINK_r1_r2" none
	echo "$ports" | tr ' ' '\n' | grep -v '^$' | sort -u | wc -l > "$d/transports"
	echo "$(hwm r1) $(hwm r2)" > "$d/hwm"
}

netem() {
	[ "$SHAPE" = line ] && [ "$ROUTERS" = "r1 r2 r3" ] || { echo "netem: needs a line of 3"; exit 1; }
	OUT=results/$TOPO-netem
	rm -rf "$OUT" && mkdir -p "$OUT"
	T0=$(date +%s.%N)
	netem_case base 64 0 none
	netem_case delay 64 0 delay 25ms 5ms
	netem_case loss1 64 0 loss 1%
	netem_case loss5 64 0 loss 5%
	netem_case wan 64 0 delay 25ms 5ms loss 2%
	netem_case loss20 64 0 loss 20%
	netem_case dark1s 64 0 blackout 1
	netem_case dark1.5s 64 0 blackout 1.5
	# A 1 Mbit/s link with 50 packets of buffer (~0.6 s), offered twice that.
	netem_case cap-drop 256 0 rate 1mbit limit 50
	netem_case cap-block 256 1 rate 1mbit limit 50
	python3 - "$OUT" <<'PY' | tee "$OUT/summary.txt"
import json, os, sys
d = sys.argv[1]
print(f"{'case':10} {'sent':>6} {'recv':>6} {'missing':>7} {'dup':>4} {'reord':>5} {'silence':>8} "
      f"{'p50':>8} {'p99':>8} {'max':>8} {'put max':>8} {'transp':>6} {'r1/r2 peak MB':>14}")
for c in ["base", "delay", "loss1", "loss5", "wan", "loss20", "dark1s", "dark1.5s", "cap-drop", "cap-block"]:
    p, s = (json.load(open(f"{d}/{c}/{x}.json")) for x in ("pub", "sub"))
    tr = open(f"{d}/{c}/transports").read().strip()
    h = [int(x) / 1024 for x in open(f"{d}/{c}/hwm").read().split()]
    l = s["latency_ms"]
    print(f"{c:10} {p['sent']:6} {s['received']:6} {p['sent'] - s['received']:7} {s['duplicates']:4} "
          f"{s['reordered']:5} {s['max_silence_ms']:6}ms {l['p50']:6.1f}ms {l['p99']:6.1f}ms "
          f"{l['max']:6.0f}ms {p['max_put_ms']:6}ms {tr:>6} {h[0]:6.1f}/{h[1]:.1f}")
PY
}

# links_par VARS up|down: both ends of the LINK_ variables VARS, in parallel.
links_par() {
	local v end
	for v in $1; do
		for end in ${!v}; do
			echo "$(ns "${end%%:*}") ${end##*:}"
		done
	done | xargs -P 16 -n 2 sh -c 'ip netns exec "$0" ip link set "$1" '"$2"
}

# router_link_vars: the LINK_ variables between two routers.
router_link_vars() {
	set | sed -n "s/^\(LINK_r[0-9]*_r[0-9]*\)=.*/\1/p"
}

# tx_total: bytes sent so far on every router interface.
tx_total() {
	local r
	for r in $ROUTERS; do
		nsexec "$r" sh -c 'cat /sys/class/net/eth*/statistics/tx_bytes'
	done | awk '{s += $1} END {print s}'
}

# dstats TAG: CPU of every lab container (docker stats) to stats-TAG.txt,
# and every router's resident memory (kB) to rss-TAG.txt.
dstats() {
	docker stats --no-stream --format '{{.Name}} {{.MemUsage}} {{.CPUPerc}}' |
		grep "^clab-$LAB-" > "$OUT/stats-$1.txt"
	for r in $ROUTERS; do echo "$r $(ns "$r")"; done |
		xargs -P 8 -n 2 sh -c 'echo "$0 $(docker exec "$1" sh -c "grep VmRSS /proc/1/status" | awk "{print \$2}")"' > "$OUT/rss-$1.txt"
}

scale() {
	[ -n "${CENTRE:-}" ] && [ -n "$HOSTS" ] || { echo "scale: needs a grid/random topology with --hosts"; exit 1; }
	OUT=results/$TOPO-scale
	rm -rf "$OUT" && mkdir -p "$OUT"
	local hs="" h rr nr nl a b ta tb
	for h in $HOSTS; do [ "${h#h}" != "$h" ] && hs="$hs $h"; done
	rr=$(router_link_vars)
	nr=$(echo "$ROUTERS" | wc -w)
	nl=$(echo "$rr" | wc -w)
	T0=$(date +%s.%N)
	event "$nr routers ($IMPLS), $nl router links, hosts$hs; settling"
	sleep 10
	ta=$(date +%s.%N); a=$(tx_total)
	sleep 10
	tb=$(date +%s.%N); b=$(tx_total)
	echo "$a $b $ta $tb $nl" > "$OUT/idle-tx"
	dstats idle
	event "idle measured"
	T0=$(date +%s.%N)
	for h in $hs; do
		agent "$h" alive -k "lab/alive/$h" -watch 'lab/alive/**' -d 78s > "$OUT/alive-$h.jsonl" &
		agent "$h" sub -k 'lab/scale/**' -per-key -d 76s > "$OUT/sub-$h.json" &
	done
	sleep 3
	event "pubs start: every host, 100/s each, for 70 s"
	for h in $hs; do
		agent "$h" pub -k "lab/scale/$h" -rate 100 -d 70s -linger 2s > "$OUT/pub-$h.json" &
	done
	sleep 10
	dstats busy
	event "busy measured"
	sleep 2
	local cv="" v
	for v in $(links_of "$CENTRE"); do cv="$cv $v"; done
	event "cut the centre router $CENTRE" && links_par "$cv" down
	sleep 10
	event "restore $CENTRE" && links_par "$cv" up
	sleep 15
	event "cut every router link ($nl)" && links_par "$rr" down
	sleep 5
	event "restore every router link" && links_par "$rr" up
	wait
	event "done"
	python3 - "$OUT" "$hs" "$CENTRE" "$TD" <<'PY' | tee "$OUT/summary.txt"
import json, re, sys
d, hs, centre, td = sys.argv[1], sys.argv[2].split(), sys.argv[3], sys.argv[4]
ev = {}
for line in open(f"{d}/events.log"):
    m = re.match(r"t=([\d.]+)s (.*)", line)
    if m:
        ev[m.group(2)] = float(m.group(1))
t = lambda prefix: next(v for k, v in ev.items() if k.startswith(prefix))
a, b, ta, tb, nl = open(f"{d}/idle-tx").read().split()
rate = (int(b) - int(a)) / (float(tb) - float(ta))
print(f"idle control traffic: {rate/1024:.1f} KiB/s in all, {rate/int(nl):.0f} B/s per router link")
def mem(f):
    out = []
    for line in open(f"{d}/{f}"):
        name, use = line.split()[0], line.split()[1]
        m = re.match(r"([\d.]+)([KMG]i?B)", use)
        v = float(m.group(1)) * {"KiB": 1/1024, "MiB": 1, "GiB": 1024, "kB": 1/1000, "MB": 1, "GB": 1000}[m.group(2)]
        cpu = float(line.split()[-1].rstrip("%"))
        out.append((name, v, cpu))
    return out
for tag in ("idle", "busy"):
    rs = [x for x in mem(f"stats-{tag}.txt") if re.search(r"-r\d+$", x[0])]
    rss = [int(l.split()[1]) / 1024 for l in open(f"{d}/rss-{tag}.txt") if len(l.split()) == 2]
    print(f"routers {tag}: RSS {sum(rss):.0f} MiB in all, {sum(rss)/len(rss):.1f} MiB each "
          f"(max {max(rss):.1f}), CPU {sum(c for *_, c in rs):.0f}% in all (max {max(c for *_, c in rs):.1f}%)")
pubs = {h: json.load(open(f"{d}/pub-{h}.json")) for h in hs}
subs = {h: json.load(open(f"{d}/sub-{h}.json"))["keys"] for h in hs}
# Second k of a subscriber is [k, k+1) after T0 (the events' clock).
p0, cut, back, mass, mback = (t("pubs start"), t("cut the centre"), t("restore " + centre),
                               t("cut every"), t("restore every"))
env = open(f"{td}/lab.env").read()
at = {h: re.search(rf"^{h.upper()}_AT=(\S+)", env, re.M).group(1) for h in hs}
tot_sent = tot_recv = tot_dup = tot_holes = 0
def window(ps, lo, hi):
    return ps[int(lo):int(hi) + 1]
def recovery(ps, after, want=90):
    for k in range(int(after) + 1, len(ps)):
        if ps[k] >= want:
            return k - after
    return None
cent, mass_rec, cent_rec, missing_pairs = [], [], [], []
for s in hs:
    for p in hs:
        k = subs[s].get(f"lab/scale/{p}")
        if k is None:
            missing_pairs.append((p, s))
            continue
        tot_sent += pubs[p]["sent"]; tot_recv += k["received"]; tot_dup += k["duplicates"]; tot_holes += k["lost"]
        ps = k["per_second"]
        through_centre = centre in (at[s], at[p])
        lost_c = 100 * (back - cut) - sum(window(ps, cut, back))
        if not through_centre:
            cent.append(max(0, lost_c))
            r = recovery(ps, cut)
            cent_rec.append(r)
        r = recovery(ps, mback)
        mass_rec.append(r)
n = len(hs)
print(f"{n} hosts, {n*n} flows (each host also hears itself): sent {tot_sent}, received {tot_recv}, "
      f"holes {tot_holes}, duplicates {tot_dup}, flows never seen {len(missing_pairs)}")
aff = [x for x in cent if x > 150]
cr = [r for r in cent_rec if r is not None]
print(f"centre {centre} cut for {back-cut:.0f} s: flows not to or from it: {len(cent)}, "
      f"of which {len(aff)} lost more than 1.5 s; worst lost {max(cent) if cent else 0} samples "
      f"(~{(max(cent) if cent else 0)/100:.1f} s); all at full rate again "
      f"{max(cr) if cr else 0:.1f} s after the cut")
mr = [r for r in mass_rec if r is not None]
print(f"every router link cut for {mback-mass:.0f} s: all flows back at full rate "
      f"{max(mr):.1f} s after restore (median {sorted(mr)[len(mr)//2]:.1f} s); "
      f"{len(mass_rec)-len(mr)} flows never back")
# Liveliness: the tokens each host sees 20 s after the mass cut was restored
# (the agents end a few ms apart, so not at the very end), and when the last
# came back.
check = mback + 20
lastput, complete = 0, True
want = sorted(f"lab/alive/{x}" for x in hs)
for h in hs:
    alive = set()
    for l in open(f"{d}/alive-{h}.jsonl"):
        e = json.loads(l)
        if e["event"] == "end" or e["t"] > check:
            continue
        (alive.add if e["event"] == "put" else alive.discard)(e["key"])
        if e["event"] == "put" and e["t"] > mback:
            lastput = max(lastput, e["t"])
    if sorted(alive) != want:
        complete = False
        print(f"[liveliness] {h} sees only {sorted(alive)}")
print(f"liveliness: every host sees every token 20 s after the restore: {complete}; "
      f"last token back {max(0, lastput - mback):.1f} s after the restore")
PY
}

case "${1:-}" in
image)
	docker build -q -t tzlab:latest image
	;;
up)
	load "$2"
	(cd "$TD" && sudo containerlab deploy -t topo.clab.yml --reconfigure >/dev/null)
	addresses
	echo "$LAB up: routers $ROUTERS ($IMPLS)"
	;;
down)
	load "$2"
	(cd "$TD" && sudo containerlab destroy -t topo.clab.yml --cleanup >/dev/null)
	;;
failover | partition | gateway | netem | scale)
	load "$2"
	"$1"
	;;
*)
	sed -n '2,25p' "$0"
	exit 2
	;;
esac
