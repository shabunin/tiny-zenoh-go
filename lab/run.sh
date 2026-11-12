#!/bin/bash
# Containerlab scenarios for tzd and zenohd routers (run on the lab host).
#
#   run.sh image            build the tzlab image (after build.sh)
#   run.sh up TOPO          deploy topos/TOPO and address its links
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
#
# Results (the agents' JSON, an event log, a summary) go to
# results/TOPO-SCENARIO.
set -euo pipefail
cd "$(dirname "$0")"

ns() { echo "clab-$LAB-$1"; }
nsexec() { local n=$1; shift; sudo ip netns exec "$(ns "$n")" "$@"; }

load() {
	TOPO=$1
	[ -f "topos/$TOPO/lab.env" ] || { echo "no topology topos/$TOPO (gen.py)"; exit 1; }
	# shellcheck disable=SC1090
	. "topos/$TOPO/lab.env"
}

addresses() {
	while read -r node ifc cidr; do
		nsexec "$node" ip addr replace "$cidr" dev "$ifc"
		nsexec "$node" ip link set "$ifc" up
	done < "topos/$TOPO/addrs.txt"
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

case "${1:-}" in
image)
	docker build -q -t tzlab:latest image
	;;
up)
	load "$2"
	(cd "topos/$TOPO" && sudo containerlab deploy -t topo.clab.yml --reconfigure >/dev/null)
	addresses
	echo "$LAB up: routers $ROUTERS ($IMPLS)"
	;;
down)
	load "$2"
	(cd "topos/$TOPO" && sudo containerlab destroy -t topo.clab.yml --cleanup >/dev/null)
	;;
failover | partition | gateway | netem)
	load "$2"
	"$1"
	;;
*)
	sed -n '2,22p' "$0"
	exit 2
	;;
esac
