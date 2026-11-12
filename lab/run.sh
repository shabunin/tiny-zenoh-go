#!/bin/bash
# Containerlab scenarios for tzd and zenohd routers (run on the lab host).
#
#   run.sh image                 build the tzlab image (after build.sh)
#   run.sh up TOPO               deploy topos/TOPO and address its links
#   run.sh down TOPO             destroy it
#   run.sh failover TOPO         traffic c1 -> c2 while r1's links are cut
#                                and restored one after the other
#
# Results (the agents' JSON and an event log) go to results/TOPO-SCENARIO.
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

# busiest_r1_link: the router link of r1 that sent the most in the last
# second (as a LINK_ value: "r1:ethX rY:ethZ").
busiest_r1_link() {
	local best="" bestn=-1 v ifc a b
	declare -A before
	for v in $(set | sed -n "s/^\(LINK_[^=]*r1[^=]*\)=.*/\1/p"); do
		for end in ${!v}; do [ "${end%%:*}" = r1 ] && ifc=${end##*:}; done
		before[$v]=$(nsexec r1 cat "/sys/class/net/$ifc/statistics/tx_bytes")
	done
	sleep 1
	for v in "${!before[@]}"; do
		for end in ${!v}; do [ "${end%%:*}" = r1 ] && ifc=${end##*:}; done
		a=${before[$v]}
		b=$(nsexec r1 cat "/sys/class/net/$ifc/statistics/tx_bytes")
		if [ $((b - a)) -gt "$bestn" ]; then bestn=$((b - a)); best=${!v}; fi
	done
	echo "$best"
}

T0=0
event() {
	local t
	t=$(awk -v a="$T0" -v b="$(date +%s.%N)" 'BEGIN{printf "%.1f", b-a}')
	echo "t=${t}s $*" | tee -a "$OUT/events.log"
}

# agent NODE ARGS...: zlab in a client container.
agent() { local n=$1; shift; docker exec "$(ns "$n")" zlab "$@"; }

summary() {
	python3 - "$OUT" <<'PY'
import json, sys
d = sys.argv[1]
pub = json.load(open(f"{d}/pub.json"))
sub = json.load(open(f"{d}/sub.json"))
missing = pub["sent"] - sub["received"]
print(f"sent {pub['sent']}  received {sub['received']}  missing {missing}  "
      f"duplicates {sub['duplicates']}  reordered {sub['reordered']}")
print(f"longest silence {sub['max_silence_ms']} ms at t={sub['max_silence_at_s']:.1f}s  "
      f"latency p50 {sub['latency_ms']['p50']} ms  p99 {sub['latency_ms']['p99']} ms  "
      f"max {sub['latency_ms']['max']} ms")
for g in sub["gaps"]:
    print(f"  gap: seq {g['From']}..{g['To']} ({g['To']-g['From']+1})")
print("per second:", " ".join(str(x) for x in sub["per_second"]))
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
failover)
	load "$2"
	OUT=results/$TOPO-failover
	rm -rf "$OUT" && mkdir -p "$OUT"
	T0=$(date +%s.%N)
	agent c2 sub -e "$C2_EP" -k lab/flow -d 52s > "$OUT/sub.json" &
	subpid=$!
	sleep 3
	event "pub starts (1000/s for 44s)"
	agent c1 pub -e "$C1_EP" -k lab/flow -rate 1000 -d 44s > "$OUT/pub.json" &
	pubpid=$!
	sleep 5
	for round in 1 2; do
		l=$(busiest_r1_link)
		event "cut $l (the busiest of r1's router links)" && link_set "$l" down
		sleep 8
		event "restore $l" && link_set "$l" up
		sleep 10
	done
	wait $pubpid
	event "pub done"
	wait $subpid
	summary | tee "$OUT/summary.txt"
	;;
*)
	sed -n '2,10p' "$0"
	exit 2
	;;
esac
