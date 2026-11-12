#!/bin/bash
# Cross-implementation throughput/latency matrix on loopback.
# Build first: go build -o $BIN ./examples/z_* ./cmd/tzd; (cd bench/rbench && cargo build --release)
ROOT=$(cd "$(dirname "$0")/.." && pwd)
BIN=${BIN:-$ROOT/bench/bin}
TMP=$(mktemp -d)
trap "rm -rf $TMP" EXIT
G=$BIN; R=${RBENCH:-$ROOT/bench/rbench/target/release/rbench}
port=24000
nextport() { port=$((port+1)); }
NS="--no-multicast-scouting"

# sub: impl mode eps...   pub likewise
sub_cmd() { # impl n s args...
  local impl=$1 n=$2 s=$3; shift 3
  if [ $impl = go ]; then echo "$G/z_sub_thr $NS -n $n -s $s $*"; else echo "$R sub_thr -n $n -c $s $*"; fi
}
pub_cmd() { # impl size args...
  local impl=$1 size=$2; shift 2
  if [ $impl = go ]; then echo "$G/z_pub_thr $NS $* $size"; else echo "$R pub_thr -s $size $*"; fi
}
router_cmd() { # impl ep
  if [ $1 = go ]; then echo "$G/tzd $NS -l $2"; else echo "$R router -m router -l $2"; fi
}

thr() { # topo(peer|go-router|rust-router) pubimpl subimpl size
  local topo=$1 pi=$2 si=$3 size=$4 n s
  case $size in 8) n=200000; s=5;; 1024) n=100000; s=5;; *) n=3000; s=5;; esac
  nextport; local ep=tcp/127.0.0.1:$port rp=
  if [ $topo = peer ]; then
    subargs="-m peer -l $ep"; pubargs="-m peer -e $ep"
  else
    $(router_cmd ${topo%-router} $ep) >/dev/null 2>&1 & rp=$!
    sleep 0.7
    subargs="-m client -e $ep"; pubargs="-m client -e $ep"
  fi
  local out=$TMP/xb_out.txt
  timeout 40 $(sub_cmd $si $n $s $subargs) > $out 2>&1 & local sp=$!
  sleep 0.8
  timeout 40 $(pub_cmd $pi $size $pubargs) >/dev/null 2>&1 & local pp=$!
  wait $sp
  kill $pp 2>/dev/null; wait $pp 2>/dev/null
  [ -n "$rp" ] && { kill $rp; wait $rp 2>/dev/null; }
  local avg=$(grep -o "average: [0-9.]*" $out | awk '{print $2}')
  printf "thr  %-12s pub=%-4s sub=%-4s %6sB  %12s msg/s\n" $topo $pi $si $size "${avg:-FAIL}"
}

ping() { # topo pingimpl pongimpl size
  local topo=$1 pi=$2 qi=$3 size=$4
  nextport; local ep=tcp/127.0.0.1:$port rp=
  if [ $topo = peer ]; then
    pongargs="-m peer -l $ep"; pingargs="-m peer -e $ep"
  else
    $(router_cmd ${topo%-router} $ep) >/dev/null 2>&1 & rp=$!
    sleep 0.7
    pongargs="-m client -e $ep"; pingargs="-m client -e $ep"
  fi
  if [ $qi = go ]; then $G/z_pong $NS $pongargs >/dev/null 2>&1 & else $R pong $pongargs >/dev/null 2>&1 & fi
  local qp=$!
  sleep 0.8
  local out=$TMP/xb_ping.txt res
  if [ $pi = go ]; then
    timeout 40 $G/z_ping $NS $pingargs -n 5000 -w 1 $size > $out 2>&1
    res=$(grep -o "rtt=[0-9]*" $out | cut -d= -f2 | sort -n | awk '{a[NR]=$1; s+=$1} END {if (NR) printf "avg %.1fµs median %dµs", s/NR, a[int(NR/2)+1]}')
  else
    timeout 40 $R ping $pingargs -n 5000 -s $size > $out 2>&1
    res=$(grep -o "avg [0-9.]*µs median [0-9]*µs" $out)
  fi
  kill $qp; wait $qp 2>/dev/null
  [ -n "$rp" ] && { kill $rp; wait $rp 2>/dev/null; }
  printf "ping %-12s ping=%-4s pong=%-4s %6sB  %s\n" $topo $pi $qi $size "${res:-FAIL}"
}

for size in 8 1024 65536; do
  for pair in "go go" "rust rust" "go rust" "rust go"; do thr peer $pair $size; done
done
for topo in go-router rust-router; do
  for size in 8 1024 65536; do
    for pair in "go go" "rust rust"; do thr $topo $pair $size; done
  done
done
for topo in peer go-router rust-router; do
  for pair in "go go" "rust rust" "go rust" "rust go"; do ping $topo $pair 64; done
done
