#!/bin/bash
# Regenerate the saved topologies in presets/ (committed, to read and to
# deploy by hand: see presets/README.md).
set -euo pipefail
cd "$(dirname "$0")"
g() { ./gen.py "$@" --out presets >/dev/null; }
alt8=tzd,zenohd,tzd,zenohd,tzd,zenohd,tzd,zenohd
g ring --impl tzd,zenohd,tzd,zenohd --name ring4-mixed
g ring --impl tzd,tzd,zenohd,zenohd --storage r1,r3 --name part4-mixed
g ring --impl $alt8 --storage r1,r4,r6 --gossip --name ring8-storage-mixed
g gateway --impl tzd,zenohd,tzd --gossip --name gw-mixed
g line --impl tzd,zenohd,tzd --wmem 65536 --name line3-netem-mixed
g grid --impl tzd --size 7x7 --hosts 8 --name grid49-tzd
g grid --impl tzd,zenohd --size 10x10 --hosts 8 --name grid100-mixed
g random --impl tzd,zenohd --size 60 --degree 3 --seed 7 --hosts 8 --name rnd60-mixed
ls presets
