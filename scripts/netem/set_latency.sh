#!/usr/bin/env bash
# Usage: set_latency.sh <ms> [interface]
set -euo pipefail
MS=${1:?usage: set_latency.sh <ms> [interface]}
IFACE=${2:-lo}
tc qdisc add dev "$IFACE" root netem delay "${MS}ms"
echo "applied ${MS}ms latency on ${IFACE}"
