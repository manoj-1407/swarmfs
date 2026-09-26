#!/usr/bin/env bash
# Usage: set_mixed.sh <latency_ms> <loss_pct> [interface]
set -euo pipefail
MS=${1:?usage: set_mixed.sh <ms> <pct>}
PCT=${2:?usage: set_mixed.sh <ms> <pct>}
IFACE=${3:-lo}
tc qdisc add dev "$IFACE" root netem delay "${MS}ms" loss "${PCT}%"
echo "applied ${MS}ms + ${PCT}% loss on ${IFACE}"
