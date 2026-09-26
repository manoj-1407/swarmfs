#!/usr/bin/env bash
# Usage: set_loss.sh <pct> [interface]
set -euo pipefail
PCT=${1:?usage: set_loss.sh <pct> [interface]}
IFACE=${2:-lo}
tc qdisc add dev "$IFACE" root netem loss "${PCT}%"
echo "applied ${PCT}% packet loss on ${IFACE}"
