#!/usr/bin/env bash
# Usage: set_bandwidth.sh <mbit> [interface]
set -euo pipefail
MBIT=${1:?usage: set_bandwidth.sh <mbit> [interface]}
IFACE=${2:-lo}
tc qdisc add dev "$IFACE" root tbf rate "${MBIT}mbit" burst 32kbit latency 400ms
echo "applied ${MBIT}mbit cap on ${IFACE}"
