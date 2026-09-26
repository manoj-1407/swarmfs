#!/usr/bin/env bash
# Removes all tc qdiscs. Run after each experiment.
set -euo pipefail
IFACE=${1:-lo}
tc qdisc del dev "$IFACE" root 2>/dev/null && echo "reset ${IFACE}" || echo "nothing to reset"
