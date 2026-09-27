#!/usr/bin/env bash
# SwarmFS scheduler experiment — static vs adaptive under a slow peer.
#
# Setup: 1 fast seeder + 1 slow seeder (--slow 300ms per piece).
# Static scheduler distributes pieces evenly → bottlenecked by the slow seeder.
# Adaptive scheduler measures throughput per peer → routes away from slow peer.
#
# Usage: bash scripts/demo/run_experiment.sh [file_size_mb] [slow_ms] [reps]
set -euo pipefail

FILE_MB=${1:-20}
SLOW_MS=${2:-300}
REPS=${3:-3}
TRACKER_PORT=17000
SEED1_PORT=17001
SEED2_PORT=17002
DATADIR=$(mktemp -d /tmp/swarmfs-exp-XXXXXX)
SRCFILE="$DATADIR/source.bin"

cleanup() {
    kill "$TRACKER_PID" "$SEED1_PID" "$SEED2_PID" 2>/dev/null || true
    rm -rf "$DATADIR"
}
trap cleanup EXIT

echo "=================================================="
echo "SwarmFS Scheduler Experiment"
echo "  file size : ${FILE_MB} MB"
echo "  slow peer : ${SLOW_MS} ms/piece delay"
echo "  reps      : ${REPS}"
echo "=================================================="
echo

# generate source file
echo "[setup] generating ${FILE_MB}MB source file..."
dd if=/dev/urandom of="$SRCFILE" bs=1M count="$FILE_MB" 2>/dev/null
HASH=$(sha256sum "$SRCFILE" | awk '{print $1}')
echo "[setup] hash: $HASH"

# start tracker
./tracker --addr ":$TRACKER_PORT" &
TRACKER_PID=$!
sleep 0.3

# start fast seeder (no delay)
./peer seed \
    --file "$SRCFILE" \
    --listen "127.0.0.1:$SEED1_PORT" \
    --tracker "localhost:$TRACKER_PORT" \
    --data "$DATADIR/seed1" \
    > "$DATADIR/seed1.log" 2>&1 &
SEED1_PID=$!

# start slow seeder (SLOW_MS delay per piece)
./peer seed \
    --file "$SRCFILE" \
    --listen "127.0.0.1:$SEED2_PORT" \
    --tracker "localhost:$TRACKER_PORT" \
    --data "$DATADIR/seed2" \
    --slow "$SLOW_MS" \
    > "$DATADIR/seed2.log" 2>&1 &
SEED2_PID=$!

# wait for both seeders to register
sleep 1.5
echo "[setup] tracker + 2 seeders running (fast=:$SEED1_PORT  slow=:$SEED2_PORT  slow_delay=${SLOW_MS}ms)"
echo

# --- run experiments ---
declare -a STATIC_TIMES=()
declare -a ADAPTIVE_TIMES=()

for rep in $(seq 1 "$REPS"); do
    echo "--- rep $rep/$REPS ---"

    # static run
    OUT_S="$DATADIR/out_static_${rep}.bin"
    T_START=$(date +%s%3N)
    ./peer leech \
        --hash "$HASH" \
        --listen "127.0.0.1:0" \
        --tracker "localhost:$TRACKER_PORT" \
        --data "$DATADIR/leech_s_${rep}" \
        --out "$OUT_S" \
        --timeout 5m \
        > "$DATADIR/leech_s_${rep}.log" 2>&1
    T_END=$(date +%s%3N)
    T_STATIC=$(( T_END - T_START ))
    STATIC_TIMES+=("$T_STATIC")
    echo "  static:   ${T_STATIC}ms"

    # verify
    GOT_HASH=$(sha256sum "$OUT_S" | awk '{print $1}')
    if [ "$GOT_HASH" != "$HASH" ]; then
        echo "  ERROR: static hash mismatch!" && exit 1
    fi

    # adaptive run
    OUT_A="$DATADIR/out_adaptive_${rep}.bin"
    T_START=$(date +%s%3N)
    ./peer leech \
        --hash "$HASH" \
        --listen "127.0.0.1:0" \
        --tracker "localhost:$TRACKER_PORT" \
        --data "$DATADIR/leech_a_${rep}" \
        --out "$OUT_A" \
        --adaptive \
        --timeout 5m \
        > "$DATADIR/leech_a_${rep}.log" 2>&1
    T_END=$(date +%s%3N)
    T_ADAPTIVE=$(( T_END - T_START ))
    ADAPTIVE_TIMES+=("$T_ADAPTIVE")
    echo "  adaptive: ${T_ADAPTIVE}ms"

    GOT_HASH=$(sha256sum "$OUT_A" | awk '{print $1}')
    if [ "$GOT_HASH" != "$HASH" ]; then
        echo "  ERROR: adaptive hash mismatch!" && exit 1
    fi
done

# --- compute averages ---
python3 - "${STATIC_TIMES[@]}" "${ADAPTIVE_TIMES[@]}" "$REPS" "$FILE_MB" "$SLOW_MS" << 'PYEOF'
import sys

args = sys.argv[1:]
reps = int(args[-3])
file_mb = int(args[-2])
slow_ms = int(args[-1])
times = list(map(int, args[:-3]))
static = times[:reps]
adaptive = times[reps:]

avg_s = sum(static) / reps
avg_a = sum(adaptive) / reps
delta_pct = (avg_a - avg_s) / avg_s * 100

tput_s = file_mb * 1000 / avg_s    # MB/s
tput_a = file_mb * 1000 / avg_a

print()
print("=" * 54)
print("RESULTS")
print("=" * 54)
print(f"{'Scenario':<22} {'Static':>10} {'Adaptive':>10} {'Δ':>8}")
print("-" * 54)
print(f"{'fast+slow('+str(slow_ms)+'ms)':<22} {avg_s/1000:>9.3f}s {avg_a/1000:>9.3f}s {delta_pct:>7.1f}%")
print()
print(f"Throughput (static):   {tput_s:.2f} MB/s")
print(f"Throughput (adaptive): {tput_a:.2f} MB/s")
print()
sign = "faster" if delta_pct < 0 else "slower"
print(f"Adaptive is {abs(delta_pct):.1f}% {sign} than static.")
print()
if delta_pct < -5:
    print("✓ Scheduler working: adaptive routed away from slow peer.")
else:
    print("△ Small difference — try --slow 500 or larger file size.")
print("=" * 54)
PYEOF
