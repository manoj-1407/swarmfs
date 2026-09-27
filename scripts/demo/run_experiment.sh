#!/usr/bin/env bash
# SwarmFS Scheduler Experiment — static (round-robin) vs adaptive (score-based)
#
# Two seeders: fast (5ms/piece) and slow (500ms/piece).
#
# Static baseline: true round-robin — 50% of pieces go to each peer regardless
# of speed. The slow peer bottlenecks half the download.
#
# Adaptive: scorer routes pieces to the fast peer; slow peer only gets work
# when the fast peer can't keep up.
#
# Usage: bash scripts/demo/run_experiment.sh [file_mb] [slow_ms] [reps]
set -euo pipefail

FILE_MB=${1:-20}
FAST_MS=${FAST_MS:-5}
SLOW_MS=${2:-500}
REPS=${3:-3}
T=17000   # tracker port
S1=17001  # fast seeder
S2=17002  # slow seeder
DIR=$(mktemp -d /tmp/swarmfs-exp-XXXXXX)
SRC="$DIR/source.bin"

cleanup() { kill "$TP" "$SP1" "$SP2" 2>/dev/null || true; rm -rf "$DIR"; }
trap cleanup EXIT

echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  SwarmFS Scheduler Experiment"
printf  "  file: %dMB   fast peer: %dms/piece   slow peer: %dms/piece\n" \
        "$FILE_MB" "$FAST_MS" "$SLOW_MS"
printf  "  reps: %d   static=round-robin   adaptive=score-based\n" "$REPS"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"

echo
echo "[1/3] generating ${FILE_MB}MB source file..."
dd if=/dev/urandom of="$SRC" bs=1M count="$FILE_MB" 2>/dev/null
HASH=$(sha256sum "$SRC" | awk '{print $1}')
PIECES=$(( (FILE_MB * 1024 * 1024 + 262143) / 262144 ))
echo "      hash   : $HASH"
echo "      pieces : $PIECES (× 256 KiB)"

echo "[2/3] starting tracker and seeders..."
./tracker --addr ":$T" > "$DIR/tracker.log" 2>&1 & TP=$!
sleep 0.3

./peer seed --file "$SRC" --listen "127.0.0.1:$S1" \
    --tracker "localhost:$T" --data "$DIR/s1" \
    --slow "$FAST_MS" > "$DIR/s1.log" 2>&1 & SP1=$!

./peer seed --file "$SRC" --listen "127.0.0.1:$S2" \
    --tracker "localhost:$T" --data "$DIR/s2" \
    --slow "$SLOW_MS" > "$DIR/s2.log" 2>&1 & SP2=$!

sleep 1.5
echo "      fast seeder :17001 (${FAST_MS}ms/piece)"
echo "      slow seeder :17002 (${SLOW_MS}ms/piece)"

# theoretical times for the report
HALF=$(( PIECES / 2 ))
T_STATIC_THEORY=$(( HALF * FAST_MS + HALF * SLOW_MS ))
T_ADAPTIVE_THEORY=$(( PIECES * FAST_MS ))
echo
printf "      theory static  : %dms  (round-robin: %d×%dms + %d×%dms)\n" \
       "$T_STATIC_THEORY" "$HALF" "$FAST_MS" "$HALF" "$SLOW_MS"
printf "      theory adaptive: %dms  (all pieces via fast peer)\n" \
       "$T_ADAPTIVE_THEORY"

echo
echo "[3/3] running $REPS repetitions..."
echo

STATIC_TIMES=()
ADAPTIVE_TIMES=()

for rep in $(seq 1 "$REPS"); do
    printf "  rep %d/%d\n" "$rep" "$REPS"

    # static (round-robin)
    OUT_S="$DIR/s_${rep}.bin"
    T0=$(date +%s%3N)
    ./peer leech --hash "$HASH" \
        --listen "127.0.0.1:0" \
        --tracker "localhost:$T" \
        --data "$DIR/ls${rep}" \
        --out "$OUT_S" \
        --timeout 10m > "$DIR/ls${rep}.log" 2>&1
    T_S=$(( $(date +%s%3N) - T0 ))
    STATIC_TIMES+=("$T_S")

    GHASH=$(sha256sum "$OUT_S" | awk '{print $1}')
    [[ "$GHASH" == "$HASH" ]] || { echo "    ERROR: static hash mismatch"; exit 1; }
    printf "    static   : %dms ✓\n" "$T_S"

    # adaptive (score-based)
    OUT_A="$DIR/a_${rep}.bin"
    T0=$(date +%s%3N)
    ./peer leech --hash "$HASH" \
        --listen "127.0.0.1:0" \
        --tracker "localhost:$T" \
        --data "$DIR/la${rep}" \
        --out "$OUT_A" \
        --adaptive \
        --timeout 10m > "$DIR/la${rep}.log" 2>&1
    T_A=$(( $(date +%s%3N) - T0 ))
    ADAPTIVE_TIMES+=("$T_A")

    GHASH=$(sha256sum "$OUT_A" | awk '{print $1}')
    [[ "$GHASH" == "$HASH" ]] || { echo "    ERROR: adaptive hash mismatch"; exit 1; }
    printf "    adaptive : %dms ✓\n" "$T_A"
    echo
done

# python analysis
python3 - \
    "$REPS" "$FILE_MB" "$FAST_MS" "$SLOW_MS" "$PIECES" \
    "${STATIC_TIMES[@]}" "${ADAPTIVE_TIMES[@]}" << 'PYEOF'
import sys

args = sys.argv[1:]
reps, file_mb, fast_ms, slow_ms, pieces = int(args[0]), int(args[1]), int(args[2]), int(args[3]), int(args[4])
rest = list(map(int, args[5:]))
static_t  = rest[:reps]
adaptive_t = rest[reps:]

avg_s = sum(static_t)  / reps
avg_a = sum(adaptive_t) / reps
delta = (avg_a - avg_s) / avg_s * 100
tput_s = (file_mb * 1000) / avg_s
tput_a = (file_mb * 1000) / avg_a

half = pieces // 2
theory_s = half * fast_ms + half * slow_ms
theory_a = pieces * fast_ms

print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print("RESULTS")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
print(f"{'':22} {'Static':>10} {'Adaptive':>10} {'Δ':>8}")
print(f"{'':22} {'(round-robin)':>10} {'(scored)':>10}")
print(f"{'─'*54}")
print(f"{'Measured (avg)':22} {avg_s/1000:>9.3f}s {avg_a/1000:>9.3f}s {delta:>+7.1f}%")
print(f"{'Theory':22} {theory_s/1000:>9.3f}s {theory_a/1000:>9.3f}s")
print(f"{'Throughput':22} {tput_s:>9.2f}  {tput_a:>9.2f}")
print(f"{'':22} {'MB/s':>10} {'MB/s':>10}")
print()
print(f"Reps: {reps}    File: {file_mb}MB    Pieces: {pieces}")
print(f"Fast peer: {fast_ms}ms/piece    Slow peer: {slow_ms}ms/piece")
print()
if delta < -10:
    print(f"✓ Adaptive is {abs(delta):.1f}% faster — scheduler correctly")
    print("  routes pieces away from the slow peer.")
elif delta < 0:
    print(f"✓ Adaptive is {abs(delta):.1f}% faster.")
    print("  Try --slow 800 for a larger difference.")
else:
    print(f"△ Static was {delta:.1f}% faster this run.")
    print("  Variance likely — try more reps or larger --slow value.")
print("━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━")
PYEOF
